import psycopg2
import json
import xml.etree.ElementTree as ET


db_name = 'university'
db_user = 'postgres'
db_password = '13BD13'
db_host = 'localhost'
db_port = '5432'



def connect_to_db():
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    return conn



def load_data_to_db(file_path, table_name):
    conn = connect_to_db()
    cur = conn.cursor()

    with open(file_path, 'r') as f:
        data = json.load(f)
        for entry in data:
            columns = ', '.join(entry.keys())
            placeholders = ', '.join(['%s'] * len(entry.values()))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cur.execute(query, list(entry.values()))

    conn.commit()
    cur.close()
    conn.close()


def fetch_query_results(query):
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results


def convert_data_for_json(data):
    if isinstance(data, (list, tuple)):
        return [convert_data_for_json(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_data_for_json(value) for key, value in data.items()}
    elif isinstance(data, (int, float, str, bool)):
        return data
    elif data is None:
        return None
    else:
        return str(data)  # Преобразовать все другие типы в строки



def save_results_to_file(results, file_format='json'):
    if file_format == 'json':
        results = convert_data_for_json(results)
        with open('results.json', 'w') as f:
            json.dump(results, f, indent=4)
    elif file_format == 'xml':
        root = ET.Element("results")
        for key, rows in results.items():
            item = ET.SubElement(root, key)
            for row in rows:
                row_elem = ET.SubElement(item, "row")
                for col, val in enumerate(row):
                    ET.SubElement(row_elem, f"column{col}").text = str(val)
        tree = ET.ElementTree(root)
        tree.write('results.xml')



def get_room_counts():
    query = """
    SELECT r.id, r.name, COUNT(s.id) AS student_count
    FROM rooms r
    LEFT JOIN students s ON r.id = s.room
    GROUP BY r.id, r.name;
    """
    return fetch_query_results(query)


def get_rooms_with_smallest_avg_age():
    query = """
    SELECT r.id, r.name, AVG(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.birthday)) AS avg_age
    FROM rooms r
    JOIN students s ON r.id = s.room
    GROUP BY r.id, r.name
    ORDER BY avg_age ASC
    LIMIT 5;
    """
    return fetch_query_results(query)


def get_rooms_with_largest_age_diff():
    query = """
    SELECT r.id, r.name, 
           MAX(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.birthday)) - 
           MIN(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM s.birthday)) AS age_diff
    FROM rooms r
    JOIN students s ON r.id = s.room
    GROUP BY r.id, r.name
    ORDER BY age_diff DESC
    LIMIT 5;
    """
    return fetch_query_results(query)


def get_rooms_with_gender_diversity():
    query = """
    SELECT r.id, r.name
    FROM rooms r
    JOIN students s ON r.id = s.room
    GROUP BY r.id, r.name
    HAVING COUNT(DISTINCT s.sex) > 1;
    """
    return fetch_query_results(query)



def main():

    file_path_rooms = r'C:\Users\bogda\Downloads\rooms.json'
    file_path_students = r'C:\Users\bogda\Downloads\students.json'


    load_data_to_db(file_path_rooms, 'rooms')
    load_data_to_db(file_path_students, 'students')


    results = {
        "room_counts": get_room_counts(),
        "rooms_with_smallest_avg_age": get_rooms_with_smallest_avg_age(),
        "rooms_with_largest_age_diff": get_rooms_with_largest_age_diff(),
        "rooms_with_gender_diversity": get_rooms_with_gender_diversity()
    }


    save_results_to_file(results, 'json')



if __name__ == "__main__":
    main()
