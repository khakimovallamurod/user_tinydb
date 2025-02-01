import csv
import os
from tinydb import TinyDB, Query
import sys

def read_csv(file_path):
    # Read and parse the CSV file
    if not os.path.exists(file_path):
        raise ValueError(f"File not found: {file_path}")
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        read_file = csv.reader(file)
        for one_user in read_file:
            user_data = {
                'id': one_user[0],
                'first_name': one_user[1],
                'last_name': one_user[2],
                'email': one_user[3],
                'gender': one_user[4],
                'job': one_user[5]
            }
            data.append(user_data)
    data.pop(0)
    return data

def insert_into_db(data, db_path='test.json'):
    # Insert data into TinyDB
    if len(data)==0:
        raise ValueError(f'Data empty {data}')
    db = TinyDB(db_path, indent = 4)
    users_data = db.table('Users')
    add_users = users_data.insert_multiple(data)
    
    return add_users

def query_db(db_path, query_field=None, query_value=None):
    # Query the database
    query = Query()
    db = TinyDB(db_path)
    users = db.table('Users')
    if query_value != None and query_field!=None:
        return users.search(query[query_field] == query_value)
    return users.all()

def main():
    csv_file = sys.argv[1]  
    db_path = sys.argv[2]

    data = read_csv(csv_file)

    add_data = insert_into_db(data, db_path)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"{db_path} fayli yaratilmadi!")
    return add_data

if __name__ == "__main__":
    # Main execution logic
    data = (read_csv(file_path='user_data.csv'))
    user_add = insert_into_db(data, 'users_data.json')
    query_data = query_db('users_data.json', 'gender', 'Male')
