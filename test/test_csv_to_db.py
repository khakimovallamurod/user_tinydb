import pytest
from src.csv_to_db import read_csv, insert_into_db, query_db
import os
from tinydb import TinyDB

@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "test.csv"
    content = """id,first_name,last_name,email,gender,job
1,John,Doe,john@test.com,Male,Engineer
2,Jane,Smith,jane@test.com,Female,Designer"""
    csv_path.write_text(content)
    return str(csv_path)

@pytest.fixture
def empty_csv(tmp_path):
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("id,first_name,last_name,email,gender,job")
    return str(csv_path)

def test_read_csv_success(sample_csv):
    data = read_csv(sample_csv)
    assert len(data) == 2
    assert data[0]['first_name'] == 'John'

def test_read_csv_file_not_found():
    with pytest.raises(ValueError):
        read_csv('nonexistent.csv')

def test_read_csv_empty(empty_csv):
    data = read_csv(empty_csv)
    assert len(data) == 0

def test_insert_and_query(tmp_path):
    db_path = str(tmp_path / "test_db.json")
    test_data = [{'id': '1', 'first_name': 'Test'}]
    
    # Test insertion
    inserted_ids = insert_into_db(test_data, db_path)
    assert len(inserted_ids) == 1
    
    # Test query
    results = query_db(db_path)
    assert len(results) == 1
    assert results[0]['first_name'] == 'Test'

def test_query_with_filter(tmp_path):
    db_path = str(tmp_path / "test_db.json")
    print(tmp_path)
    test_data = [
        {'id': '1', 'gender': 'Male'},
        {'id': '2', 'gender': 'Female'}
    ]
    insert_into_db(test_data, db_path)
    
    results = query_db(db_path, 'gender', 'Female')
    assert len(results) == 1
    assert results[0]['id'] == '2'

def test_insert_empty_data():
    with pytest.raises(ValueError):
        insert_into_db([])

def test_main_cli(tmp_path, sample_csv, monkeypatch):
    db_path = str(tmp_path / "cli_test.json")
    
    # Mock command line arguments
    monkeypatch.setattr('sys.argv', ['script.py', sample_csv, '--db', db_path])
    
    # Run main script
    import src.csv_to_db as csv_to_db
    csv_to_db.main()
    
    # Verify database creation
    assert os.path.exists(db_path)
    db = TinyDB(db_path)
    assert  len(db.table('Users')) == 2