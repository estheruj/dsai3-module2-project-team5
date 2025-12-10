from sqlalchemy import create_engine
import os

def get_database_connection():
    db_type = os.getenv('DB_TYPE', 'postgresql')
    user = os.getenv('DB_USER', 'your_username')
    password = os.getenv('DB_PASSWORD', 'your_password')
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    database = os.getenv('DB_NAME', 'your_database')

    connection_string = f"{db_type}://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    return engine.connect()