
from collections.abc import Callable
from pathlib import Path
import json
import psycopg
import traceback

CONNECTION_SETTINGS_FILE_NAME = 'connection.json'

default_connection_settings = lambda : {
    'host': '',
    'port': 5432,
    'database': '',
    'user': '',
    'password': '',
}

def write_default_connection_settings_file(file_path: str):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(default_connection_settings(), f, ensure_ascii=False, indent=4)

def load_connection_settings_from_file(file_path: str):    
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def please_configure_settings_file_msg(file_path: str):
    return f'please configure the psql connection settings in file {file_path}' 

def new_scalar_psql_executor(connection_settings_file_path: str):
    '''
    returns a callable (that returns a new connection) if successful, otherwise None
    '''

    if not Path(connection_settings_file_path).is_file():
        write_default_connection_settings_file(connection_settings_file_path)
        print(please_configure_settings_file_msg(connection_settings_file_path))
        return None

    settings = load_connection_settings_from_file(connection_settings_file_path)

    def execute_scalar_sql(sql: str) -> bool:

        try:
            with psycopg.connect(
                host=settings['host'],
                dbname=settings['database'],
                user=settings['user'],
                password=settings['password'],
                port=settings['port'],) as connection:
                
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    connection.commit()
        except KeyError as ke:
            missing_key = ke.args[0]
            print(f'missing connection setting: {missing_key}')
            return False
        except:
            exc_error = traceback.format_exc()
            error_msg = f'psql execution failed with error: {exc_error}, sql: {sql}'
            print(error_msg)
            return False

        return True
    
    return execute_scalar_sql

def entrypoint():
    
    exec_scalar_sql = new_scalar_psql_executor(CONNECTION_SETTINGS_FILE_NAME)
    if not exec_scalar_sql:
        print('config error')
        return

    exec_scalar_sql('select * from auth_user;')

if __name__ == '__main__':
    entrypoint()