
from collections.abc import Callable
from pathlib import Path
import json
import psycopg
import traceback
import argparse

default_connection_settings = lambda : {
    'host': '',
    'port': 5432,
    'database': '',
    'user': '',
    'password': '',
}

def write_default_connection_settings_file(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as f:
        json.dump(default_connection_settings(), f, ensure_ascii=False, indent=4)

def load_json_file(file_path: str):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def load_connection_settings_from_file(file_path: str):    
    return load_json_file(file_path)

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
            error_msg = f'psql execution failed\nerror: {exc_error}\nsql: {sql}'
            print(error_msg)
            return False

        return True
    
    return execute_scalar_sql

def load_template_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as template_file:
        return template_file.read()

def load_scenarios_from_file(file_path):
    return load_json_file(file_path)

def parse_args():
    parser = argparse.ArgumentParser(description='execute psql templated sql for scenarios')

    parser.add_argument(f'--connection',
        help='path to json connection file',
        default='connection.json',
        required=True)
    
    parser.add_argument(f'--template',
        help='path to psql template file',
        required=True)
    
    parser.add_argument(f'--scenarios',
        help='path to json scenarios file',
        required=True)

    return vars(parser.parse_args())

def entrypoint():

    args = parse_args()

    template_file_path = args['template']
    template = load_template_from_file(template_file_path)

    scenarios_file_path = args['scenarios']
    scenarios = load_scenarios_from_file(scenarios_file_path)

    connection_file_path = args['connection']
    exec_scalar_sql = new_scalar_psql_executor(connection_file_path)
    if not exec_scalar_sql:
        print('error executing sql')
        return False

    for scenario in scenarios:

        rendered_sql = template
        for key, value in scenario.items():
            rendered_sql = rendered_sql.replace('{' + key + '}', value)

        successful = exec_scalar_sql(rendered_sql)
        if not successful:
            print(f'scalar sql execution failed for scenario: {scenario}')
            return False
        
    return True

if __name__ == '__main__':
    entrypoint()