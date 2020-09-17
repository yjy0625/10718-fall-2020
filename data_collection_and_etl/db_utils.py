import os
import psycopg2


PGPASS_PATH = os.path.join(os.path.expanduser('~'), '.pgpass')


def get_connection(host, user, database, password_path=PGPASS_PATH):
    with open(password_path, 'r') as f:
        password = f.read().replace('\n', '').split(':')[-1]

    print('Connecting to database...', end=' ')
    conn = psycopg2.connect(host=host,
                            user=user,
                            password=password,
                            database=database)
    print('done.')
    return conn


def insert_data_from_csv(db_connection,
                         schema_name, table_name,
                         column_types,
                         csv_filename='data.csv'):
    try:
        cursor = db_connection.cursor()

        create_schema_command = f'create schema if not exists {schema_name};'

        print(f'Creating schema {schema_name}...', end=' ')
        cursor.execute(create_schema_command)
        db_connection.commit()
        print('done.')

        full_table_name = f'{schema_name}.{table_name}'
        types_str = ", ".join([k + ' ' + v for k, v in column_types.items()])
        drop_table_command = f'drop table if exists {full_table_name};'
        create_table_command = f'create table {full_table_name} ({types_str});'

        print(f'Creating table {full_table_name}...', end=' ')
        cursor.execute(drop_table_command)
        db_connection.commit()

        cursor.execute(create_table_command)
        db_connection.commit()
        print('done.')

        print(f'Inserting data...', end=' ')
        with open(csv_filename, 'r') as f:
            next(f)
            cursor.copy_from(f, full_table_name, sep='|')
        db_connection.commit()
        print('done.')

        return cursor.rowcount
    except:
        return -1
