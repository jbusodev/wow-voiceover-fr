from tts_cli.env_vars import MYSQL_HOST, MYSQL_PORT, MYSQL_PASSWORD, MYSQL_USER, MYSQL_DATABASE
import pymysql
import io
import zipfile
import requests
import os
import sys
import re
from tqdm import tqdm

# vanilla db dump url
VMANGOS_DB_DUMP_URL = "https://api.github.com/repos/vmangos/core/releases/tags/db_latest"
# tbc db dump url
TBC_DB_DUMP_URL = "https://api.github.com/repos/cmangos/tbc-db/releases/latest"
# wotlk db dump url
WOTLK_DB_DUMP_URL = "https://api.github.com/repos/cmangos/wotlk-db/releases/latest"

EXPORTED_FILES = ['assets/sql/exported/CreatureDisplayInfo.sql',
                  'assets/sql/exported/CreatureDisplayInfoExtra.sql']

# TBC extra tables - extracted from wago tools 2.5.1.38043
TBC_EXPORTED_FILES = ['assets/sql/exported/tbc/CreatureDisplayInfo.sql',
                      'assets/sql/exported/tbc/CreatureDisplayInfoExtra.sql']

# WotLK extra tables - extracted from wago tool 3.4.0.43746
WOTLK_EXPORTED_FILES = ['assets/sql/exported/wotlk/CreatureDisplayInfo.sql',
                      'assets/sql/exported/wotlk/CreatureDisplayInfoExtra.sql']

def download_and_extract_latest_db_dump():
    expansion = 'vanilla'
    print(f"Retrieving latest version for {expansion}")
    check_version = requests.get(VMANGOS_DB_DUMP_URL)
    get_latest = check_version.json()['assets'][0]['browser_download_url']
    response = requests.get(get_latest)
    if response.status_code == 200:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        if(expansion == 'vanilla'):
            z.extractall("assets/sql")
        else:
            # logic for extracting mangos databases.
            # If scraping data, logic should be modified or moved to a separate function
            z.extractall(f"assets/sql/db_dump/{expansion}")
        print("Successfully downloaded and extracted database dump.")
    else:
        print("Error: Unable to download the database dump.")
        exit(1)


def count_total_chunks(files, delimiter):
    total_chunks = 0
    for file in files:
        with open(file, "rb") as f:
            buffer = f.read()
            total_chunks += buffer.count(delimiter)
    return total_chunks


def count_commands_from_file(filename):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    sqlCommands = sqlFile.split(';')
    return len(sqlCommands)


def execute_scripts_from_file(cursor, filename, progress_update_fn):
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    sqlCommands = sqlFile.split(';')

    for command in sqlCommands:
        try:
            cursor.execute(command)
        except pymysql.Error as e:
            pass
        progress_update_fn()

def prompt_import():
    user_input = input("\nDo you want to continue with the import? (yes/no): ").lower().strip()
    if user_input in ['yes', 'y']:
        print("Continuing with import...")
    elif user_input in ['no', 'n']:
        print("Import cancelled by user.")
        sys.exit(0)  # Exit the script
    else:
        print("Invalid input. Import cancelled.")
        sys.exit(0)  # Exit the script for any invalid input

def import_sql_files_to_database():
    expansion = 'vanilla'
    db = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset='utf8mb4'
    )
    cursor = db.cursor()
    # Enforce UTF-8 for the connection.
    cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES,NO_ENGINE_SUBSTITUTION';")
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")
    
    if expansion == 'vanilla':
        db_name = MYSQL_DATABASE
        db_dump_dir = f"assets/sql/db_dump"
    else:
        db_name = f"{expansion}{MYSQL_DATABASE}"
        db_dump_dir = f"assets/sql/db_dump/{expansion}"

    print(f"Importing into {db_name} database from {db_dump_dir}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    cursor.execute(f"USE {db_name};")

    sql_files = [os.path.join(db_dump_dir, f) for f in os.listdir(db_dump_dir) 
             if f.endswith(".sql") and os.path.isfile(os.path.join(db_dump_dir, f))]
        

    print(f"Files to be imported:{sql_files}")
    # prompt_import()
    
    chunk_size = 1024 * 1024  # 1MB
    delimiter = b";\n"
    total_chunks = count_total_chunks(sql_files, delimiter) + sum(map(count_commands_from_file, EXPORTED_FILES))

    def execute_sql_command(command):
        command = command.strip()
        if command:
            # Remove version-specific comment wrappers if present
            if command.startswith('/*!') and command.endswith('*/'):
                command = re.sub(r'/\*!\d+\s*(.*?)\s*\*/$', r'\1', command, flags=re.DOTALL)
            cursor.execute(command)

    with tqdm(total=total_chunks, unit='chunks', desc='Importing SQL files', ncols=100) as pbar:
        for file in sql_files:
            with open(file, "rb") as f:
                buffer = bytearray()
                while chunk := f.read(chunk_size):
                    buffer.extend(chunk)
                    while delimiter in buffer:
                        pos = buffer.index(delimiter)
                        try:
                            sql_command = buffer[:pos].decode('utf-8')
                            execute_sql_command(sql_command)
                            db.commit()
                        except pymysql.Error as e:
                            print(f"Error importing {file}: {e}")
                            # print(f"Problematic SQL command: {sql_command}")
                            raise
                        buffer = buffer[pos+len(delimiter):]
                        pbar.update(1)  # Update progress bar for each chunk
                # Execute any remaining SQL commands
                if buffer:
                    try:
                        sql_command = buffer.decode('utf-8')
                        execute_sql_command(sql_command)
                        db.commit()
                    except pymysql.Error as e:
                        print(f"Error importing {file}: {e}")
                        # print(f"Problematic SQL command: {sql_command}")
                        raise
            print(f'Imported {file}')

        for file in EXPORTED_FILES:
            execute_scripts_from_file(cursor, file, progress_update_fn=lambda: pbar.update(1))

    db.commit()
    cursor.close()
    db.close()


if __name__ == "__main__":
    download_and_extract_latest_db_dump()
    import_sql_files_to_database()
    
    print("Database initialized successfully.")
