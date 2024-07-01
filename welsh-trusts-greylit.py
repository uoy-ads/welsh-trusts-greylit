import requests
import cx_Oracle
import configparser
import xml.etree.ElementTree as ET
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file='config.ini'):
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        if not config.sections():
            raise FileNotFoundError(f"No sections found in the configuration file: {config_file}")
        return config
    except Exception as e:
        logging.error(f"Error loading configuration file: {e}")
        raise

def connect_to_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Failed to connect to API: {e}")
        raise

def parse_xml(xml_data):
    try:
        return ET.fromstring(xml_data)
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML data: {e}")
        raise

def connect_to_database(username, password, dsn):
    try:
        connection = cx_Oracle.connect(username, password, dsn)
        return connection
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise

def get_project_ids_from_db(cursor):
    try:
        cursor.execute("SELECT DESCRIPTION FROM RESOURCE_DC_IDENTIFIER")
        return {row[0] for row in cursor.fetchall()}
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Failed to fetch project IDs from the database: {e}")
        raise

def insert_project(cursor, project_data):
    try:
        # Implement your insertion logic here based on your database schema
        pass
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Failed to insert project data into the database: {e}")
        raise

def insert_authors(cursor, authors):
    try:
        # Implement your insertion logic here based on your database schema
        pass
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Failed to insert authors data into the database: {e}")
        raise

def main():
    total_steps = 4
    try:
        print(f"Step 1/{total_steps}: Load configuration")
        config = load_config()

        print(f"Step 2/{total_steps}: Verify API & database connection")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            try:
                xml_data = connect_to_api(config['API']['url'])
                logging.info("Connected to API and fetched data.")
            except Exception as e:
                logging.error(f"API connection failed: {e}")
                return

            try:
                connection = connect_to_database(config['DATABASE']['username'],
                                                 config['DATABASE']['password'],
                                                 config['DATABASE']['dsn'])
                cursor = connection.cursor()
                logging.info("Connected to the database.")
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                return
        else:
            return

        print(f"Step 3/{total_steps}: Process the XML data")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            root = parse_xml(xml_data)
            logging.info("Parsed XML data.")
        else:
            return

        print(f"Step 4/{total_steps}: Check for existing projects and remove from XML")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            project_ids_in_db = get_project_ids_from_db(cursor)
            xml_projects = root.findall('.//oasisProjDetails')
            xml_project_ids = {proj.find('projReference').text for proj in xml_projects}

            common_project_ids = project_ids_in_db.intersection(xml_project_ids)
            logging.info(f"Found {len(common_project_ids)} matching projects in the database.")
            for project_id in common_project_ids:
                logging.info(f"Matching project ID: {project_id}")

            if common_project_ids:
                print(f"Found {len(common_project_ids)} matching projects. Do you want to proceed with removal from copy of XML data source? (yes/no): ")
                if input().lower() == 'yes':
                    root[:] = [proj for proj in xml_projects if proj.find('projReference').text not in common_project_ids]
                    logging.info("Removed matching projects from the XML data.")
                else:
                    return
            else:
                logging.info("No matching projects found.")
        else:
            return

        print(f"Step 5/{total_steps}: Insert new projects and authors")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            for project in root.findall('.//oasisProjDetails'):
                insert_project(cursor, project)
                authors = project.find('oasisProjBiblioList').findall('oasisProjBiblioAuthsList')
                insert_authors(cursor, authors)
            connection.commit()
            logging.info("Inserted new projects and authors.")
        else:
            return

        logging.info("Process completed.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        except NameError:
            pass

if __name__ == "__main__":
    main()
