import os
import requests
import oracledb
import configparser
import json
import logging
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file='config.ini'):
    try:
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), config_file)
        config.read(config_path)
        if not config.sections():
            raise FileNotFoundError(f"No sections found in the configuration file: {config_file}")
        return config
    except Exception as e:
        logging.error(f"Error loading configuration file: {e}")
        raise

def connect_to_api(api_url, use_local):
    try:
        if use_local:
            # Open and read the local JSON file
            json_path = os.path.join(os.path.dirname(__file__), 'welsh_trusts_sample.json')
            with open(json_path, 'r') as f:
                json_data = json.load(f)
            logging.info("Loaded JSON data from local file.")
        else:
            # Fetch JSON data from the API
            response = requests.get(api_url)
            response.raise_for_status()
            json_data = response.json()
            logging.info("Fetched JSON data from API.")

        return json_data
    except IOError as e:
        logging.error(f"Failed to read JSON file: {e}")
        raise
    except requests.RequestException as e:
        logging.error(f"Failed to connect to API: {e}")
        raise
    except ValueError as e:
        logging.error(f"Invalid JSON data: {e}")
        raise

def get_project_ids_from_db(cursor):
    try:
        cursor.execute("SELECT RESOURCE_DC_IDENTIFIER_ID, DESCRIPTION FROM RESOURCE_DC_IDENTIFIER WHERE TYPE = 'Project Number'")
        rows = cursor.fetchall()
        for row in rows:
            print(f"Fetched row: {row}")
        return {row[1]: row[0] for row in rows}  # Return a dictionary with DESCRIPTION as key and RESOURCE_DC_IDENTIFIER_ID as value
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to fetch project IDs from the database: {e}")
        raise

def parse_authors(authors_str):
    authors = authors_str.split('&')
    parsed_authors = []
    for author in authors:
        parts = author.strip().split(',')
        if len(parts) == 2:
            surname = parts[0].strip()
            rest = parts[1].strip().split()
            forename = rest[0] if len(rest) > 0 else ''
            initials = ' '.join(rest[1:]) if len(rest) > 1 else ''
            parsed_authors.append({'SURNAME': surname, 'FORENAME': forename, 'INITIALS': initials})
    return parsed_authors

def insert_project(cursor, project_reference):
    try:
        cursor.execute(
            "INSERT INTO RESOURCE_DC_IDENTIFIER (DESCRIPTION, TYPE) VALUES (:description, :type) RETURNING RESOURCE_DC_IDENTIFIER_ID INTO :id",
            description=project_reference,
            type='Project Number',
            id=cursor.var(oracledb.NUMBER)
        )
        project_id = cursor.getvalue(0)
        logging.info(f"Inserted project with reference: {project_reference} and RESOURCE_DC_IDENTIFIER_ID: {project_id}")
        return project_id
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert project data into the database: {e}")
        raise

def author_exists(cursor, surname, forename, initials):
    cursor.execute(
        "SELECT PERSON_ID FROM PERSON WHERE SURNAME = :surname AND FORENAME = :forename AND INITIALS = :initials",
        surname=surname, forename=forename, initials=initials
    )
    result = cursor.fetchone()
    return result[0] if result else None

def insert_authors(cursor, authors):
    author_ids = []
    try:
        for author in authors:
            author_id = author_exists(cursor, author['SURNAME'], author['FORENAME'], author['INITIALS'])
            if not author_id:
                cursor.execute(
                    "INSERT INTO PERSON (SURNAME, FORENAME, INITIALS) VALUES (:surname, :forename, :initials) RETURNING PERSON_ID INTO :id",
                    surname=author['SURNAME'],
                    forename=author['FORENAME'],
                    initials=author['INITIALS'],
                    id=cursor.var(oracledb.NUMBER)
                )
                author_id = cursor.getvalue(0)
                logging.info(f"Inserted author: {author} with ID: {author_id}")
            else:
                logging.info(f"Author already exists: {author} with ID: {author_id}")
            author_ids.append(author_id)
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert authors data into the database: {e}")
        raise
    return author_ids

def source_exists(cursor, source_name):
    cursor.execute(
        "SELECT SOURCE_ID FROM SOURCE WHERE NAME = :name",
        name=source_name
    )
    result = cursor.fetchone()
    return result[0] if result else None

def series_exists(cursor, series_name):
    cursor.execute(
        "SELECT SERIES_ID FROM SERIES WHERE SERIES_NAME = :series_name",
        series_name=series_name
    )
    result = cursor.fetchone()
    return result[0] if result else None

def get_next_source_id(cursor):
    try:
        cursor.execute("SELECT MAX(SOURCE_ID) FROM SOURCE")
        result = cursor.fetchone()
        next_id = (result[0] or 0) + 1
        return next_id
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to fetch the next SOURCE_ID: {e}")
        raise

def insert_source_and_series(cursor, connection):
    source_name = "Welsh Archaeological Trusts - Archwilio"
    source_description = (
        "Archwilio is a Wales-wide database of archaeological and historical information.\n"
        "Clwyd-Powys Archaeological Trust\n"
        "Dyfed Archaeological Trust\n"
        "Glamorgan-Gwent Archaeological Trust\n"
        "Gwynedd Archaeological Trust"
    )
    source_link = "https://archwilio.org.uk/wp/"

    # Check if source exists
    source_id = source_exists(cursor, source_name)
    if not source_id:
        try:
            next_source_id = get_next_source_id(cursor)
            cursor.execute(
                "INSERT INTO SOURCE (SOURCE_ID, NAME, DESCRIPTION, LINK) VALUES (:source_id, :name, :description, :link)",
                source_id=next_source_id,
                name=source_name,
                description=source_description,
                link=source_link
            )
            source_id = next_source_id
            logging.info(f"Inserted source: {source_name} with SOURCE_ID: {source_id}")
        except oracledb.DatabaseError as e:
            logging.error(f"Failed to insert source data into the database: {e}")
            raise
    else:
        logging.info(f"Source already exists: {source_name} with SOURCE_ID: {source_id}")

    # Insert series
    series_name = "Welsh Archaeological Trusts reports"
    publication_type = "GreyLit"
    series_id = series_exists(cursor, series_name)
    if not series_id:
        try:
            series_id_var = cursor.var(oracledb.NUMBER)
            cursor.execute(
                "INSERT INTO SERIES (SERIES_ID, SERIES_NAME, PUBLICATION_TYPE, SOURCE_ID, CREATED_AT, WF_STAGE, ACCESS_TYPE) "
                "VALUES (series_seq.NEXTVAL, :series_name, :publication_type, :source_id, :created_at, :wf_stage, :access_type) "
                "RETURNING SERIES_ID INTO :series_id",
                series_name=series_name,
                publication_type=publication_type,
                source_id=source_id,
                created_at=datetime.datetime.now(),
                wf_stage="published",
                access_type="reference",
                series_id=series_id_var
            )
            series_id = series_id_var.getvalue()
            if isinstance(series_id, list):
                series_id = series_id[0]
            series_id = int(series_id)  # Convert to integer to remove decimal
            logging.info(f"Inserted series: {series_name} with SERIES_ID: {series_id}")

            # Insert into SERIES_NAME table
            series_name_id_var = cursor.var(oracledb.NUMBER)
            cursor.execute(
                "INSERT INTO SERIES_NAME (SERIES_NAME_ID, SERIES_ID, TITLE) VALUES (series_name_seq.NEXTVAL, :series_id, :title) RETURNING SERIES_NAME_ID INTO :series_name_id",
                series_id=series_id,
                title=series_name,
                series_name_id=series_name_id_var
            )
            series_name_id = series_name_id_var.getvalue()
            if isinstance(series_name_id, list):
                series_name_id = series_name_id[0]
            series_name_id = int(series_name_id)  # Convert to integer to remove decimal
            logging.info(f"Inserted series name: {series_name} with SERIES_NAME_ID: {series_name_id}")

        except oracledb.DatabaseError as e:
            logging.error(f"Failed to insert series data into the database: {e}")
            raise
    else:
        logging.info(f"Series already exists: {series_name} with SERIES_ID: {series_id}")
        # Fetch the existing series_name_id
        cursor.execute(
            "SELECT SERIES_NAME_ID FROM SERIES_NAME WHERE SERIES_ID = :series_id",
            series_id=series_id
        )
        result = cursor.fetchone()
        if result:
            series_name_id = result[0]
            logging.info(f"Fetched existing SERIES_NAME_ID: {series_name_id} for SERIES_ID: {series_id}")
        else:
            logging.error(f"No SERIES_NAME_ID found for existing SERIES_ID: {series_id}")
            raise ValueError("Existing series_id does not have a corresponding series_name_id")

    # Commit the transaction
    try:
        connection.commit()
        logging.info("Transaction committed successfully.")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to commit transaction: {e}")
        raise

    return source_id, series_id, series_name_id

def insert_issue(cursor, project, source_id, series_id, connection):
    try:
        title = project['oasisProjBiblioList'][0]['title']
        abstract = project['oasisProjDetails']['descOutcome']
        year_of_publication = project['oasisProjBiblioList'][0]['pubdate']

        cursor.execute(
            """
            INSERT INTO ISSUE (
                ISSUE_ID, TITLE, ABSTRACT, YEAR_OF_PUBLICATION, ACCESS_TYPE, LICENSE_TYPE,
                PUBLICATION_TYPE, PUBLICATION_TYPE2, SERIES_NAME_ID, SOURCE_ID,
                IS_UNPUBLISHED, WF_STAGE
            ) VALUES (
                issue_seq.NEXTVAL, :title, :abstract, :year_of_publication, 'linked', 'Standard',
                'GreyLitSeries', 'GreyLitSeries', :series_id, :source_id,
                1, 'published'
            )
            """,
            title=title,
            abstract=abstract,
            year_of_publication=year_of_publication,
            series_id=series_id,
            source_id=source_id
        )
        # Optionally, fetch the last inserted ID if needed
        cursor.execute("SELECT issue_seq.CURRVAL FROM dual")
        issue_id = cursor.fetchone()[0]
        logging.info(f"Inserted issue for project: {project['projReference']} with ISSUE_ID: {issue_id}")
        return issue_id
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert issue data into the database: {e}")
        raise
            # Commit the transaction
    try:
        connection.commit()
        logging.info("Transaction committed successfully.")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to commit transaction: {e}")
        raise

def link_authors_to_issue(cursor, author_ids, issue_id):
    try:
        for author_id in author_ids:
            cursor.execute(
                "INSERT INTO RESOURCE_PERSON (PERSON_ID, RELATIONSHIP_TYPE_ID, ISSUE_ID) VALUES (:person_id, 4, :issue_id)",
                person_id=author_id,
                issue_id=issue_id
            )
            logging.info(f"Linked PERSON_ID {author_id} to ISSUE ID {issue_id}")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to link authors to issue in the database: {e}")
        raise

def insert_sites_and_project_number(cursor, project, issue_id):
    try:
        # Insert site codes
        for site in project['oasisProjSiteList']:
            cursor.execute(
                "INSERT INTO RESOURCE_DC_IDENTIFIER (DESCRIPTION, TYPE, ISSUE_ID) VALUES (:description, 'Site Code', :issue_id)",
                description=site['sitecode'],
                issue_id=issue_id
            )
            logging.info(f"Inserted site code: {site['sitecode']} for issue ID: {issue_id}")

        # Insert project number
        cursor.execute(
            "INSERT INTO RESOURCE_DC_IDENTIFIER (DESCRIPTION, TYPE, ISSUE_ID) VALUES (:description, 'Project Number', :issue_id)",
            description=project['projReference'],
            issue_id=issue_id
        )
        logging.info(f"Inserted project number: {project['projReference']} for issue ID: {issue_id}")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert site codes and project number into the database: {e}")
        raise

def insert_bibliographic_urls(cursor, project, issue_id):
    try:
        for biblio in project['oasisProjBiblioList']:
            if 'url' in biblio:
                cursor.execute(
                    "INSERT INTO RESOURCE_DC_RELATION (TYPE, URI, ISSUE_ID) VALUES ('URI', :uri, :issue_id)",
                    uri=biblio['url'],
                    issue_id=issue_id
                )
                logging.info(f"Inserted bibliographic URL: {biblio['url']} for issue ID: {issue_id}")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert bibliographic URLs into the database: {e}")
        raise

def insert_location_data(cursor, project, issue_id, connection):
    try:
        # Insert country
        cursor.execute(
            "INSERT INTO RESOURCE_DC_COV_LOC (TYPE, DESCRIPTION, ISSUE_ID) VALUES ('Country', 'Wales', :issue_id)",
            issue_id=issue_id
        )
        logging.info(f"Inserted location: Country=Wales for issue ID: {issue_id}")

        # Insert parish, district, county, and site
        admin_areas = project['adminAreasMap']
        cursor.execute(
            "INSERT INTO RESOURCE_DC_COV_LOC (TYPE, DESCRIPTION, ISSUE_ID) VALUES ('Parish', :community, :issue_id)",
            community=admin_areas['Community'],
            issue_id=issue_id
        )
        logging.info(f"Inserted location: Parish={admin_areas['Community']} for issue ID: {issue_id}")

        cursor.execute(
            "INSERT INTO RESOURCE_DC_COV_LOC (TYPE, DESCRIPTION, ISSUE_ID) VALUES ('District', :district, :issue_id)",
            district=admin_areas['Unitary Authority'],
            issue_id=issue_id
        )
        logging.info(f"Inserted location: District={admin_areas['Unitary Authority']} for issue ID: {issue_id}")

        cursor.execute(
            "INSERT INTO RESOURCE_DC_COV_LOC (TYPE, DESCRIPTION, ISSUE_ID) VALUES ('County', :county, :issue_id)",
            county=admin_areas['Old County'],
            issue_id=issue_id
        )
        logging.info(f"Inserted location: County={admin_areas['Old County']} for issue ID: {issue_id}")

        # Handle site insertion based on conditions
        site_list = project['oasisProjSiteList']
        if len(site_list) == 1:
            # Single site: associate with all issues
            cursor.execute(
                "INSERT INTO RESOURCE_DC_COV_LOC (TYPE, DESCRIPTION, ISSUE_ID) VALUES ('Site', :sitename, :issue_id)",
                sitename=site_list[0]['sitename'],
                issue_id=issue_id
            )
            logging.info(f"Inserted location: Site={site_list[0]['sitename']} for issue ID: {issue_id}")
        elif len(site_list) > 1 and len(project['oasisProjBiblioList']) == 1:
            # Multiple sites, single issue: associate all sites with the issue
            for site in site_list:
                cursor.execute(
                    "INSERT INTO RESOURCE_DC_COV_LOC (TYPE, DESCRIPTION, ISSUE_ID) VALUES ('Site', :sitename, :issue_id)",
                    sitename=site['sitename'],
                    issue_id=issue_id
                )
                logging.info(f"Inserted location: Site={site['sitename']} for issue ID: {issue_id}")
        else:
            logging.info("Multiple sites and multiple issues detected; no site association made.")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert location data into the database: {e}")
        raise

def insert_coordinates(cursor, project, issue_id, connection):
    try:
        site_list = project['oasisProjSiteList']
        if len(site_list) == 1 or len(project['oasisProjBiblioList']) == 1:
            for site in site_list:
                coords = site['oasisProjSiteCoordsList']
                vector_type = coords['vectorType']
                easting, northing = parse_coordinates(coords['geomNgrOut'])
                lat, long = parse_coordinates(coords['geomLlOut'])

                cursor.execute(
                    """
                    INSERT INTO RESOURCE_DC_COV_COORD (
                        TYPE, EASTING, NORTHING, ISSUE_ID, COORDINATE_TYPE, LAT_Y, LONG_X
                    ) VALUES (
                        :type, :easting, :northing, :issue_id, 'POINT', :lat, :long
                    )
                    """,
                    type=vector_type,
                    easting=easting,
                    northing=northing,
                    issue_id=issue_id,
                    lat=lat,
                    long=long
                )
                logging.info(f"Inserted coordinates for site: {site['sitename']} with issue ID: {issue_id}")
        else:
            logging.info("Multiple sites and multiple issues detected; no coordinate association made.")
    except oracledb.DatabaseError as e:
        logging.error(f"Failed to insert coordinates into the database: {e}")
        raise

def parse_coordinates(coord_str):
    # Extracts the numeric values from the POINT string
    coord_str = coord_str.replace('POINT(', '').replace(')', '')
    return tuple(map(float, coord_str.split(',')))

def main():
    total_steps = "?"
    try:
        print(f"Step 1/{total_steps}: Loading configuration")
        config = load_config()

        # Read configuration values
        api_url = config['API']['url']
        use_local = config.getboolean('API', 'use_local')

        print(f"Step 2/{total_steps}: Verify API feed & database connection")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            try:
                json_data = connect_to_api(api_url, use_local)
                logging.info("Connected to API and fetched data.")
            except Exception as e:
                logging.error(f"API connection failed: {e}")
                return

            try:
                username = config['DATABASE']['username']
                password = config['DATABASE']['password']
                host = config['DATABASE']['host']
                port = config['DATABASE']['port']
                sid = config['DATABASE']['sid']
                print(f"Username: {username}, Password: XXXXX, Host: {host}, Port: {port}, SID: {sid}")
                print(f"Connecting to database with host={host}, port={port}, sid={sid}")
                dsn_str = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SID={sid})))"
                print(f"DSN String: {dsn_str}")
                connection = oracledb.connect(user=username, password=password, dsn=dsn_str)
                cursor = connection.cursor()
                logging.info("Connected to the database.")
            except oracledb.DatabaseError as e:
                logging.error(f"Failed to connect to the database: {e}")
                raise
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                return
        else:
            return

        print(f"Step 3/{total_steps}: Insert source and series if not exists")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
             try:
                 source_id, series_id, series_name_id = insert_source_and_series(cursor, connection)
             except Exception as e:
                 logging.error(f"Failed to insert or locate source and series (including series_names): {e}")
                 return

        print(f"Step 4/{total_steps}: Check for existing projects and remove from JSON")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            try:
                project_ids_in_db = get_project_ids_from_db(cursor)

                # Access oasisProjDetails directly as a dictionary
                try:
                    print(json_data['oasisProjDetails'])
                    project_reference = json_data['oasisProjDetails']['projReference']
                    print(f"Project Reference: {project_reference}")
                except KeyError as e:
                    logging.error(f"KeyError accessing 'projReference': {e}")
                    return

                # Create a set with the single project reference
                json_project_ids = {project_reference}

                common_project_ids = project_ids_in_db.keys() & json_project_ids
                logging.info(f"Found {len(common_project_ids)} matching projects in the database.")

                if common_project_ids:
                    print(f"Found {len(common_project_ids)} matching projects. Do you want to proceed with removal from copy of JSON data source? (yes/no): ")
                    if input().lower() == 'yes':
                        # Remove the project from oasisProjDetails if it matches
                        if project_reference in common_project_ids:
                            json_data['oasisProjDetails'] = {}
                            logging.info("Removed matching project from the JSON data.")
                    else:
                        return
                else:
                    logging.info("No matching projects found.")
            except Exception as e:
                logging.error(f"Failed to check for existing projects: {e}")
                return
        else:
            return

        print(f"Step 5/{total_steps}: Insert new projects, authors, issues, site codes, bibliographic URLs, and location data")
        if input("Do you want to proceed? (yes/no): ").lower() == 'yes':
            try:
                for project in json_data['oasisProjDetails']:
                    project_id = insert_project(cursor, project['projReference'])
                    authors_str = project['oasisProjBiblioList'][0]['oasisProjBiblioAuthsList']['name']
                    parsed_authors = parse_authors(authors_str)
                    author_ids = insert_authors(cursor, parsed_authors)
                    issue_id = insert_issue(cursor, project, source_id, series_id)
                    link_authors_to_issue(cursor, author_ids, issue_id)
                    insert_sites_and_project_number(cursor, project, issue_id)
                    insert_bibliographic_urls(cursor, project, issue_id)
                    insert_location_data(cursor, project, issue_id)
                connection.commit()
                logging.info("Inserted new projects, authors, issues, site codes, bibliographic URLs, and location data.")
            except Exception as e:
                logging.error(f"Failed to insert new projects, authors, issues, site codes, bibliographic URLs, and location data: {e}")
                return
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
