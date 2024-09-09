import json

# Sample JSON data for testing
json_data = {
    "oasisProjDetails": [
        {
            "projReference": "PRJ001",
            "oasisProjBiblioList": [
                {
                    "oasisProjBiblioAuthsList": {
                        "name": "Doe, John A. &  Smith, Jane B. & rober, brew & robert, B."
                    }
                }
            ]
        },
        {
            "projReference": "PRJ002",
            "oasisProjBiblioList": [
                {
                    "oasisProjBiblioAuthsList": {
                        "name": "Brown, Charlie C. & White, Alice D."
                    }
                }
            ]
        }
    ]
}

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

def main():
    # Commented out most of the script to focus on parsing authors
    print("Parsing authors from JSON data...")
    for project in json_data['oasisProjDetails']:
        authors_str = project['oasisProjBiblioList'][0]['oasisProjBiblioAuthsList']['name']
        parsed_authors = parse_authors(authors_str)
        print(f"Project Reference: {project['projReference']}")
        for author in parsed_authors:
            print(f"Author: SURNAME={author['SURNAME']}, FORENAME={author['FORENAME']}, INITIALS={author['INITIALS']}")

if __name__ == "__main__":
    main()
