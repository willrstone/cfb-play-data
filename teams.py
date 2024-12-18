import psycopg2
import requests
import csv
from dotenv import load_dotenv
import os

load_dotenv()


def fetch_data_from_api(api_url):
    api_key = os.getenv("API_KEY")
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    response = requests.get(api_url, headers=headers)
    data = response.json()
    return data


def insert_data(data, connection, cursor):
    with open('teams.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['id', 'school', 'abbreviation', 'conference', 'division', 'classification',
                         'color', 'logo'])
        # Loop through each week and fetch play data
        for team in data:
            print(team.get('logos')[0])
            id = team.get('id')
            school = team.get('school')
            abbreviation = team.get('abbreviation')
            conference = team.get('conference')
            division = team.get('division')
            classification = team.get('classification')
            color = team.get('color')
            logo = team.get('logos')[0]


            query = """
            INSERT INTO teams (
                id,
                school,
                abbreviation,
                conference,
                division,
                classification,
                color,
                logo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                id,
                school,
                abbreviation,
                conference,
                division,
                classification,
                color,
                logo
            )

            cursor.execute(query, values)

            # Write row to CSV
            writer.writerow([
                id,
                school,
                abbreviation,
                conference,
                division,
                classification,
                color,
                logo
            ])

        connection.commit()


def main():
    api_url = 'https://apinext.collegefootballdata.com/teams/fbs?year=2024'
    conn = psycopg2.connect(
        user=os.getenv("USERNAME"),
        password=os.getenv("PASSWORD"),
        host=os.getenv("HOST"),
        port=os.getenv("PORT"),
        dbname=os.getenv("DATABASE")
    )

    cur = conn.cursor()

    api_data = fetch_data_from_api(api_url)

    insert_data(api_data, conn, cur)

    cur.close()
    conn.close()


main()
