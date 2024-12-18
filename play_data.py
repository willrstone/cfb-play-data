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
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data if data else []
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return []


def get_existing_game_ids(cursor):
    """Retrieve all distinct game IDs from the database."""
    query = "SELECT DISTINCT game_id FROM plays;"
    cursor.execute(query)
    return {row[0] for row in cursor.fetchall()}


def insert_data(data, week, season, connection, cursor):
    try:
        with open('plays_data.csv', mode='a', newline='') as file:
            writer = csv.writer(file)

            new_records = 0
            updated_records = 0

            # Loop through plays to get play data
            for item in data:
                try:
                    values = (
                        item['id'], season, week, item['offense'], item['defense'],
                        item['offenseConference'], item['defenseConference'], item['home'], item['away'],
                        item['gameId'], item['driveId'], item['driveNumber'], item['playNumber'], item['period'],
                        item['yardline'], item['yardsToGoal'], item['down'], item['distance'],
                        item['scoring'], item['yardsGained'], item['playType'], item['playText'],
                        item['ppa'], item['offenseScore'], item['defenseScore']
                    )

                    query = """
                    INSERT INTO plays (
                        id, season, week, offense, defense, offense_conference, defense_conference, home,
                        away, game_id, drive_id, drive_number, play_number, period, yard_line,
                        yards_to_goal, down, distance, scoring, yards_gained, play_type, play_text, ppa,
                        offense_score, defense_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s)
                    """

                    cursor.execute(query, values)

                    # Write row to CSV
                    writer.writerow(values)
                    new_records += 1

                except KeyError as e:
                    print(f"Skipping row due to missing data: {e}")
                except psycopg2.IntegrityError:
                    print(f"Updating play ID {item['id']}...")
                    updated_records += 1

        connection.commit()
        print(f'Successfully inserted {new_records} new records and updated {updated_records} records for week {week}.')

    except Exception as e:
        print(f"Error inserting data: {e}")
        connection.rollback()


def main():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            dbname=os.getenv("DATABASE")
        )
        cur = conn.cursor()

        # Get all existing game IDs from the database
        existing_game_ids = get_existing_game_ids(cur)

        # Write CSV header
        with open('plays_data.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                'id', 'season', 'week', 'offense', 'defense', 'offense_conference', 'defense_conference',
                'home', 'away', 'game_id', 'drive_id', 'drive_number', 'play_number', 'period',
                'yard_line', 'yards_to_goal', 'down', 'distance', 'scoring', 'yards_gained',
                'play_type', 'play_text', 'ppa', 'offense_score', 'defense_score'
            ])

        # Loop through weeks, from 1 to 15
        for week in range(1, 16):
            print(f"Processing week {week}...")
            api_url = f"https://apinext.collegefootballdata.com/plays?year=2024&week={week}&classification=fbs"
            api_data = fetch_data_from_api(api_url)
            season = 2024
            if not api_data:  # Stop if no data is returned
                print(f"No data found for week {week}. Stopping.")
                break
            # Use a set to store unique gameIds from the API data
            unique_game_ids = {str(item['gameId']) for item in api_data if 'gameId' in item}
            plays = []
            for game in unique_game_ids:
                if game in existing_game_ids:
                    print(f'GameId {game} is already in the database.')
                else:
                    print(f'New game found: {game}')

                    # Push all plays that belong to this game to plays array
                    matching_plays = [item for item in api_data if item['gameId'] == int(game)]
                    for play in matching_plays:
                        plays.append(play)

            # Push new plays to plays array
            if plays:
                print('plays found')
                insert_data(plays, week, season, conn, cur)

    except Exception as e:
        print(f"Error in main loop: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")


main()
