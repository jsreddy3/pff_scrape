import asyncio
import aiohttp
import pandas as pd

# Read the DataFrame
df = pd.read_csv('merged_players.csv')

async def fetch_and_process_player(session, player_id):
    url = f"https://www.pff.com/api/players/{player_id}/stats?season=2023&week_group=REG"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            print(f"Fetching data for Player ID: {player_id}")
            return player_id, process_player_data(data)
        else:
            print(f"Failed to fetch data for Player ID: {player_id}")
            return player_id, None

def process_player_data(data):
    processed_data = {}
    # Process primary and secondary stats
    for stat in data.get('primary_stats', []) + data.get('secondary_stats', []):
        if 'locked' not in stat:
            label = stat['label']
            value = stat.get('value', None)
            processed_data[label] = value

    # Extract 'Overall' grade from player grades
    player_grades = data.get('player_grades', [])
    overall_grade = next((item['grade'] for item in player_grades if item.get('label') == 'Overall'), None)
    processed_data['Overall Grade'] = overall_grade

    return processed_data


async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_process_player(session, pid) for pid in df['Player ID']]
        results = await asyncio.gather(*tasks)

        # Update the DataFrame with results
        for player_id, player_data in results:
            if player_data:
                for label, value in player_data.items():
                    if label not in df.columns:
                        df[label] = None  # Create the column if it doesn't exist
                        print(f"Adding new column: {label}")
                    df.loc[df['Player ID'] == player_id, label] = value

    # Save the updated DataFrame
    df.to_csv('updated_players.csv', index=False)
    print("Updated DataFrame saved to 'updated_players.csv'.")

if __name__ == "__main__":
    asyncio.run(main())
