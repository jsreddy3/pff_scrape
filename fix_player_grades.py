import asyncio
import aiohttp
import pandas as pd

# Load the existing DataFrame
df = pd.read_csv('updated_players.csv')

async def fetch_and_update_player_grade(session, player_id):
    url = f"https://www.pff.com/api/players/{player_id}/stats?season=2023&week_group=REG"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            overall_grade = process_player_grade(data)
            print(player_id, overall_grade)
            return player_id, overall_grade
        else:
            return player_id, None

def process_player_grade(data):
    player_grades = data.get('player_grades', [])
    for grade in player_grades:
        if grade.get('label') == 'Overall':
            return grade.get('grade')
    return None  # Return None if 'Overall' grade is not found

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_update_player_grade(session, pid) for pid in df['Player ID']]
        results = await asyncio.gather(*tasks)

        # Check if 'Overall Grade' column exists, if not create it
        if 'Overall Grade' not in df.columns:
            df['Overall Grade'] = None

        # Update the DataFrame with the fetched grades
        for player_id, grade in results:
            df.loc[df['Player ID'] == player_id, 'Overall Grade'] = grade

    # Save the updated DataFrame
    df.to_csv('updated_players_with_grades.csv', index=False)
    print("Updated DataFrame with grades saved to 'updated_players_with_grades.csv'.")

if __name__ == "__main__":
    asyncio.run(main())
