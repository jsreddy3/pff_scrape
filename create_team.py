import asyncio
import aiohttp
import pandas as pd
import csv

input_file = 'forgotten.csv'  # Your existing CSV file
output_file = 'players_with_teams.csv'  # New CSV file with teams

async def get_player_team(session, player_id):
    url = f"https://www.pff.com/api/players/{player_id}/next_game?"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                game_info = data.get('game')
                if game_info is None:
                    return player_id, None  # Skip this player if game info is not available
                opponent_team = data.get('opponent', {}).get('display_abbreviation', '')
                away_team = game_info.get('away_team', {}).get('abbreviation', '')
                home_team = game_info.get('home_team', {}).get('abbreviation', '')
                player_team = home_team if opponent_team == away_team else away_team
                print(player_id, player_team)
                return player_id, player_team
            else:
                return player_id, 'Unknown'
    except Exception as e:
        print(f"Error fetching player ID {player_id}: {e}")
        return player_id, 'Unknown'

async def main():
    player_ids = []
    with open(input_file, mode='r') as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip the header row
        for row in reader:
            _, player_id, _ = row
            player_ids.append(player_id)

    async with aiohttp.ClientSession() as session:
        tasks = [get_player_team(session, player_id) for player_id in player_ids]
        results = await asyncio.gather(*tasks)

    # Write results to a new CSV file
    with open(output_file, mode='w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Player ID', 'Team'])  # Header for the new CSV
        for player_id, team in results:
            if team is not None:
                writer.writerow([player_id, team])

if __name__ == "__main__":
    asyncio.run(main())
