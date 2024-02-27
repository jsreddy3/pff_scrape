import asyncio
import aiohttp
import pandas as pd
import time

MAX_RETRIES = 3  # Max number of retries for a single request
RETRY_SLEEP = 0.5  # Sleep time (in seconds) between retries

async def fetch_player_data(session, player_id, file_name, log_file):
    url = f"https://www.pff.com/api/players/{player_id}/stats?season=2023&week_group=REG"
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with session.get(url) as response:
                with open(log_file, 'a') as log:
                    log.write(f"{player_id},{response.status}\n")

                if response.status == 200:
                    data = await response.json()
                    primary_premium_url = data.get('primary_premium_url', '')
                    player_name = extract_player_name(primary_premium_url) if primary_premium_url else 'Unknown'
                    print("Successfully fetched data for player:", player_name)
                    
                    with open(file_name, 'a') as file:
                        file.write(f"{player_name},{player_id}\n")
                    return  # Successful fetch, exit function
                elif response.status == 404:
                    print(f"Player ID {player_id} not found (404).")
                    return  # Player ID not found, exit function
                else:
                    # Other errors, possible rate limit
                    print(f"Error fetching player ID {player_id}: {response.status}. Retrying...")
                    retries += 1
                    await asyncio.sleep(RETRY_SLEEP)
                    
        except Exception as e:
            print(f"Exception for player ID {player_id}: {e}")
            retries += 1
            await asyncio.sleep(RETRY_SLEEP)

    print(f"Max retries reached for player ID {player_id}.")

def extract_player_name(url):
    """
    Extracts the player's name from the PFF premium URL.
    Assumes the format: https://premium.pff.com/nfl/players/<player-name>/<player-id>/...
    """
    segments = url.split('/')
    try:
        # Extracts the segment before the player ID
        name_segment = segments[segments.index('players') + 1]
        return name_segment.replace('-', ' ').title()
    except (IndexError, ValueError):
        return 'Unknown'

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for player_id in range(0, 10000):  # Example range
            file_name = f"player_data_{player_id % 10}.csv"  # Example file naming
            task = asyncio.ensure_future(fetch_player_data(session, player_id, file_name, "log.txt"))
            tasks.append(task)
        await asyncio.gather(*tasks)

    # Merging data from files
    all_data = pd.DataFrame()
    for i in range(10):
        file_name = f"player_data_{i}.csv"
        df = pd.read_csv(file_name, names=['Player Name', 'Home Team', 'Player ID'])
        all_data = pd.concat([all_data, df], ignore_index=True)

    # Saving merged data to a single file
    all_data.to_csv("merged_player_data.csv", index=False)

if __name__ == "__main__":
    asyncio.run(main())
