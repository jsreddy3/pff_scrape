import pandas as pd

# File paths
players_file = 'forgotten.csv'  # First CSV file
teams_file = 'players_with_teams.csv'      # Second CSV file
merged_file = 'merged_players.csv'  # Output file after merging

# Read CSV files
players_df = pd.read_csv(players_file)
teams_df = pd.read_csv(teams_file)

# Merge the dataframes on 'Player ID'
merged_df = pd.merge(players_df, teams_df, on='Player ID', how='left')

# Save the merged dataframe to a new CSV file
merged_df.to_csv(merged_file, index=False)

print("Merged file saved as", merged_file)
