import pandas as pd
# Load the merged CSV file
df = pd.read_csv('final.csv')

# Prioritize values from '_x' columns, and if they are NaN, take from '_y'
for column in df.columns:
    if '_x' in column:
        base_column = column.replace('_x', '')
        df[base_column] = df[column].fillna(df[base_column + '_y'])

# Drop the '_x' and '_y' columns
df.drop(columns=[col for col in df.columns if '_x' in col or '_y' in col], inplace=True)

# Save the cleaned DataFrame to a new CSV file
cleaned_file_path = 'cleaned_merged_final_players.csv'
df.to_csv(cleaned_file_path, index=False)