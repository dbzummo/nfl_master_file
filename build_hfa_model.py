import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points on the earth."""
    R = 3958.8 # Radius of earth in miles
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    distance = R * c
    return distance

print("STEP 1: Loading all necessary data files...")
# Load all data sources
pbp_files = ['play_by_play_2022.csv', 'play_by_play_2023.csv', 'play_by_play_2024.csv']
stadiums = pd.read_csv('stadium_details.csv')
locations = pd.read_csv('team_locations.csv')
pbp_data = pd.concat([pd.read_csv(file, low_memory=False) for file in pbp_files])

# Correctly calculate final scores using the right column names
final_scores = pbp_data.groupby('game_id').agg(
    home_team=('home_team', 'first'),
    away_team=('away_team', 'first'),
    home_score=('total_home_score', 'max'),
    away_score=('total_away_score', 'max')
).dropna()

games_df = final_scores.copy()

print("STEP 2: Engineering features...")
# Calculate point differential
games_df['point_differential'] = games_df['home_score'] - games_df['away_score']

# Merge stadium and location data
games_df = games_df.merge(stadiums, left_on='home_team', right_on='team_code', how='left')
games_df = games_df.merge(locations, left_on='home_team', right_on='team_code', suffixes=('_home', '_away'), how='left')
games_df = games_df.merge(locations, left_on='away_team', right_on='team_code', suffixes=('_home', '_away'), how='left')

# Calculate travel distance
games_df['travel_distance'] = haversine_distance(
    games_df['latitude_away'], games_df['longitude_away'],
    games_df['latitude_home'], games_df['longitude_home']
).fillna(0)

# Create binary features
games_df['is_dome'] = (games_df['dome_status'] != 'Outdoor').astype(int)
games_df['is_turf'] = (games_df['turf_type'] == 'Turf').astype(int)

# Impute missing values with the mean for numeric columns
numeric_features = ['altitude_ft', 'travel_distance']
for col in numeric_features:
    mean_val = games_df[col].mean()
    games_df[col].fillna(mean_val, inplace=True)

print("STEP 3: Building and training the regression model...")
# Define features (X) and target (y)
features = ['is_dome', 'is_turf', 'altitude_ft', 'travel_distance']
target = 'point_differential'

team_dummies = pd.get_dummies(games_df['home_team'], prefix='team')
X = pd.concat([games_df[features], team_dummies], axis=1)
y = games_df[target]

model = LinearRegression()
model.fit(X, y)

print("STEP 4: Calculating HFA for each team...")
baseline_travel = X['travel_distance'].mean()
hfa_results = []
all_teams = locations['team_code'].unique()

for team in all_teams:
    team_data = games_df[games_df['home_team'] == team]
    if not team_data.empty:
        team_feature_row = pd.Series(0, index=X.columns)

        team_feature_row['is_dome'] = team_data['is_dome'].iloc[0]
        team_feature_row['is_turf'] = team_data['is_turf'].iloc[0]
        team_feature_row['altitude_ft'] = team_data['altitude_ft'].iloc[0]
        team_feature_row['travel_distance'] = baseline_travel

        if f'team_{team}' in team_feature_row.index:
            team_feature_row[f'team_{team}'] = 1

        predicted_hfa = model.predict([team_feature_row])[0]
        hfa_results.append({'team_code': team, 'advanced_hfa': predicted_hfa})

hfa_df = pd.DataFrame(hfa_results).sort_values('advanced_hfa', ascending=False)
output_filename = 'stadium_hfa_advanced.csv'
hfa_df.to_csv(output_filename, index=False)

print(f"\nSuccess! Advanced HFA model complete.")
print(f"Results saved to '{output_filename}'.")
