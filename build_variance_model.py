import pandas as pd
from sklearn.linear_model import LinearRegression
import joblib

print("STEP 1: Loading all necessary data files...")
# Load all data sources
pbp_files = ['play_by_play_2022.csv', 'play_by_play_2023.csv', 'play_by_play_2024.csv']
kalman_ratings = pd.read_csv('kalman_state_preseason.csv').set_index('team_code') # Using preseason as a simple baseline
hfa_data = pd.read_csv('stadium_hfa_advanced.csv').set_index('team_code')

pbp_data = pd.concat([pd.read_csv(file, low_memory=False) for file in pbp_files])

# Get a unique list of games with final scores and Vegas totals
games_df = pbp_data.groupby('game_id').agg(
    home_team=('home_team', 'first'),
    away_team=('away_team', 'first'),
    home_score=('total_home_score', 'max'),
    away_score=('total_away_score', 'max'),
    vegas_total=('total_line', 'first') # Vegas Over/Under line
).dropna().reset_index()

print("STEP 2: Calculating historical prediction errors...")
# Merge in the ratings and HFA to calculate the expected spread
games_df = games_df.merge(kalman_ratings['power_rating'], left_on='home_team', right_index=True)
games_df = games_df.merge(kalman_ratings['power_rating'], left_on='away_team', right_index=True, suffixes=('_home', '_away'))
games_df = games_df.merge(hfa_data['advanced_hfa'], left_on='home_team', right_index=True)

# Calculate the model's expected outcome and the actual outcome
games_df['expected_spread'] = games_df['power_rating_home'] - games_df['power_rating_away'] + games_df['advanced_hfa']
games_df['actual_spread'] = games_df['home_score'] - games_df['away_score']

# The "error" is the difference between what we expected and what happened
games_df['prediction_error'] = games_df['actual_spread'] - games_df['expected_spread']

print("STEP 3: Building and training the variance model...")
# Our model will predict the magnitude of the error (volatility)
# The feature (X) is the Vegas total, the target (y) is the absolute error
X = games_df[['vegas_total']]
y = abs(games_df['prediction_error'])

# Train the regression model
variance_model = LinearRegression()
variance_model.fit(X, y)

output_filename = 'variance_model.joblib'
joblib.dump(variance_model, output_filename)

print(f"\nSuccess! Contextual Variance Model trained.")
print(f"Model saved to '{output_filename}'.")
