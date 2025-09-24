import pandas as pd

def update_kalman_ratings(current_state_path: str, weekly_pbp_path: str, output_path: str, week_num: int):
    process_variance, measurement_variance = 0.15, 2.5
    print(f"Loading current state from {current_state_path}...")
    try:
        current_ratings = pd.read_csv(current_state_path).set_index('team_code')
        pbp_data = pd.read_csv(weekly_pbp_path, low_memory=False)
    except FileNotFoundError as e:
        print(f"Error: Missing required file: {e.filename}")
        return

    mean_rating = current_ratings['power_rating'].mean()
    current_ratings['power_rating'] = 0.98 * current_ratings['power_rating'] + 0.02 * mean_rating
    current_ratings['uncertainty'] = current_ratings['uncertainty'] + process_variance

    print("Calculating weekly performance (EPA per Play)...")
    offensive_epa = pbp_data.groupby('posteam')['epa'].mean().rename('off_epa_per_play')
    defensive_epa = pbp_data.groupby('defteam')['epa'].mean().rename('def_epa_per_play')

    team_performance = pd.concat([offensive_epa, defensive_epa], axis=1).fillna(0)
    team_performance['net_epa_per_play'] = team_performance['off_epa_per_play'] - team_performance['def_epa_per_play']

    print("Updating ratings with new game data...")
    for team, performance in team_performance.iterrows():
        if team not in current_ratings.index:
            continue

        rating = current_ratings.loc[team]
        kalman_gain = rating['uncertainty'] / (rating['uncertainty'] + measurement_variance)
        innovation = (performance['net_epa_per_play'] * 15) - rating['power_rating']

        new_rating = rating['power_rating'] + kalman_gain * innovation
        new_uncertainty = (1 - kalman_gain) * rating['uncertainty']

        current_ratings.loc[team, 'power_rating'] = new_rating
        current_ratings.loc[team, 'uncertainty'] = new_uncertainty

    current_ratings['week_ended'] = week_num
    current_ratings['last_updated_utc'] = pd.Timestamp.utcnow().isoformat()

    current_ratings.reset_index().to_csv(output_path, index=False)
    print(f"\nSuccess! New ratings saved to '{output_path}'.")

if __name__ == '__main__':
    print("This script updates team ratings after a week of games.")
