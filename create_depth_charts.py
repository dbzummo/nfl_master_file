import pandas as pd
from fetch_rosters import get_api_roster

def create_value_based_depth_charts():
    """
    Fetches live depth charts and merges them with our calculated player priors
    to create a single source of truth for player values.
    """
    print("Loading player priors...")
    try:
        player_priors = pd.read_csv('player_priors_2025.csv')
    except FileNotFoundError:
        print("Error: player_priors_2025.csv not found.")
        return

    all_teams_depth = []
    # This list would be dynamically generated in a full run
    teams_to_process = ['PHI', 'DAL', 'KC', 'SF', 'BUF', 'CIN'] 

    for team in teams_to_process:
        roster_data = get_api_roster(team)
        if not roster_data:
            continue

        # Convert raw API data to a DataFrame
        team_df = pd.DataFrame(roster_data)
        
        # We only care about key offensive positions for this model
        key_positions = ['QB', 'RB', 'WR', 'TE']
        team_df = team_df[team_df['Position'].isin(key_positions)]
        
        # Standardize the name for merging
        team_df['player_name'] = team_df['FirstName'].str[0] + '.' + team_df['LastName']
        
        # Merge with our player priors
        depth_chart = pd.merge(
            team_df[['player_name', 'Position', 'DepthOrder']],
            player_priors[['player_name', 'prior_2025']],
            on='player_name',
            how='left'
        )
        
        depth_chart['team_code'] = team
        # Fill missing priors with a baseline replacement value
        depth_chart['prior_2025'].fillna(-1.0, inplace=True)
        all_teams_depth.append(depth_chart)

    if not all_teams_depth:
        print("Could not generate any depth charts.")
        return

    # Combine all teams into one final DataFrame
    final_depth_chart = pd.concat(all_teams_depth).sort_values(by=['team_code', 'Position', 'DepthOrder'])
    
    output_filename = 'team_depth_charts_with_values.csv'
    final_depth_chart.to_csv(output_filename, index=False)
    print(f"\nSuccess! Value-based depth charts saved to '{output_filename}'.")


if __name__ == '__main__':
    create_value_based_depth_charts()