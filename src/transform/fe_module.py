import pandas as pd


def team_stats(df):
    stats = (df.groupby(['team'], as_index = False)
        .agg(
            pass_yards = ('passing_yards', 'sum'),
            rush_yards = ('rushing_yards', 'sum'),
            pass_attempts = ('attempts', 'sum'),
            rush_attempts = ('carries', 'sum'),
            pass_touchdowns = ('passing_tds', 'sum'),
            rush_touchdowns = ('rushing_tds', 'sum')
            )
        )
    return stats

def team_records(df):
    # Calculates wins from schedule dataframe
    wins = (
        df
        .assign(
            home_win=lambda x: (x.home_score > x.away_score).astype(int),
            away_win=lambda x: (x.away_score > x.home_score).astype(int)
        )
    )

    # Assigns wins to teams
    home_wins = wins.groupby(["season", "home_team"])["home_win"].sum()
    away_wins = wins.groupby(["season", "away_team"])["away_win"].sum()

    # Creates dataframe with season team and wins
    team_record = home_wins.add(away_wins, fill_value=0).reset_index()
    team_record.columns = ["season", "team", "wins"]

    # Caluclates losses by getting total number of games
    home_games = df.groupby(["season", "home_team"]).size()
    away_games = df.groupby(["season", "away_team"]).size()
    total_games = home_games.add(away_games, fill_value=0).reset_index()
    total_games.columns = ["season", "team", "games_played"]

    # Adds losses column to team_record dataframe
    team_record['losses'] = total_games['games_played'] - team_record['wins']

    return team_record

def stat_percentages(df_stats, df_record):
    # TEAM STATS
    # Pass % = pass attempts / pass attempts + rush attempts
    # Rush % = rush attempts / pass attempts + rush attempts
    # YPA = pass yards / pass attempts
    # YPR = rush yards / rush attempts
    # Win % = wins / wins + losses
    # Combine the data first
    df = df_stats.merge(df_record, on=['team'])
    
    # Total plays
    total_plays = df['pass_attempts'] + df['rush_attempts']
    
    # Percentages
    df_percent = pd.DataFrame({
        'team': df['team'],
        'pass_percent': df['pass_attempts'] / total_plays,
        'rush_percent': df['rush_attempts'] / total_plays,
        'ypa': df['pass_yards'] / df['pass_attempts'],
        'ypr': df['rush_yards'] / df['rush_attempts'],
        'win_percent': df['wins'] / (df['wins'] + df['losses'])
    })

    return df_percent


def league_averages(df_stats):
    # LEAGUE STATS
    # Average pass attempts
    # Average rush attempts
    # Average pass %
    # Average rush %
    avg_pass_attempts = df_stats['pass_attempts'].mean()
    avg_rush_attempts = df_stats['rush_attempts'].mean()
    
    total_pass = df_stats['pass_attempts'].sum()
    total_rush = df_stats['rush_attempts'].sum()
    total_plays = total_pass + total_rush
    
    avg_pass_percent = total_pass / total_plays
    avg_rush_percent = total_rush / total_plays
    
    return {
        'avg_pass_attempts': avg_pass_attempts,
        'avg_rush_attempts': avg_rush_attempts,
        'avg_pass_percent': avg_pass_percent,
        'avg_rush_percent': avg_rush_percent
    }