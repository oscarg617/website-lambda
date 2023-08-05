import os
import json
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, request
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder

load_dotenv()

ec2_dns = os.getenv("EC2")
ec2_user = os.getenv("EC2_USER")
ec2_key = os.getenv("EC2_KEY")

database_name = os.getenv("DATABASE")
hostname = os.getenv("RDS_HOST")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")


SELECT_PLAYER_LOGS = """SELECT log.id AS log_id, log.season, player.id AS player_id, player.full_name, log.game_day, log.team, 
log.opponent, log.minutes, log.field_goals, log.field_goals_attempted, log.field_goal_percentage, log.three_pointers, 
log.three_pointers_attempted, log.three_pointer_percentage, log.free_throws, log.free_throws_attempted, 
log.free_throw_percentage, log.offensive_rebounds, log.defensive_rebounds, log.total_rebounds, log.assists, log.steals, 
log.blocks, log.turnovers, log.personal_fouls, log.points, log.plus_minus, team.def_rtg, team.ts_pct, game.pace
FROM log, player, team, game WHERE log.player_id = player.id AND log.game_id = game.id
AND log.opponent_id = team.id AND player.full_name = '%s' AND log.season >= %s AND log.season <= %s 
AND team.def_rtg >= %s AND team.def_rtg < %s ;"""

app = Flask(__name__)


@app.route("/api/stats", methods=["GET"])
def index():
    if request.method == "GET":
        data = request.get_json()
        return handle_get(data)


def handle_get(data):
    name = data["name"]
    start_year = data["start_year"]
    end_year = data["end_year"]
    min_def_rtg = data["min_def_rtg"]
    max_def_rtg = data["max_def_rtg"]

    df = create_pandas_dataframe(name, start_year, end_year, min_def_rtg, max_def_rtg)

    ts_pct = df["ts_pct"].mean()
    misc_stats = create_misc_stats(df)

    df = drop_columns(df)

    percentages_stats = create_percentage_stats(df, ts_pct)

    per_game_stats = create_per_game_stats(df)

    per_100_stats = create_per_100_stats(df, misc_stats)

    adjusted_stats = create_adjusted_stats(per_100_stats, misc_stats)

    return json.dumps(
        create_tables(
            per_game_stats, per_100_stats, adjusted_stats, percentages_stats, misc_stats
        )
    )


def create_pandas_dataframe(name, start_year, end_year, min_def_rtg, max_def_rtg):
    with SSHTunnelForwarder(
        (ec2_dns),
        ssh_username=ec2_user,
        ssh_pkey=ec2_key,
        remote_bind_address=(hostname, int(port)),
        local_bind_address=("127.0.0.1", 5332),
    ):
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@127.0.0.1:5332/{database_name}"
        )

        df = pd.read_sql(
            SELECT_PLAYER_LOGS % (name, start_year, end_year, min_def_rtg, max_def_rtg),
            engine,
        )
        return df


def create_misc_stats(df):
    avg_minutes = df["minutes"].mean()
    avg_pace = df["pace"].mean()
    avg_def_rtg = df["def_rtg"].mean()
    misc = {
        "name": "-",
        "minutes": avg_minutes,
        "pace": avg_pace,
        "def-rtg": avg_def_rtg,
    }
    return misc


def drop_columns(df):
    return df.drop(
        columns=[
            "log_id",
            "player_id",
            "season",
            "full_name",
            "game_day",
            "team",
            "opponent",
            "field_goal_percentage",
            "three_pointer_percentage",
            "free_throw_percentage",
            "minutes",
            "pace",
            "def_rtg",
            "ts_pct",
        ]
    )


def create_percentage_stats(df, ts_pct):
    sum_df = df.sum()

    percentages = handle_percentages(ts_pct, sum_df)
    percentages["name"] = "-"

    return percentages


def handle_percentages(ts_pct, sum_df):
    fg = sum_df["field_goals"]
    fga = sum_df["field_goals_attempted"]
    fg_pct = fg / fga

    threes = sum_df["three_pointers"]
    threes_a = sum_df["three_pointers_attempted"]
    three_pct = threes / threes_a

    ft = sum_df["free_throws"]
    fta = sum_df["free_throws_attempted"]
    ft_pct = ft / fta

    points = sum_df["points"]
    ts_pct = points / (2 * (fga + (0.44 * fta)))

    rts_pct = 100 * (ts_pct - ts_pct)

    return {
        "field_goal_percentage": round(100 * fg_pct, 1),
        "three_pointer_percentage": round(100 * three_pct, 1),
        "free_throw_percentage": round(100 * ft_pct, 1),
        "true_shooting_percentage": round(100 * ts_pct, 1),
        "relative_true_shooting_percentage": round(rts_pct, 2),
    }


def create_per_game_stats(df):
    per_game_stats = json.loads(df.describe().to_json(orient="records"))[1]
    per_game_stats["name"] = "Per Game"

    return per_game_stats


def create_per_100_stats(df, misc_stats):
    avg_minutes = misc_stats["minutes"]
    avg_pace = misc_stats["pace"]
    avg_possessions = (avg_minutes / 48) * avg_pace
    factor = 100 / avg_possessions

    for col in list(df.columns):
        df[col] *= factor

    per_100_stats = json.loads(df.describe().to_json(orient="records"))[1]
    per_100_stats["name"] = "Per 100"

    return per_100_stats


def create_adjusted_stats(per_100_stats, misc_stats):
    avg_def_rtg = misc_stats["def-rtg"]
    avg_minutes = misc_stats["minutes"]
    points_factor = (110 / avg_def_rtg) * (avg_minutes / 48)
    adjusted_stats = {
        "name": "Per Game (Adjusted)",
        "points": per_100_stats["points"] * points_factor,
    }

    return adjusted_stats


def create_tables(
    per_game_stats, per_100_stats, adjusted_stats, percentages_stats, misc_stats
):
    points_lists = [per_game_stats, per_100_stats, adjusted_stats]
    percentages_lists = [percentages_stats]
    box_scores_lists = [per_game_stats, per_100_stats]
    misc_lists = [misc_stats]

    points_mapping = {"points": "PTS"}

    points_table = create_table(points_mapping, points_lists, 1)

    percentages_mapping = {
        "true_shooting_percentage": "TS%",
        "relative_true_shooting_percentage": "rTS%",
        "field_goal_percentage": "FG%",
        "three_pointer_percentage": "3PT%",
        "free_throw_percentage": "FT%",
    }

    percentages_table = create_table(percentages_mapping, percentages_lists, 2)

    box_scores_mapping = {
        "assists": "AST",
        "total_rebounds": "REB",
        "offensive_rebounds": "ORB",
        "defensive_rebounds": "DRB",
        "steals": "STL",
        "blocks": "BLK",
        "turnovers": "TO",
        "personal_fouls": "PF",
        "field_goals": "FG",
        "field_goals_attempted": "FGA",
        "three_pointers": "3P",
        "three_pointers_attempted": "3PA",
        "free_throws": "FT",
        "free_throws_attempted": "FTA",
    }

    box_scores_table = create_table(box_scores_mapping, box_scores_lists, 2)

    misc_mapping = {"minutes": "MIN", "pace": "Pace", "def-rtg": "DRTG"}

    misc_table = create_table(misc_mapping, misc_lists, 3)

    return [points_table, percentages_table, box_scores_table, misc_table]


def create_table(mapping, stats_dicts, table_id):
    keys = list(mapping.keys())

    box_score_names = [
        {"id": id + 1, "name": mapping[keys[id]]} for id in range(len(keys))
    ]

    stat_types_list = []
    for stats_dict, i in zip(stats_dicts, range(len(stats_dicts))):
        stats_box_scores = [
            {"id": id + 1, "value": stats_dict[keys[id]]} for id in range(len(keys))
        ]
        stat_type = {
            "id": i + 1,
            "name": stats_dict["name"],
            "boxScores": stats_box_scores,
        }
        stat_types_list.append(stat_type)

    return {
        "id": table_id,
        "boxScoreStats": box_score_names,
        "statTypes": stat_types_list,
    }


if __name__ == "__main__":
    app.run(debug=True)
