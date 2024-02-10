import boto3
import json
import os
import psycopg2
import smtplib, ssl
import statsapi
import structlog
import tempfile
import time

from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

"""
This script will run daily at 5:00 AM.

Tasks include:
 - Adding the day's games to the database
 - Updating yesterday's games with the winning team
"""
# This code makes the script work on PythonAnywhere
config_file_path = "config.env"
if os.path.exists(config_file_path):
    # Read environment variables from the file
    with open(config_file_path, "r") as file:
        for line in file:
            if not line.startswith("#") and "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[key.strip()] = value.strip()

EMAIL_FROM = os.getenv("MLB_GAMES_EMAIL_FROM")
EMAIL_PASSWORD = os.getenv("MLB_GAMES_EMAIL_PASSWORD")
EMAIL_TO = os.getenv("MLB_GAMES_EMAIL_TO")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
PSQL_CONNECTION_STRING = os.getenv("PSQL_CONNECTION_STRING")
TABLE_NAME = os.getenv("MLB_DB_TABLE_NAME")
LOGS_ACCESS_KEY_ID = os.getenv("LOGS_ACCESS_KEY_ID")
LOGS_SECRET_ACCESS_KEY = os.getenv("LOGS_SECRET_ACCESS_KEY")
LOGS_ENDPOINT_URL = os.getenv("LOGS_ENDPOINT_URL")

current_time = str(datetime.now()).replace(" ", "_")[:19].replace(":", "-")
updated = []
prepared = []

s3 = boto3.resource(
    service_name="s3",
    aws_access_key_id=LOGS_ACCESS_KEY_ID,
    aws_secret_access_key=LOGS_SECRET_ACCESS_KEY,
    endpoint_url=LOGS_ENDPOINT_URL,
)

connection_string = PSQL_CONNECTION_STRING

aws_psql_conn = psycopg2.connect(connection_string)


def lookup_player(player: str) -> str:
    """
    Gets the ID of a player specified by name

    :param player: the ID of the player whose information is being accessed
    :returns: the player ID for the specified name
    """
    try:
        return statsapi.lookup_player(player)[0]["id"]
    except IndexError:
        print(f"Unable to get ID for pitcher {player}")
        return None


def get_ERA(pitcher: str) -> float:
    """
    Gets the ERA for a pitcher

    :param pitcher: the name of the pitcher whose ERA is being accessed
    :returns: the ERA for the given pitcher as a float to two decimal places; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]

            return format(float(pitcher_stats["era"]), ".2f")
        except IndexError:
            print(f"Unable to get ERA for pitcher {pitcher}")
            return None
    else:
        return None


def get_win_percentage(pitcher: str) -> float:
    """
    Gets the win percentage for a pitcher

    :param pitcher: the name of the pitcher whose win percentage is being accessed
    :returns: the win percentage for the given pitcher as a float to two decimal places; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                return format(float(pitcher_stats["winPercentage"]), ".3f")
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get win percentage for pitcher {pitcher}")
            return None
    else:
        return None


def get_losses(pitcher: str) -> int:
    """
    Gets the pitcher's losses

    :param pitcher: the name of the pitcher whose losses are being accessed
    :returns: the pitcher's losses as an int; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                return int(pitcher_stats["losses"])
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get losses for pitcher {pitcher}")
            return None
    else:
        return None


def get_wins(pitcher: str) -> int:
    """
    Gets the pitcher's wins

    :param pitcher: the name of the pitcher whose wins are being accessed
    :returns: the pitcher's wins as an int; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                return int(pitcher_stats["wins"])
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get wins for pitcher {pitcher}")
            return None
    else:
        return None


def get_IP(pitcher: str) -> float:
    """
    Gets the number of innings pitched for a pitcher.

    Because innings pitched are counted with .0, .1, .2, where 1, and 2 are outs, the decimal point will be multipled by 3 for later computation purposes.

    :param pitcher: the name of the pitcher whose IP are being accessed
    :returns: the innings pitched as a float to one decimal place; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                IP = pitcher_stats["inningsPitched"]
                outs = IP.split(".")[1]
                outs = int(outs) * 3
                IP = f'{IP.split(".")[0]}.{outs}'
                IP_formatted = float(IP)
                return IP_formatted
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get innings pitched for pitcher {pitcher}")
            return None
    else:
        return None


def get_K9(pitcher: str) -> float:
    """
    Gets the strikeouts per 9 innings.

    :param pitcher: the name of the pitcher whose IP are being accessed
    :returns: the K/9 as a float; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                K9 = pitcher_stats["strikeoutsPer9Inn"]
                return float(K9)
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get K/9 for pitcher {pitcher}")
            return None
    else:
        return None


def get_BB9(pitcher: str) -> float:
    """
    Gets the number of walks per 9 innings

    :param pitcher: the name of the pitcher whose IP are being accessed
    :returns: the BB/9 pitched as a float; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                bb9 = pitcher_stats["walksPer9Inn"]
                return float(bb9)
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get BB/9 for pitcher {pitcher}")
            return None
    else:
        return None


def get_K_BB_diff(pitcher: str) -> float:
    """
    Gets the strikeout percentage minus the walk percentage of a pitcher

    :param pitcher: the name of the pitcher whose IP are being accessed
    :returns: the K% - BB% as a float; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                k_perc = float(pitcher_stats["strikeOuts"]) / float(
                    pitcher_stats["battersFaced"]
                )
                bb_perc = float(pitcher_stats["baseOnBalls"]) / float(
                    pitcher_stats["battersFaced"]
                )
                diff = k_perc - bb_perc
                return diff
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get K% - BB% for pitcher {pitcher}")
            return None
    else:
        return None


def get_WHIP(pitcher: str) -> float:
    """
    Gets the walks and hits per innings pitched for a pitcher.

    :param pitcher: the name of the pitcher whose IP are being accessed
    :returns: the WHIP as a float; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                WHIP = pitcher_stats["whip"]
                return float(WHIP)
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get WHIP for pitcher {pitcher}")
            return None
    else:
        return None


def get_BABIP(pitcher: str) -> float:
    """
    Gets the batting average on balls in play for a pitcher.

    More on BABIP: https://library.fangraphs.com/pitching/babip/

    :param pitcher: the name of the pitcher whose IP are being accessed
    :returns: the BABIP as a float; None if pitcher's ID cannot be found
    """
    pitcher_id = lookup_player(pitcher)

    if pitcher_id:
        try:
            pitcher_stats = statsapi.player_stat_data(
                personId=pitcher_id, group="pitching", type="season", sportId=1
            )["stats"][0]["stats"]
            try:
                hits = float(pitcher_stats["hits"])
                home_runs = float(pitcher_stats["homeRuns"])
                at_bats = float(pitcher_stats["atBats"])
                strikeouts = float(pitcher_stats["strikeOuts"])
                sac_flies = float(pitcher_stats["sacFlies"])

                BABIP = (hits - home_runs) / (
                    at_bats - strikeouts - home_runs + sac_flies
                )

                return BABIP
            except ValueError:
                return None
        except IndexError:
            print(f"Unable to get BABIP for pitcher {pitcher}")
            return None
    else:
        return None


def config_struct_log(file_name: str) -> structlog:
    """
    Configures the structured logging.

    :param file_name: the file to write the logs to
    :returns: an instance of of the structlog
    """
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.WriteLoggerFactory(file=file_name),
    )

    return structlog.get_logger()


def send_email():
    prepared_html_list = ""
    updated_html_list = ""

    for game in prepared:
        prepared_html_list = prepared_html_list + f"<li>{game}</li>"

    for game in updated:
        updated_html_list = updated_html_list + f"<li>{game}</li>"

    html = f"""
        <h1 id="mlb-pipeline-today-">MLB Pipeline {datetime.now().strftime("%m/%d/%Y")}</h1>
            <h2 id="games-updated">Games Updated</h2>
                <p>There were {len(updated)} games updated:</p>
                <p><ul>{updated_html_list}</ul></p>
            <h2 id="games-prepared">Games Prepared</h2>
                <p>There were {len(prepared)} games added:</p>
                <p><ul>{prepared_html_list}</ul></p>
            <p><em>Email sent {datetime.now().strftime("%m/%d/%Y %H:%M:%S")}</em></p>
        """

    email_message = MIMEMultipart()
    email_message["From"] = EMAIL_FROM
    email_message["To"] = EMAIL_TO
    email_message["Subject"] = f"MLB Pipeline Update"

    email_message.attach(MIMEText(html, "html"))
    email_string = email_message.as_string()

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, email_string)

    print(f"\nEmail sent to {EMAIL_TO}.")


def send_error_email(method, error):
    html = f"""
        <h1 id="mlb-pipeline-today-">MLB Pipeline {datetime.now().strftime("%m/%d/%Y")}</h1>
            <p>There was an error when trying to run the pipeline. Please see below:</p>
            <h2 id="where">Where</h2>
            <p>The error occurred in {method}</p>
            <h2 id="what">What</h2>
            <p>Error message: </br> {error}</p>
            <h2 id="when">When</h2>
            <p>The error occurred at {datetime.now()}</p>

        """

    email_message = MIMEMultipart()
    email_message["From"] = EMAIL_FROM
    email_message["To"] = EMAIL_TO
    email_message["Subject"] = f"MLB Pipeline ERROR"

    email_message.attach(MIMEText(html, "html"))
    email_string = email_message.as_string()

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, email_string)

    print(f"\nEmail sent to {EMAIL_TO}.")


def send_error_email(method, error):
    html = f"""
        <h1 id="mlb-pipeline-today-">MLB Pipeline {datetime.now().strftime("%m/%d/%Y")}</h1>
            <p>There was an error when trying to run the pipeline. Please see below:</p>
            <h2 id="where">Where</h2>
            <p>The error occurred in {method}</p>
            <h2 id="what">What</h2>
            <p>Error message: </br> {error}</p>
            <h2 id="when">When</h2>
            <p>The error occurred at {datetime.now()}</p>

        """

    email_message = MIMEMultipart()
    email_message["From"] = EMAIL_FROM
    email_message["To"] = EMAIL_TO
    email_message["Subject"] = f"MLB Pipeline ERROR"

    email_message.attach(MIMEText(html, "html"))
    email_string = email_message.as_string()

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, email_string)

    print(f"\nEmail sent to {EMAIL_TO}.")


def update_games():
    start_time = time.time()

    temp = tempfile.NamedTemporaryFile(prefix=current_time, suffix="_temp", mode="w")

    logger = config_struct_log(temp)

    yesterday = datetime.now() - timedelta(1)
    yesterday = datetime.strftime(yesterday, "%m/%d/%Y")

    try:
        sched = statsapi.schedule(date=yesterday)
    except:
        print(f"An error occurred when trying to get games for {yesterday}")
    # sched = statsapi.schedule(date="8/25/2022")  # use for testing purposes

    cursor = aws_psql_conn.cursor()

    sql = f"UPDATE {TABLE_NAME} set winning_team=(%s) where game_id=(%s)"
    for i, game in enumerate(sched):
        print(f"Updating: {i + 1} of {len(sched)}...")

        try:
            winning_team = (
                statsapi.lookup_team(game["winning_team"])[0]["id"]
                if "winning_team" in game
                else None
            )
        except:
            print(
                "There is no winning team, implying that this game may have ended in a tie. Winner has been set to None."
            )
            winning_team = None

        record_to_insert = (
            winning_team,
            game["game_id"],
        )

        updated.append(
            f'Game ID {game["game_id"]} had the winner set to {winning_team}.'
        )

        cursor.execute(
            sql,
            (record_to_insert),
        )
        aws_psql_conn.commit()
        print(
            cursor.rowcount,
            f"record(s) inserted successfully into {TABLE_NAME} table.\n",
        )

        logger.info(
            event="game_updated",
            game_id=game["game_id"],
            away_team=game["away_name"],
            home_team=game["home_name"],
            game_date=game["game_date"],
            winning_team=record_to_insert,
        )

    key = f"{current_time}_updated_games"

    s3.meta.client.upload_file(
        Filename=temp.name,
        Bucket=S3_BUCKET_NAME,
        Key=key,
    )
    print(f"{temp.name} has been successfully uploaded to {S3_BUCKET_NAME} as {key}\n")
    print(
        f"------------------------------------------------\nFinished updating games at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\nTotal time to update games: {timedelta(seconds=(time.time() - start_time))}\n------------------------------------------------"
    )


def prepare_games():
    start_time = time.time()

    temp = tempfile.NamedTemporaryFile(prefix=current_time, suffix="_temp", mode="w")

    logger = config_struct_log(temp)

    date = datetime.strftime(datetime.now(), "%m/%d/%Y")

    try:
        sched = statsapi.schedule(date=date)
    except:
        print(f"An error occurred when trying to get games for {date}")
        return None
    # sched = statsapi.schedule(date="8/26/2022")  # use for testing purposes

    cursor = aws_psql_conn.cursor()

    sql = f"INSERT INTO {TABLE_NAME} (game_id, home_team_id, home_team_name, away_team_id, away_team_name, home_pitcher, home_pitcher_id, home_pitcher_era, home_pitcher_win_percentage, home_pitcher_wins, home_pitcher_losses, home_pitcher_innings_pitched, away_pitcher, away_pitcher_id, away_pitcher_era, away_pitcher_win_percentage, away_pitcher_wins, away_pitcher_losses, away_pitcher_innings_pitched, home_pitcher_k_nine, home_pitcher_bb_nine, home_pitcher_k_bb_diff, home_pitcher_whip, home_pitcher_babip, away_pitcher_k_nine, away_pitcher_bb_nine, away_pitcher_k_bb_diff, away_pitcher_whip, away_pitcher_babip) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for i, game in enumerate(sched):
        print(f"Preparing: {i + 1} of {len(sched)}...")
        home_probable_pitcher = game["home_probable_pitcher"]
        away_probable_pitcher = game["away_probable_pitcher"]

        record_to_insert = (
            game["game_id"],
            game["home_id"],
            game["home_name"],
            game["away_id"],
            game["away_name"],
            home_probable_pitcher,
            (
                statsapi.lookup_player(home_probable_pitcher)[0]["id"]
                if statsapi.lookup_player(home_probable_pitcher)
                else None
            ),
            get_ERA(home_probable_pitcher),
            get_win_percentage(home_probable_pitcher),
            get_wins(home_probable_pitcher),
            get_losses(home_probable_pitcher),
            get_IP(home_probable_pitcher),
            away_probable_pitcher,
            (
                statsapi.lookup_player(away_probable_pitcher)[0]["id"]
                if statsapi.lookup_player(away_probable_pitcher)
                else None
            ),
            get_ERA(away_probable_pitcher),
            get_win_percentage(away_probable_pitcher),
            get_wins(away_probable_pitcher),
            get_losses(away_probable_pitcher),
            get_IP(away_probable_pitcher),
            get_K9(home_probable_pitcher),
            get_BB9(home_probable_pitcher),
            get_K_BB_diff(home_probable_pitcher),
            get_WHIP(home_probable_pitcher),
            get_BABIP(home_probable_pitcher),
            get_K9(away_probable_pitcher),
            get_BB9(away_probable_pitcher),
            get_K_BB_diff(away_probable_pitcher),
            get_WHIP(away_probable_pitcher),
            get_BABIP(away_probable_pitcher),
        )

        prepared.append(
            f'{game["away_name"]} @ {game["home_name"]}, game ID {game["game_id"]}.'
        )

        cursor.execute(sql, record_to_insert)
        aws_psql_conn.commit()
        print(
            cursor.rowcount,
            f"record(s) inserted successfully into {TABLE_NAME} table.\n",
        )

        logger.info(
            event="game_prepared",
            game_id=game["game_id"],
            away_team=game["away_name"],
            home_team=game["home_name"],
            game_date=game["game_date"],
        )

    key = f"{current_time}_prepared_games"

    s3.meta.client.upload_file(
        Filename=temp.name,
        Bucket=S3_BUCKET_NAME,
        Key=key,
    )
    print(f"{temp.name} has been successfully uploaded to {S3_BUCKET_NAME} as {key}\n")

    print(
        f"------------------------------------------------\nFinished preparing games at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\nTotal time to prepare games: {timedelta(seconds=(time.time() - start_time))}\n------------------------------------------------"
    )


def main():
    error_occurred = False
    try:
        print("Trying to update games...")
        update_games()
    except Exception as e:
        print(f"Error occurred updating games: {e}")
        send_error_email("update_games()", e)
        error_occurred = True
    try:
        print("Trying to prepare games...")
        prepare_games()
    except Exception as e:
        print(f"Error occurred preparing games: {e}")
        send_error_email("prepare_games()", e)
        error_occurred = True

    if not error_occurred:
        send_email()
        return {
            "statusCode": 200,
            "body": json.dumps(
                "Script has successfully run. Check logs for further status updates."
            ),
        }

    if error_occurred:
        return {
            "statusCode": 400,
            "body": json.dumps(
                "There has been an error when running the script. Check logs for further status updates."
            ),
        }


main()
