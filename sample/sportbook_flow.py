import prefect
import requests
import json
from prefect.tasks.notifications.slack_task import SlackTask
from prefect import task, Flow, Parameter
from prefect.tasks.secrets import PrefectSecret
from prefect.storage import Docker


api_key = PrefectSecret("SportsLines")

    
@task
def parse_data(data, home_team, away_team, bookmaker):
    teams = [x for x in data if x['home_team'] == home_team and x['away_team'] == away_team]
    for x in teams:
        for y in x['bookmakers']:
            if y['key'] == bookmaker:
                for z in y['markets']:
                    return z['outcomes']


@task
def get_odds(api_key):
    base_url = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?"
    regions = "&regions=us&markets=spreads&oddsFormat=american"
    key = "apiKey=" + api_key
    url = base_url + key + regions
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return data

@task
def create_slack_message(odds):
    team_1 = odds[0]['name']
    point_1 = odds[0]['point']
    team_2 = odds[1]['name']
    message = str(team_1) + ' vs ' + str(team_2) + ': ' + str(point_1)
    return message

@task
def log_task(data):
    logger = prefect.context.get("logger")
    logger.info(data)


notification  = SlackTask(webhook_secret="SALES_DEMO_NOTIFICATIONS_SLACK_URL")


with Flow(
    "sportsbook", 
    storage=Docker(
        registry_url="kaliserichmond",
        image_name="flows",
        image_tag="sportsbook-rich-flow",
        )) as flow:
    data = get_odds(api_key =api_key)
    # log_task(data)
    home_team = Parameter("home_team", default="Denver Broncos")
    away_team = Parameter("away_team", default="Philadelphia Eagles")
    bookmaker = Parameter("bookmaker", default="barstool")
    odds = parse_data(data, home_team, away_team, bookmaker)
    log_task(odds)
    message = create_slack_message(odds)
    log_task(message)

    notification(message=message)

flow.register(project_name="AE Demos")

















