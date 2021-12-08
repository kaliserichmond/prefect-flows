import prefect
import requests
import re

from prefect import task, Flow, Parameter, case
from prefect.tasks.notifications.slack_task import SlackTask
from bs4 import BeautifulSoup
from prefect.storage import Docker


nikeURL = "https://www.nike.com/t/air-max-270-womens-shoes-Pgb94t/AH6789-601"

@task
def find_nike_price():
    k = requests.get(nikeURL).text
    soup = BeautifulSoup(k,'html.parser')
    price_string = soup.find('div', {"class":"product-price"}).text
    price_string = price_string.replace(' ','')
    price = int(re.search('[0-9]+',price_string).group(0))
    return price

@task
def compare_price(price, buy_price):
    if price <= buy_price:
        return True
    return False


# Notification Task sends message to Cloud once authenticated with a webhook
buy_notification, dont_buy_notification = SlackTask(
    message="BUY the nikes!",
    webhook_secret="SALES_DEMO_NOTIFICATIONS_SLACK_URL",
), SlackTask(
    message="DONT BUY the nikes!",
    webhook_secret="SALES_DEMO_NOTIFICATIONS_SLACK_URL",
)

with Flow(
    "Nike Flow",
     storage=Docker(
        registry_url="kaliserichmond",
        image_name="flows",
        image_tag="nike-flow",
        python_dependencies=['beautifulsoup4']

    )) as flow:
    price = find_nike_price()
    buy_price = Parameter("buy_price", default=120)

    buy = compare_price(price, buy_price)
    with case(buy, True):
        buy_notification()
    with case(buy, False):
        dont_buy_notification()

flow.register(project_name='AE Demos')