import requests
import pandas as pd
from bs4 import BeautifulSoup, element
import datetime
from google.oauth2 import service_account

# Autenticação no BigQuery
key_file = r"C:\Users\nayya\Downloads\Estudo\projetos\desafio-beAnalytic-engdadosjr\credentials\projeto-key.json"
credentials = service_account.Credentials.from_service_account_file(
    key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Scraping dos Dados
headers = {
    "authority": "steamdb.info",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "max-age=0",
    "content-type": "application/x-www-form-urlencoded",
    "cookie": "cf_chl_2=6aea11f94091fb2; cf_clearance=JBW_rM4Qq2MaBk6xnMJG37RP.Va_DHBPfNysw6vM704-1688421692-0-160; __cf_bm=E0YlDKRYDk1pik0CjW6uyZc9IKW28NlMBDBuYc1Wirw-1688422242-0-ASuD4L7JwQy7SdOlANZHd2tZATnOii1GLnLXjy3UDC6N4jl73z8aAuGdkCW7Jb01hHGwwhKRu+nqyd21+RhEjfo=",
    "dnt": "1",
    "origin": "https://steamdb.info",
    "referer": "https://steamdb.info/sales/?__cf_chl_tk=iOssqADThWuoQZf.pPHZ1ZMefyeuvSqIeK2Mi3i8ChA-1688343216-0-gaNycGzNDRA",
    "sec-ch-ua": '^\^"Not.A/Brand^\^";v=^\^"8^\^", ^\^"Chromium^\^";v=^\^"114^\^", ^\^"Google Chrome^\^";v=^\^"114^\^"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
}

data_request = requests.get("https://steamdb.info/sales/", headers=headers)


def extract_table_rows(html_data: str) -> element.ResultSet:
    soup = BeautifulSoup(html_data, "html.parser")
    items = soup.find_all("tr", {"class": "app"})
    return items


def extract_data_from_rows(items: element.ResultSet) -> list[dict]:
    items_data = []

    for item in items:
        tds = item.findAll("td")

        name = tds[2].text.strip("\n").split("\n\n")[0]
        try:
            all_time_low = float(
                tds[2]
                .text.strip("\n")
                .split("\n\n")[1]
                .split("$ ")[1]
                .replace(",", ".")
            )
        except:
            all_time_low = None

        other_fields = tds[3:]
        other_fields_values = [field["data-sort"] for field in other_fields]
        (
            discount_in_percent,
            price_in_brl,
            rating_in_percent,
            end_time_in_seconds,
            start_time_in_second,
            release_time_in_second,
        ) = other_fields_values

        discount_in_percent = int(discount_in_percent)
        price_in_brl = float(price_in_brl) / 100
        rating_in_percent = float(rating_in_percent)
        end_time_in_seconds = int(end_time_in_seconds)
        start_time_in_second = int(start_time_in_second)
        release_time_in_second = int(release_time_in_second)

        items_data.append(
            {
                "name": name,
                "discount_in_percent": discount_in_percent,
                "price_in_brl": price_in_brl,
                "all_time_low": all_time_low,
                "rating_in_percent": rating_in_percent,
                "end_time_in_seconds": end_time_in_seconds,
                "start_time_in_second": start_time_in_second,
                "release_time_in_second": release_time_in_second,
            }
        )

    return items_data


items = extract_table_rows(data_request.text)
items_data = extract_data_from_rows(items)
df = pd.DataFrame(items_data)


# Função para converter o timestamp em formato de data
def convert_timestamp_to_date(timestamp):
    return datetime.datetime.fromtimestamp(timestamp)


# Converter colunas de timestamp para formato de data
timestamp_columns = [
    "end_time_in_seconds",
    "start_time_in_second",
    "release_time_in_second",
]

for column in timestamp_columns:
    df[column] = df[column].apply(convert_timestamp_to_date)


# Enviar para o BigQuery
df.to_gbq(
    credentials=credentials,
    destination_table="projeto-beanalytic.steamdb.sales",
    if_exists="replace",
)

df.to_csv(
    r"C:\Users\nayya\Downloads\Estudo\projetos\desafio-beAnalytic-engdadosjr\data\sales.csv"
)
