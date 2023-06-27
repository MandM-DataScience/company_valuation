import time
from datetime import datetime
from ssl import SSLError

import yfinance as yf
import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from requests import ReadTimeout
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import MaxRetryError, ReadTimeoutError
import traceback

packet_stream_proxy = "http://easymap_buyer:34Qgo0O03zOhrx8h@proxy.packetstream.io:31112"
bright_data_proxy = 'http://brd-customer-hl_f8b1a708-zone-finance:u7iz73qdf9wv@brd.superproxy.io:22225'
proxy = bright_data_proxy

def request_yahoo_url(url):
    hed = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
           'Upgrade-Insecure-Requests': '1',
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15',
           'Cache-Control': 'PUBLIC'}
    cookies = {
        "EuConsent": "CPUNe08PUNe08AOACBITB-CoAP_AAH_AACiQIJNe_X__bX9n-_59__t0eY1f9_r3v-QzjhfNt-8F2L_W_L0H_2E7NB36pq4KuR4ku3bBIQFtHMnUTUmxaolVrzHsak2MpyNKJ7LkmnsZe2dYGHtPn9lD-YKZ7_7___f73z___9_-39z3_9f___d9_-__-vjfV_993________9nd____BBIAkw1LyALsSxwJNo0qhRAjCsJCoBQAUUAwtEVgAwOCnZWAT6ghYAITUBGBECDEFGDAIAAAIAkIiAkALBAIgCIBAACAFCAhAARMAgsALAwCAAUA0LEAKAAQJCDI4KjlMCAiRaKCWysQSgr2NMIAyywAoFEZFQgIlCCBYGQkLBzHAEgJYAYaADAAEEEhEAGAAIIJCoAMAAQQSA",
        "OTH": "v=1&d=eyJraWQiOiIwMTY0MGY5MDNhMjRlMWMxZjA5N2ViZGEyZDA5YjE5NmM5ZGUzZWQ5IiwiYWxnIjoiUlMyNTYifQ.eyJjdSI6eyJndWlkIjoiWVZURENIQVJDVFFUSVM3WDVBN0g0NzZYVDQiLCJwZXJzaXN0ZW50Ijp0cnVlLCJzaWQiOiJNZm1Bc291aHZTbzIifX0.Qz8bX4q6yUmgNqoxVogtnln1kNlA5oc9hhMFm_baVHvl2_gnK5almd6r-u_Wx4W9c9uhi2g9dvovheQr6DXlkGlG7Bw7OJubPeSGqy4asxOWAO4VNpUppmdK9kVuwOQIbnpg5skXXuGykmWRnUrtZH4resNBrOJhXgfUehIROpQ",
        "GUC": "AQAABgFiBrFi6kIhUQUB",
        "maex": "%7B%22v2%22%3A%7B%22106c4e0d%22%3A%7B%22lst%22%3A1644604741%2C%22ic%22%3A56%7D%7D%7D",
        "UIDR": "1599641610",
        "cmp": "v=22&t=1644604104&j=1",
        "PRF": "t%3DAAPL%252BABTG.MI%252BFB%252B%255ESOX%252BMSFT%252BSLV%252BKO%252BGOOG%252BVT%252BREET%252BBNDW%252BFCT.MI%252BISP.MI%252BCRES.MI%252BTRASTOR.AT",
        "A1S": "d=AQABBJDkvF8CEEnmdkmy3hsZxUP4oHXu3MoFEgAABgGxBmLqYudVb2UB9iMAAAcIf7Z8XzRwxloID4-gDUXX3Q7JnS7c59zqFwkBBwoBMA&S=AQAAAujaKZu-E9Ike-e7u6WnYmk&j=WORLD",
        "B": "5lhjg6hfnpdjv&b=4&d=Jw.N6YdtYFmKelHVCZg9&s=cc&i=j6ANRdfdDsmdLtzn3OoX",
    }
    proxies = {
        "http": proxy,
        "https": proxy,
    }
    response = None
    num_retry = 0
    max_retry = 5

    #return requests.get(url, headers=hed, proxies=proxies)

    while ((response is None or response.status_code == 403) and num_retry < max_retry):

        if num_retry > 0:
            print("# retry = "+str(num_retry)+", response = "+str(response)+", url = "+url)

        if num_retry > 0:
            time.sleep(0.5 * num_retry)
        try:
            response = requests.get(url, headers=hed, proxies=proxies, timeout=20)
        except requests.exceptions.SSLError:
            pass
        except requests.exceptions.ConnectionError:
            pass
        except ReadTimeout:
            pass
        except ChunkedEncodingError:
            pass
        except MaxRetryError:
            pass
        except ReadTimeoutError:
            pass
        except:
            print(traceback.format_exc())

        num_retry += 1

    return response

def get_premarket_price_yahoo(ticker):

    url = f"https://finance.yahoo.com/quote/{ticker}"
    response = request_yahoo_url(url)
    # network_size = len(response.content)

    if response is not None:
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')
            quote_header = soup.find("div", id="quote-header-info")

            if quote_header is None:
                return None#, network_size

            premarket = quote_header.select_one('fin-streamer[data-field="preMarketPrice"]')
            if premarket is not None:
                return float(premarket.text.replace(",",""))#, network_size

            regular = quote_header.select_one('fin-streamer[data-field="regularMarketPrice"]')
            if regular is not None:
                return float(regular.text.replace(",",""))#, network_size

    return None#, network_size

def get_current_price_from_yahoo(ticker, created_at=None):

    if created_at is None:
        price = get_premarket_price_yahoo(ticker)
        if price is not None:
            return price
        else:
            created_at = datetime.now().date()

    t = yf.Ticker(ticker)

    todays_data = None
    max_retry = 5
    retry = 0
    while todays_data is None and retry < max_retry:
        try:
            todays_data = t.history(start=created_at - relativedelta(days=5), end=created_at,
                                    interval="1m")

        except:
            print(f"{ticker} conn err - retry")
        retry += 1
    try:
        return todays_data['Close'][-1]
    except IndexError:
        return None
    except TypeError:
        return None