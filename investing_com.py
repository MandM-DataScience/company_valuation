import requests
import urllib3
from bs4 import BeautifulSoup

currency_country = {
    "AUD": "Australia",
    "BRL": "Brazil",
    "CAD": "Canada",
    "CHF": "Switzerland",
    "CLP": "Chile",
    "CNY": "China",
    "COP": "Colombia",
    "CZK": "Czech Republic",
    "DKK": "Denmark",
    "EGP": "Egypt",
    "EUR": "Germany",
    "GBP": "United Kingdom",
    "HKD": "Hong Kong",
    "HUF": "Hungary",
    "IDR": "Indonesia",
    "ILS": "Israel",
    "INR": "India",
    "ISK": "Iceland",
    "JPY": "Japan",
    "KRW": "South Korea",
    "KZT": "Kazakhstan",
    "MXN": "Mexico",
    "MYR": "Malaysia",
    "NGN": "Nigeria",
    "NOK": "Norway",
    "NZD": "New Zealand",
    "PHP": "Philippines",
    "PLN": "Poland",
    "QAR": "Qatar",
    "RUB": "Russia",
    "SGD": "Singapore",
    "THB": "Thailand",
    "TRY": "Turkey",
    "TWD": "Taiwan",
    "USD": "United States",
    "ZAR": "South Africa",
}
country_url = {
    "Australia": "https://www.investing.com/rates-bonds/australia-10-year-bond-yield",
    "Austria": "https://www.investing.com/rates-bonds/austria-10-year-bond-yield",
    "Belgium": "https://www.investing.com/rates-bonds/belguim-10-year-bond-yield",
    "Brazil": "https://www.investing.com/rates-bonds/brazil-10-year-bond-yield",
    "Canada": "https://www.investing.com/rates-bonds/canada-10-year-bond-yield",
    "Chile": "https://www.investing.com/rates-bonds/chile-10-year-bond-yield",
    "China": "https://www.investing.com/rates-bonds/china-10-year-bond-yield",
    "Colombia": "https://www.investing.com/rates-bonds/colombia-10-year-bond-yield",
    "Cyprus": "https://www.investing.com/rates-bonds/cyprus-10-year",
    "Czech Republic": "https://www.investing.com/rates-bonds/czech-republic-10-year-bond-yield",
    "Denmark": "https://www.investing.com/rates-bonds/denmark-10-year-bond-yield",
    "Egypt": "https://www.investing.com/rates-bonds/egypt-10-year-bond-yield",
    "Finland": "https://www.investing.com/rates-bonds/finland-10-year-bond-yield",
    "France": "https://www.investing.com/rates-bonds/france-10-year-bond-yield",
    "Germany": "https://www.investing.com/rates-bonds/germany-10-year-bond-yield",
    "Greece": "https://www.investing.com/rates-bonds/greece-10-year-bond-yield",
    "Hong Kong": "https://www.investing.com/rates-bonds/hong-kong-10-year-bond-yield",
    "Hungary": "https://www.investing.com/rates-bonds/hungary-10-year-bond-yield",
    "Iceland": "https://www.investing.com/rates-bonds/iceland-10-year-bond-yield",
    "India": "https://www.investing.com/rates-bonds/india-10-year-bond-yield",
    "Indonesia": "https://www.investing.com/rates-bonds/indonesia-10-year-bond-yield",
    "Ireland": "https://www.investing.com/rates-bonds/ireland-10-year-bond-yield",
    "Israel": "https://www.investing.com/rates-bonds/israel-10-year-bond-yield",
    "Italy": "https://www.investing.com/rates-bonds/italy-10-year-bond-yield",
    "Japan": "https://www.investing.com/rates-bonds/japan-10-year-bond-yield",
    "Kazakhstan": "https://www.investing.com/rates-bonds/kazakhstan-10-year",
    "Malaysia": "https://www.investing.com/rates-bonds/malaysia-10-year-bond-yield",
    "Malta": "https://www.investing.com/rates-bonds/malta-10-year",
    "Mauritius": "https://www.investing.com/rates-bonds/mauritius-10-year",
    "Mexico": "https://www.investing.com/rates-bonds/mexico-10-year",
    "Netherlands": "https://www.investing.com/rates-bonds/netherlands-10-year-bond-yield",
    "New Zealand": "https://www.investing.com/rates-bonds/new-zealand-10-years-bond-yield",
    "Nigeria": "https://www.investing.com/rates-bonds/nigeria-10-year",
    "Norway": "https://www.investing.com/rates-bonds/norway-10-year-bond-yield",
    "Philippines": "https://www.investing.com/rates-bonds/philippines-10-year-bond-yield",
    "Poland": "https://www.investing.com/rates-bonds/poland-10-year-bond-yield",
    "Portugal": "https://www.investing.com/rates-bonds/portugal-10-year-bond-yield",
    "Qatar": "https://www.investing.com/rates-bonds/qatar-10-year-bond-yield",
    "Russia": "https://www.investing.com/rates-bonds/russia-10-year-bond-yield",
    "Singapore": "https://www.investing.com/rates-bonds/singapore-10-year-bond-yield",
    "South Africa": "https://www.investing.com/rates-bonds/south-africa-10-year-bond-yield",
    "South Korea": "https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield",
    "Spain": "https://www.investing.com/rates-bonds/spain-10-year-bond-yield",
    "Switzerland": "https://www.investing.com/rates-bonds/switzerland-10-year-bond-yield",
    "Taiwan": "https://www.investing.com/rates-bonds/taiwan-10-year-bond-yield",
    "Thailand": "https://www.investing.com/rates-bonds/thailand-10-year-bond-yield",
    "Turkey": "https://www.investing.com/rates-bonds/turkey-10-year-bond-yield",
    "United Kingdom": "https://www.investing.com/rates-bonds/uk-10-year-bond-yield",
    "United States": "https://www.investing.com/rates-bonds/u.s.-10-year-bond-yield",
    "Vietnam": "https://www.investing.com/rates-bonds/vietnam-10-year-bond-yield"
}

def get_10y_bond_yield(currency):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if currency not in currency_country:
        return None, None

    url = country_url[currency_country[currency]]
    headers = {
        'accept': 'text/plain, */*; q=0.01',
        'accept-encoding': 'gzip, deflate, utf-8',
        'accept-language': 'en,it-IT;q=0.9,it;q=0.8,en-US;q=0.7',
        'cache-control': 'no-cache',
        'origin': 'https://www.investing.com',
        'pragma': 'no-cache',
        'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'upgrade-insecure-requests': '1',
    }

    bondyield = None
    retries = 0
    max_retries = 3
    while bondyield is None:
        response = request_with_retries(url, headers=headers)

        # with open("response.html", "w", encoding="utf-8") as f:
        #     f.write(response.text)

        soup = BeautifulSoup(response.text, 'html.parser')
        span = soup.select_one('dd[data-test="prevClose"]')
        try:
            bondyield = round(float(span.text) / 100, 5)
        except:
            print("ERROR in getting riskfree", url)
            retries += 1
            if retries >= max_retries:
                break

    return bondyield, currency_country[currency]

def request_with_retries(url, headers=None):

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    resp = None
    max_retry = 5
    retry = 0

    while resp is None and retry < max_retry:
        try:
            if headers is not None:
                resp = requests.get(url, verify=False, headers=headers)
            else:
                resp = requests.get(url, verify=False)
        except:
            print(f"{url} conn err - retry")
        retry += 1
    return resp