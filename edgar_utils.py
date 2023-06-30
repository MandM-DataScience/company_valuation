import datetime
import time

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from pymongo.errors import DocumentTooLarge

import mongodb

CIK_TICKER_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
AAPL_CIK = "0000320193"
BABA_CIK = "0001577552"
ATKR_CIK = "0001666138"
META_CIK = "0001326801"
_8K_URL = "https://www.sec.gov/Archives/edgar/data/320193/000114036123023909/ny20007635x4_8k.htm"

def make_edgar_request(url):
    '''
    Make a request to EDGAR (Electronic Data Gathering, Analysis and Retrieval)
    :param url:
    :return: response
    '''
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Accept-Encoding": "gzip, deflate, br",
        # "Host": "www.sec.gov",
        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        # "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        # "Cache-Control": "no-cache",
        # "Dnt": "1",
        # "Pragma": "no-cache",
        # "Sec-Ch-Ua": '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        # "Sec-Ch-Ua-Mobile": "?0",
        # "Sec-Ch-Ua-Platform": '"Windows"',
        # "Sec-Fetch-Mode": "navigate",
        # "Sec-Fetch-Dest": "document",
        # "Sec-Fetch-Site": "none",
        # "Sec-Fetch-User": "?1",
        # "Upgrade-Insecure-Requests":"1",
        # "Cookie": "__utma=27389246.804353070.1685091901.1685093973.1685093973.1; __utmz=27389246.1685093973.1.1.utmcsr=sec.gov|utmccn=(referral)|utmcmd=referral|utmcct=/; bm_mi=B82D20C974658C457E67541C1FA06E92~YAAQrf4SAgnVbUOIAQAA5kTaZhP8SH44Ep4Y7qv74ji3U46x7MtoMEhdCWYuo+5f9bOrS6O0wBC14p6pGxEmHQdow2YE3NW8W/16PZ6ku5jMGAmKUvQaB7G/Xvd97td0SDTJHTIy9jL91d6yjqAosAbilIVsZh0MkVqOWshsZhgk6um90vrxajxh+EFv5ctg6DlrbJdDxGwgSchQF9u9NQ33h3widFIh3STZUyaGWsLg0wsY8wns4Q9F+hxzXqY7HNnHxq2ubUrgf6nByZeBddTG3eZ6kz97Sk/DjiTudr7VyCJSp/KmeRCMKw==~1; _gid=GA1.2.1868488376.1685352763; ak_bmsc=9EB490152B324B861B40FC3A30C3362A~000000000000000000000000000000~YAAQrf4SAtfVbUOIAQAAjVXaZhOc7ebHss/IuJnTU36x32ytoEOBEhXFg6Y6Saan5JyIEVgqQiS9daPT9Mz+9jsL0AWJi92xVIpMfkaO8VjmkvKq6szWZAloUPUg8oCo1kopo33cZtQZtAgTcbit46B/X7UEtMFF5o0CzfC8C9kDFS0lEO99bGp3YFyNZT659bMVSiES9f/CdiuLlL9e3X96KrZRQyO815Iv8ywEP6st30e/b+MNiSQtsIwjcaMSVeDN/fWwkxEjSpD4RjoPF4yNpbp/nePWbCY5CecUATzzaWub1qGZRBbUZ5AMFE29XPY3LxvAW9yGkeccZUextfUjj9JewyYxDPP88e83zHGpUcKWvn9pxAo/kYo6W1DQkKKoBSd9D9WknoW/gmcsYvjzpHaK; _ga_300V1CHKH1=GS1.1.1685355929.4.0.1685355929.0.0.0; _ga=GA1.2.804353070.1685091901; _4c_=%7B%22_4c_s_%22%3A%22dZJdT8MgFIb%2FysL12kHpB%2FTOzMR4oUbjx%2BUy4WwlbqOhONSl%2F91D1xldYm9anvOclxPogYQGdqRmpSh4UUhORUmn5A0%2BO1IfiDM6vvakJrQEIZZUJlW%2BqpJ8tVTJUuFSCckKUJnkwMmUfMQsmYmykpSKivZTonenDAcaOrPe%2FfVYUeUCPdP6UYzTUMmwIqvizEUS3VOkPs861l34iToWsozLv1EDQbV1Z7v%2Br6p2VA9EWQ24PZMpEylNVh3O4b8iySiJoVa%2FK7%2Fwn23UArxOOv2GBQ17o2ARjPbN0D%2FoI23ArBsfMRUDjrMRhl%2FB7LQN520j%2FWmTvEL66mzoIHbOG2e3MGEs3ozFGyUvQ0cc1sEKnBu0xvu2q2ezEEK6tna9gVTZ7Qylzvg4fgcKC%2FsR4M9xZMmRPZtI9eRx%2FoD89he5n9%2FdjOjqYvF0fYkLQXNecFrR9HTelJF%2BPG5R5HnGMylkhsfpN6QWZU7j0%2Ff9Nw%3D%3D%22%7D; bm_sv=53960FB1953DF5F2F69291ED0CF24E84~YAAQJhTfrdbrcjWIAQAA2xgMZxOH1e+/OnF9QgXOnTuGnbqBep0aTe+AZznJ/O006oMAcMg2t37FQN+XnZDULq9IKkefXyT88NjcX9blHQmi01Yew6BtdF9g+Veoklyw/EoyiKH31X+dCjmXRxTn1eKnD19Y3aEF0aWmNrjpCksWzfHAH8noXJrNMCtbMimxLHWqY4mkCZovpZzz99zdcIXjHtucokMKTJNdkUHusQE59sxA1ecIw1Ltf8474g==~1"
    }
    return requests.get(url, headers=headers)

def download_cik_ticker_map():
    '''
    Get a mapping of cik (Central Index Key, id of company on edgar) and ticker on the exchange.
    It saves this mapping in mongodb.
    :return:
    '''
    response = make_edgar_request(CIK_TICKER_URL)
    r = response.json()
    r["_id"] = "cik_ticker"
    mongodb.upsert_document("cik_ticker", r)


def get_df_cik_ticker_map():
    '''
    Create DataFrame from cik ticker document on mongodb.
    :return: DataFrame
    '''
    try:
        cik_ticker = mongodb.get_collection_documents("cik_ticker").next()
    except StopIteration:
        print("cik ticker document not found")
        return

    df = pd.DataFrame(cik_ticker["data"], columns=cik_ticker["fields"])

    # add leading 0s to cik (always 10 digits)
    df["cik"] = df.apply(lambda x: add_trailing_to_cik(x["cik"]), axis=1)
    return df


def company_from_cik(cik):
    '''
    Get company info from cik
    :param cik: company id on edgar
    :return: DataFrame row with company information (name, ticker, exchange)
    '''
    df = get_df_cik_ticker_map()
    try:
        return df[df["cik"] == cik].iloc[0]
    except IndexError:
        return None

def cik_from_ticker(ticker):
    '''
    Get company cik from ticker
    :param ticker: company ticker
    :return: cik (company id on edgar)
    '''
    df = get_df_cik_ticker_map()

    try:
        cik = df[df["ticker"] == ticker]["cik"].iloc[0]
    except:
        cik = -1
    return cik

def download_all_cik_submissions(cik):
    '''
    Get list of submissions for a single company.
    Upsert this list on mongodb (each download contains all the submissions).
    :param cik: cik of the company
    :return:
    '''
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    response = make_edgar_request(url)
    r = response.json()
    r["_id"] = cik
    mongodb.upsert_document("submissions", r)

def download_submissions_documents(cik, forms_to_download=("10-Q", "10-K", "8-K"), years=5):
    '''
    Download all documents for submissions forms 'forms_to_download' for the past 'max_history' years.
    Insert them on mongodb.
    :param cik: company cik
    :return:
    '''

    try:
        submissions = mongodb.get_document("submissions", cik)
    except StopIteration:
        print(f"submissions file not found in mongodb for {cik}")
        return

    cik_no_trailing = submissions["cik"]
    filings = submissions["filings"]["recent"]

    for i in range(len(filings["filingDate"])):

        filing_date = filings['filingDate'][i]
        difference_in_years = relativedelta(datetime.date.today(),
                                            datetime.datetime.strptime(filing_date, "%Y-%m-%d")).years

        # as the document are ordered cronologically when we reach the max history we can return
        if difference_in_years > years:
            return

        form_type = filings['form'][i]

        if form_type not in forms_to_download:
            continue

        accession_no_symbols = filings["accessionNumber"][i].replace("-","")
        primary_document = filings["primaryDocument"][i]
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_trailing}/{accession_no_symbols}/{primary_document}"

        # if we already have the document, we don't download it again
        if mongodb.check_document_exists("documents", url):
            continue

        print(f"{filing_date} ({form_type}): {url}")
        download_document(url, cik, form_type, filing_date)

        # insert a quick sleep to avoid reaching edgar rate limit
        time.sleep(0.2)

def download_document(url, cik, form_type, filing_date, updated_at=None):
    '''
    Download and insert submission document
    :param url:
    :param cik:
    :param form_type:
    :param filing_date:
    :return:
    '''
    response = make_edgar_request(url)
    r = response.text
    doc = {"html": r, "cik": cik, "form_type": form_type, "filing_date": filing_date, "updated_at":updated_at, "_id":url}

    try:
        mongodb.insert_document("documents", doc)
    except DocumentTooLarge:
        print("Document too Large (over 16MB)", url)

def download_financial_data(cik):
    """
    Download financial data for a company.
    Upsert document on mongodb (each requests returns the entire history)
    :param cik:
    :return:
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    response = make_edgar_request(url)
    try:
        r = response.json()
        r["_id"] = cik
        r["url"] = url
        mongodb.upsert_document("financial_data", r)

    # ETFs, funds, trusts do not have financial information
    except:
        print(f"ERROR {cik} - {response} - {url}")
        print(company_from_cik(cik))

def get_filing_from_index(url):
    """
    Get the document url from the filing index page.
    This is a filing index page:
    https://www.sec.gov/Archives/edgar/data/320193/000114036123023909/0001140361-23-023909-index.htm
    The document url we want is the first url in the Document Format Files table.
    :param url: filing index page url
    :return:
    """
    index_page = make_edgar_request(url)
    soup = BeautifulSoup(index_page.text, "html.parser")
    table = soup.find("table", {"class":"tableFile", "summary":"Document Format Files"})
    return table.find("a")["href"]


def add_trailing_to_cik(cik_no_trailing):
    return "{:010d}".format(cik_no_trailing)


def get_size_in_bytes(size_string):
    size = int(size_string.split()[0])
    unit = size_string.split()[1].upper()

    if unit == "MB":
        return size * 1024 * 1024
    elif unit == "KB":
        return size * 1024
    else:
        raise ValueError("Invalid size unit. Must be either MB or KB.")


def get_latest_filings(form_type, start_date):

    """
    Get new filings (for all companies) since 'start_date' (yyyy-mm-dd).
    Insert new submission documents on mongodb.
    Insert new financial data on mongodb.

    Used to update submissions documents and financial data in our db.
    :param form_type: form that we want to request (you can pass multiple forms delimited by commas 10-K,10-Q,...
    :param start_date: date from where we want to retrieve new submissions
    :return:
    """

    start_idx = 0
    entries_per_request = 100
    done = False

    cik_df = get_df_cik_ticker_map()
    ciks = list(cik_df["cik"].unique())

    while not done:
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&datea={start_date}&" \
              f"start={start_idx}&count={entries_per_request}&output=atom"
        print(f"{url}")
        response = make_edgar_request(url)

        soup = BeautifulSoup(response.text, 'xml')
        entries = soup.findAll("entry")

        # If the response contains less entry than what we requested it means we are done
        if len(entries) < entries_per_request:
            done = True

        for entry in entries:
            index_url = entry.find("link")["href"]
            entry_form_type = entry.find("category")["term"]
            entry_updated_at = entry.find("updated").text.split("T")[0]
            entry_summary = entry.find("summary").text.replace("<b>",";").replace("</b>","").replace("\n", "")
            filed_date = entry_summary.split(';')[1].split(":")[1].strip()
            size = get_size_in_bytes(entry_summary.split(';')[3].split(":")[1].strip())
            start_cik = index_url.find('data/') + 5
            end_cik = index_url.find('/', start_cik)
            cik = add_trailing_to_cik(int(index_url[start_cik: end_cik]))

            if cik not in ciks:
                print(f"{cik} not present in cik map - skip")
                continue

            if size > 16 * 1024 * 1024:
                print(f"SKIP {cik} because of size {size}")
                continue
            url = get_filing_from_index(index_url)
            url = f"https://www.sec.gov/{url.replace('/ix?doc=/','')}"

            # if we already have the document on mongodb we can skip
            if mongodb.check_document_exists("documents", url):
                continue

            download_document(url, cik, entry_form_type, filed_date, entry_updated_at)

            # if entry_form_type in ["10-Q", "10-Q/A" "10-K", "10-K/A"]:
            #     download_financial_data(cik)

        start_idx += entries_per_request


if __name__ == '__main__':
    get_latest_filings("10-K", "2023-01-01")
    # download_cik_ticker_map()
    # download_all_cik_submissions("0001326801")
    # download_submissions_documents("0001326801")