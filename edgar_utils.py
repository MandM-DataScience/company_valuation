import datetime
import time

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from pymongo.errors import DocumentTooLarge

import mongodb

AAPL_CIK = "0000320193"
BABA_CIK = "0001577552"
ATKR_CIK = "0001666138"
META_CIK = "0001326801"
_8K_URL = "https://www.sec.gov/Archives/edgar/data/320193/000114036123023909/ny20007635x4_8k.htm"

def make_edgar_request(url):
    """
    Make a request to EDGAR (Electronic Data Gathering, Analysis and Retrieval)
    :param url:
    :return: response
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Accept-Encoding": "gzip, deflate, br",
    }
    return requests.get(url, headers=headers)


def download_cik_ticker_map():
    """
    Get a mapping of cik (Central Index Key, id of company on edgar) and ticker on the exchange.
    It saves this mapping in mongodb.
    """
    CIK_TICKER_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
    response = make_edgar_request(CIK_TICKER_URL)
    r = response.json()
    r["_id"] = "cik_ticker"
    mongodb.upsert_document("cik_ticker", r)


def get_df_cik_ticker_map():
    """
    Create DataFrame from cik ticker document on mongodb.
    :return: DataFrame
    """
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
    """
    Get company info from cik
    :param cik: company id on EDGAR
    :return: DataFrame row with company information (name, ticker, exchange)
    """
    df = get_df_cik_ticker_map()
    try:
        return df[df["cik"] == cik].iloc[0]
    except IndexError:
        return None

def cik_from_ticker(ticker):
    """
    Get company cik from ticker
    :param ticker: company ticker
    :return: cik (company id on EDGAR)
    """
    df = get_df_cik_ticker_map()
    try:
        cik = df[df["ticker"] == ticker]["cik"].iloc[0]
    except:
        cik = -1
    return cik

def download_all_cik_submissions(cik):
    """
    Get list of submissions for a single company.
    Upsert this list on mongodb (each download contains all the submissions).
    :param cik: cik of the company
    :return:
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    response = make_edgar_request(url)
    r = response.json()
    r["_id"] = cik
    mongodb.upsert_document("submissions", r)


def download_submissions_documents(cik, forms_to_download=("10-Q", "10-K", "8-K"), years=5):
    """
    Download all documents for submissions forms 'forms_to_download' for the past 'max_history' years.
    Insert them on mongodb.
    :param cik: company cik
    :param forms_to_download: a tuple containing the form types to download
    :param years: the max number of years to download
    :return:
    """
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
    """
    Download and insert submission document
    :param url:
    :param cik:
    :param form_type:
    :param filing_date:
    :return:
    """
    response = make_edgar_request(url)
    r = response.text
    doc = {"html": r, "cik": cik, "form_type": form_type, "filing_date": filing_date, "updated_at": updated_at, "_id": url}
    try:
        mongodb.insert_document("documents", doc)
    except DocumentTooLarge:
        # DocumenTooLarge is raised by mongodb when uploading files larger than 16MB
        # To avoid this it is better to save this kind of files in a separate storate like S3 and retriving them when needed.
        # Another options could be using mongofiles: https://www.mongodb.com/docs/database-tools/mongofiles/#mongodb-binary-bin.mongofiles
        # for management of large files saved in mongo db.
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
    table = soup.find("table", {"class": "tableFile", "summary": "Document Format Files"})
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
    apple_tiker = "AAPL"
    cik = cik_from_ticker(apple_tiker)
    download_all_cik_submissions(cik)
    # get_latest_filings("10-K", "2023-01-01")
    # download_cik_ticker_map()
    # download_all_cik_submissions("0001326801")
    # download_submissions_documents("0001326801")