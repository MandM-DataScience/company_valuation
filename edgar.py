import datetime
import time

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

import mongodb
from openai_interface import summarize_section

CIK_TICKER_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
AAPL_CIK = "0000320193"
BABA_CIK = "0001577552"
ATKR_CIK = "0001666138"
_8K_URL = "https://www.sec.gov/Archives/edgar/data/320193/000114036123023909/ny20007635x4_8k.htm"

def make_edgar_request(url):
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
    response = make_edgar_request(CIK_TICKER_URL)
    r = response.json()
    r["_id"] = "cik_ticker"
    mongodb.upsert_document("cik_ticker", r)

def cik_leading_zeros(row):
    return "{:010d}".format(row["cik"])

def read_cik_ticker_map():
    cik_ticker = mongodb.get_collection_documents("cik_ticker").next()
    df = pd.DataFrame(cik_ticker["data"], columns=cik_ticker["fields"])
    df["cik"] = df.apply(lambda x: cik_leading_zeros(x), axis=1)
    return df

def company_from_cik(cik):
    df = read_cik_ticker_map()
    return df[df["cik"] == cik].iloc[0]

def cik_from_ticker(ticker):
    df = read_cik_ticker_map()
    return df[df["ticker"] == ticker]["cik"].iloc[0]

def download_all_cik_submissions(cik):
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    response = make_edgar_request(url)
    r = response.json()
    r["_id"] = cik
    mongodb.upsert_document("submissions", r)

def download_submissions_documents(cik):

    submissions = mongodb.get_document("submissions", cik)
    cik_no_trailing = submissions["cik"]
    filings = submissions["filings"]["recent"]

    start_time = time.time()

    for i in range(len(filings["filingDate"])):

        filing_date = filings['filingDate'][i]
        difference_in_years = relativedelta(datetime.date.today(),
                                            datetime.datetime.strptime(filing_date, "%Y-%m-%d")).years

        if difference_in_years > 5:
            return

        form_type = filings['form'][i]

        if form_type not in ["10-Q", "10-K", "8-K"]:
            continue

        accession_no_symbols = filings["accessionNumber"][i].replace("-","")
        primary_document = filings["primaryDocument"][i]
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_no_trailing}/{accession_no_symbols}/{primary_document}"

        if mongodb.check_document_exists("documents", url):
            continue

        print(f"{filing_date} ({form_type}): {url} elapsed_time {time.time() - start_time}")
        download_document(url, cik, form_type, filing_date)
        time.sleep(0.2)

def download_document(url, cik, form_type, filing_date):

    response = make_edgar_request(url)
    r = response.text
    doc = {"html": r, "cik": cik, "form_type": form_type, "filing_date": filing_date, "_id":url}
    mongodb.insert_document("documents", doc)

def parse_document(url):

    doc = mongodb.get_document("documents",url)
    html = doc["html"]
    bs = BeautifulSoup(html, features="html.parser")

    section = None
    next_section = False
    result = {}

    if doc["form_type"] == "8-K":

        body = bs.body

        # remove tables
        for table in body.find_all("table"):
            table.decompose()

        divs = body.findAll("div")
        for div in divs:

            span = div.find("span")

            # A new section is identified as:
            # <div> <span> Item .... </span> </div> - new section is starting
            # <div> <span> Section name </span> </div> - section name
            # ....
            # <div> <span> Item .... </span> </div> - end previous section

            if span is not None:

                if next_section and not span.text.startswith("Item"):
                    section = span.text

                    # information contained in the document is finished
                    if section == "Financial Statements and Exhibits.":
                        break

                    result[section] = ""
                    next_section = False
                    continue

                if span.text.startswith("Item"):
                    next_section = True
                    continue

            if section is not None:
                text = div.findAll(string=True, recursive=False)
                for t in text:
                    if "SIGNATURE" in t.strip():
                        section = None
                        break

                    result[section] += t.strip()

    result["_id"] = doc["_id"]
    mongodb.upsert_document("parsed_documents", result)

def sections_summary(url):
    doc = mongodb.get_document("documents",url)
    parsed_doc = mongodb.get_document("parsed_documents", url)
    del parsed_doc["_id"]
    company = company_from_cik(doc["cik"])

    result = {"_id": doc["_id"],
              "name": company["name"],
              "ticker": company["ticker"],
              "form_type": doc["form_type"],
              "filing_date": doc["filing_date"]}

    for section_title, section_text in parsed_doc.items():
        if len(section_text) > 0:
            summary = summarize_section(company, doc["form_type"], doc["filing_date"], section_title, section_text)
            result[section_title] = summary

    mongodb.upsert_document("items_summary", result)

def download_financial_data(cik):
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    response = make_edgar_request(url)
    try:
        r = response.json()
        r["_id"] = cik
        r["url"] = url
        mongodb.upsert_document("financial_data", r)

    # ETFs, funds, trusts do not have financial information
    except:
        print(f"ERROR {cik}")
        print(response)

def build_financial_df(doc, measure, unit, tag="us-gaap"):

    try:
        data = doc["facts"][tag][measure]["units"][unit]
    except:
        return None

    df = pd.DataFrame(data)
    df["val"] = pd.to_numeric(df["val"])

    try:
        df["start"] = pd.to_datetime(df["start"])
    except:
        pass

    df["end"] = pd.to_datetime(df["end"])
    df["filed"] = pd.to_datetime(df["filed"])
    df = df[~df.frame.isna()]
    return df

def get_ttm_from_df(df):
    df["period"] = (df["end"] - df["start"]).dt.days
    df = df[~(df.frame.str.contains("Q")) | ((df.frame.str.contains("Q")) & (df.period < 100))]

    last_yearly_row = df[df.period > 100].iloc[-1]

    post_quarterly_rows = df[df.index > last_yearly_row.name]
    pre_frames = list(post_quarterly_rows.frame)
    pre_frames = [x[:2] + str(int(x[2:6]) - 1) + x[6:] for x in pre_frames]

    pre_quarterly_rows = df[df.frame.isin(pre_frames)]

    ttm = last_yearly_row.val + post_quarterly_rows.val.sum() - pre_quarterly_rows.val.sum()
    return ttm, last_yearly_row.name

def get_filing_from_index(url):

    index_page = make_edgar_request(url)
    soup = BeautifulSoup(index_page.text, "html.parser")
    table = soup.find("table", {"class":"tableFile", "summary":"Document Format Files"})
    return table.find("a")["href"]

def get_latest_filings(form_type, start_date):

    start_idx = 0
    entries_per_request = 5
    done = False

    while not done:
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&datea={start_date}&" \
              f"start={start_idx}&count={entries_per_request}&output=atom"

        response = make_edgar_request(url)

        soup = BeautifulSoup(response.text, 'xml')
        entries = soup.findAll("entry")

        if len(entries) < entries_per_request:
            done = True

        for entry in entries:

            cik = entry.find("id").text.split(":")[-1].split("=")[-1].split("-")[0]
            index_url = entry.find("link")["href"]

            url = get_filing_from_index(index_url)
            url = f"https://www.sec.gov/{url.replace('/ix?doc=/','')}"

            if mongodb.check_document_exists("documents", url):
                continue

            download_document(url, cik, form_type, start_date)

            if form_type in ["10-Q", "10-K"]:
                download_financial_data(cik)

def get_all_possible_measures_from_financial_data():

    result = []

    docs = mongodb.get_collection_documents("financial_data")
    for d in docs:

        for facts in d["facts"]:

            if facts == "dei":
                continue

            try:
                measures = d["facts"][facts]
            except:
                print(d["facts"].keys())
                return

            for m in measures:
                label = measures[m]["label"]
                desc = measures[m]["description"]
                units = next(iter(measures[m]["units"]))

                result.append({"measure":m, "label":label, "desc":desc, "units":units, "accounting":facts})

    df = pd.DataFrame(result)
    df = df.drop_duplicates()

    df.to_csv("financial_metrics.csv", index=False)

def get_USD_measures_from_financial_data():


    docs = mongodb.get_collection_documents("financial_data")
    result = []

    for d in docs:

        company = d["entityName"]

        for facts in d["facts"]:

            if facts != "us-gaap":
                continue

            try:
                measures = d["facts"][facts]
            except:
                print(d["facts"].keys())
                return

            for m in measures:

                label = measures[m]["label"]

                if label is not None and "deprecat" in label.lower():
                    continue

                desc = measures[m]["description"]
                units = next(iter(measures[m]["units"]))

                if units not in ["USD","shares"]:
                    continue

                start_date = measures[m]["units"][units][0]["end"]
                end_date = measures[m]["units"][units][-1]["end"]

                result.append({"measure":m, "label":label, "desc":desc, "units":units, "accounting":facts,
                               "company":company, "start":start_date, "end":end_date})

    df = pd.DataFrame(result)
    df = df.drop_duplicates()

    df.to_csv("financial_metrics_USD.csv", index=False)

def group_csv():
    df = pd.read_csv("financial_metrics_USD.csv")

    df["start"] = pd.to_datetime(df["start"])
    df["end"] = pd.to_datetime(df["end"])

    print(df.iloc[0:5].to_markdown())

    # for each measure, how many companies? min and max dates?
    g = df.groupby(["measure","label","desc","units","accounting"]).agg({"company":"count","start":"min","end":"max"}).reset_index()
    print(g.iloc[0:5].to_markdown())

    g.to_csv("financial_metrics_USD_group.csv", index=False)

def examine_financial_data(cik):

    measures_df = pd.read_csv("financial_metrics_USD_group.csv").fillna("-")
    # print(measures_df)

    groupings = measures_df[["DOC","GROUP","DETAIL"]].drop_duplicates().fillna("-").reset_index()
    # print(groupings)

    doc = mongodb.get_document("financial_data", cik)
    # import json
    # with open("ATKR.json", "w") as fp:
    #     json.dump(doc, fp)
    #
    # return

    shares_df = build_financial_df(doc, "EntityCommonStockSharesOutstanding", "shares", tag="dei")
    shares_df["DOC"] = "Shares"

    num_groupings = len(groupings)

    list_of_dfs = [shares_df]

    for i, r in groupings.iterrows():

        if r.GROUP != "Revenue":
            continue

        print(f"{i+1} / {num_groupings}", r.to_dict())
        measures = measures_df[(measures_df.DOC == r.DOC) & (measures_df.GROUP == r.GROUP) & (measures_df.DETAIL == r.DETAIL)]
        measures = measures["measure"].unique()

        for m in measures:

            try:
                unit = "USD"
                df = build_financial_df(doc, m, unit)
                df["DOC"] = r.DOC
                df["GROUP"] = r.GROUP
                df["DETAIL"] = r.DETAIL
                df["MEASURE"] = m

                print(df.to_markdown())

                list_of_dfs.append(df)
            except:
                print(f"ERROR {m}")
                continue

    df = pd.concat(list_of_dfs)
    df.to_csv(f"financial_data_{cik}.csv", index=False)

def get_most_recent_value(doc, measure, unit="USD", tag="us-gaap"):
    df = build_financial_df(doc, measure, unit, tag)

    if df is not None:
        return df.iloc[-1]["val"], df.iloc[-1]["end"]

    return None, None

def get_yearly_values(doc, measure, instant=False, last_annual_report_date=None, unit="USD", tag="us-gaap"):

    df = build_financial_df(doc, measure, unit, tag)

    if df is not None:

        # income statement
        if not instant:

            df = df[~df.frame.str.contains("Q")]
            return {"dates": list((df.frame.str.replace("CY", "")).astype(int)),
                    "values": list(df.val),
                    "last_annual_report_date": df.iloc[-1].end if len(df) > 0 else None}

        # balance sheet
        else:

            if last_annual_report_date is None:
                return

            last_annual_report_row = df[df.end == last_annual_report_date]

            if last_annual_report_row.empty:
                return

            quarter_of_annual_report = last_annual_report_row.iloc[0]["frame"][7]

            df = df[df.frame.str.contains(f"Q{quarter_of_annual_report}I")]
            return {"dates": list((df.frame.str.replace("CY", "")
                                   .str.replace(f"Q{quarter_of_annual_report}I","")).astype(int)),
                    "values": list(df.val),
                    "last_annual_report_date": df.iloc[-1].end}


def get_ttm_value(doc, measure, unit="USD", tag="us-gaap"):
    df = build_financial_df(doc, measure, unit, tag)

    if df is not None:
        return get_ttm_from_df(df)

    return None, None


def get_values_from_measures(doc, measures, get_ttm=True, get_most_recent=True, get_yearly=True, instant=False,
                             last_annual_report_date=None, debug=False):

    ttm = 0
    ttm_year = None
    most_recent = 0
    most_recent_date = None
    yearly = {"dates": [], "values": []}

    for m in measures:

        # We can create the financial df here and pass it to the methods

        if get_ttm:
            ttm_value_tmp, ttm_year_tmp = get_ttm_value(doc, m)

            if ttm_value_tmp is not None:
                if ttm_year is None or ttm_year_tmp > ttm_year:
                    ttm = ttm_value_tmp
                    ttm_year = ttm_year_tmp

                if debug:
                    print(m, ttm_year_tmp, ttm_value_tmp)

        if get_most_recent:
            most_recent_value_tmp, most_recent_date_tmp = get_most_recent_value(doc, m)

            if most_recent_value_tmp is not None:
                if most_recent_date is None or most_recent_date_tmp > most_recent_date:
                    most_recent_date = most_recent_date_tmp
                    most_recent = most_recent_value_tmp

                if debug:
                    print(m, most_recent_date_tmp, most_recent_value_tmp)

        if get_yearly:
            yearly_tmp = get_yearly_values(doc, m, instant, last_annual_report_date)

            if yearly_tmp is not None:

                for i, d in enumerate(yearly_tmp["dates"]):
                    if d not in yearly["dates"]:
                        yearly["dates"].append(d)
                        yearly["values"].append(yearly_tmp["values"][i])

                if last_annual_report_date is None or yearly_tmp["last_annual_report_date"] > last_annual_report_date:
                        last_annual_report_date = yearly_tmp["last_annual_report_date"]

                if debug:
                    print(m, yearly_tmp)

    sort = sorted(zip(yearly["dates"], yearly["values"]))
    yearly["dates"] = [x for x, _ in sort]
    yearly["values"] = [x for _, x in sort]
    yearly["last_annual_report_date"] = last_annual_report_date

    if debug:
        print("ttm", ttm)
        print("most recent", most_recent)
        print("yearly", yearly)

    return most_recent, ttm, yearly


def merge_subsets(superset, subsets, must_include=None):

    to_add = {"dates":[],"values":[]}

    # superset = cash+cashrestrictes
    # subset = cash & restricted

    if must_include is None:
        for s in subsets:
            for i, d in enumerate(s["dates"]):
                if d not in superset["dates"]:
                    if d not in to_add["dates"]:
                        to_add["dates"].append(d)
                        to_add["values"].append(s["values"][i])
                    else:
                        idx = to_add["dates"].index(d)
                        to_add["values"][idx] += s["values"][i]

    else:
        if not isinstance(must_include, tuple):
            raise Exception("must_include must be a tuple")

        tmp_dates = subsets[must_include[0]]["dates"]
        remove_dates = []
        for d in tmp_dates:
            for m in must_include:
                s = subsets[m]
                if d not in s["dates"]:
                    remove_dates.append(d)

        must_include_dates = [x for x in tmp_dates if x not in remove_dates and x not in superset["dates"]]

        if len(must_include_dates) == 0:
            return

        for m in must_include_dates:
            to_add["dates"].append(m)
            to_add["values"].append(0)

        for s in subsets:
            for i, d in enumerate(s["dates"]):
                if d in to_add["dates"]:
                    idx = to_add["dates"].index(d)
                    to_add["values"][idx] += s["values"][i]

    for i, d in enumerate(to_add["dates"]):
        superset["dates"].append(d)
        superset["values"].append(to_add["values"][i])

    sort = sorted(zip(superset["dates"], superset["values"]))
    superset["dates"] = [x for x, _ in sort]
    superset["values"] = [x for _, x in sort]


def extract_company_financial_information(cik, l=[]):

    pd.options.mode.chained_assignment = None
    doc = mongodb.get_document("financial_data", cik)

    #### SHARES ####
    shares = get_most_recent_value(doc, "EntityCommonStockSharesOutstanding", unit="shares", tag="dei")

    ######################
    ## INCOME STATEMENT ##
    ######################

    #### REVENUE ####
    measures = [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
    ]

    _, ttm_revenue, yearly_revenue = get_values_from_measures(doc, measures, get_most_recent=False, debug=False)

    #### R and D ####
    measures = ["ResearchAndDevelopmentExpense"]
    _, _, yearly_rd = get_values_from_measures(
        doc, measures, get_ttm=False, get_most_recent=False, debug=False)

    measures = ["ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"]
    _, _, yearly_rd_not_inprocess = get_values_from_measures(
        doc, measures, get_ttm=False, get_most_recent=False, debug=False)

    measures = ["ResearchAndDevelopmentInProcess"]
    _, _, yearly_rd_inprocess = get_values_from_measures(
        doc, measures, get_ttm=False, get_most_recent=False, debug=False)

    merge_subsets(yearly_rd, [yearly_rd_not_inprocess, yearly_rd_inprocess])

    #### Net Income ####
    measures = [
        "NetIncomeLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "NetIncomeLossAvailableToCommonStockholdersDiluted",
        "ComprehensiveIncomeNetOfTax",
        "IncomeLossFromContinuingOperations",

        # including minority interest
        "ProfitLoss",
        "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
        "IncomeLossFromSubsidiariesNetOfTax"
    ]

    _, ttm_net_income, yearly_net_income = get_values_from_measures(doc, measures, get_most_recent=False, debug=False)

    #### Interest Expenses ####
    measures = [
    "InterestExpense",
    "InterestAndDebtExpense",
    "InterestPaid",
    "InterestPaidNet",
    "InterestCostsIncurred"]

    _, ttm_interest_expenses, yearly_interest_expenses = get_values_from_measures(doc, measures, get_most_recent=False, debug=False)

    measures = ["InterestExpenseDebt",
    "InterestExpenseDebtExcludingAmortization"]
    _, ttm_ie_debt, yearly_ie_debt = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                  debug=False)

    measures = ["InterestExpenseLongTermDebt"]
    _, ttm_ie_debt_lt, yearly_ie_debt_lt = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                  debug=False)

    measures = ["InterestExpenseShortTermBorrowings"]
    _, ttm_ie_debt_st, yearly_ie_debt_st = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                  debug=False)
    merge_subsets(yearly_ie_debt, [yearly_ie_debt_lt, yearly_ie_debt_st])

    measures = ["InterestExpenseBorrowings"]
    _, ttm_ie_borrowings, yearly_ie_borrowings = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                    debug=False)
    measures = ["InterestExpenseDeposits"]
    _, ttm_ie_deposits, yearly_ie_deposits = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                    debug=False)
    measures = ["InterestExpenseOther"]
    _, ttm_ie_others, yearly_ie_others = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                    debug=False)
    measures = ["InterestExpenseRelatedParty"]
    _, ttm_ie_related, yearly_ie_related = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                    debug=False)

    merge_subsets(yearly_ie_borrowings, [yearly_ie_debt, yearly_ie_deposits, yearly_ie_others, yearly_ie_related])
    merge_subsets(yearly_interest_expenses, [yearly_ie_borrowings])

    #### Gross Profit ####
    measures = ["Gross Profit"]
    _, ttm_gross_profit, yearly_gross_profit = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                    debug=False)

    #### Depreciation ####

    measures = [
    "DepreciationDepletionAndAmortization",
    "DepreciationAmortizationAndAccretionNet"]
    _, ttm_depreciation_amortization, yearly_depreciation_amortization = get_values_from_measures(doc, measures, get_most_recent=False,
                                                              debug=False)

    # sum depreciation and amortization
    measures = ["Depreciation"]
    _, ttm_depreciation, yearly_depreciation = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)

    # sum to get amortization
    measures = ["AmortizationOfFinancingCostsAndDiscounts"]
    _, ttm_amortization_fincost_disc, yearly_amortization_fincost_disc = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)
    # sum to get fin.costs + discounts
    measures = ["AmortizationOfDebtDiscountPremium"]
    _, ttm_amortization_disc, yearly_amortization_disc = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)
    measures = ["AmortizationOfFinancingCosts"]
    _, ttm_amortization_fincost, yearly_amortization_fincost = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)

    merge_subsets(yearly_amortization_fincost_disc, [yearly_amortization_disc, yearly_amortization_fincost])

    measures = ["AmortizationOfDeferredCharges"]
    _, ttm_amortization_charges, yearly_amortization_charges = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)
    measures = ["AmortizationOfDeferredSalesCommissions"]
    _, ttm_amortization_comm, yearly_amortization_comm = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)
    measures = ["AmortizationOfIntangibleAssets"]
    _, ttm_amortization_intan, yearly_amortization_intan = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                                  debug=False)

    merge_subsets(yearly_amortization_fincost_disc, [yearly_amortization_charges, yearly_amortization_comm, yearly_amortization_intan])
    merge_subsets(yearly_depreciation_amortization, [yearly_depreciation, yearly_amortization_fincost_disc])

    #### EBIT ####
    measures = ["OperatingIncomeLoss",
                "IncomeLossFromContinuingOperationsBeforeInterestExpenseInterestIncomeIncomeTaxesExtraordinaryItemsNoncontrollingInterestsNet",
                "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
                "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
                "IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign",
                "IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic",
                ]
    _, ttm_ebit, yearly_ebit = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)

    ######################
    ## BALANCE SHEET    ##
    ######################

    last_annual_report_date = yearly_revenue["last_annual_report_date"]
    # print("LAST", last_annual_report_date)

    #### ASSETS ####
    measures = ["Assets"]
    most_recent_assets, _, yearly_assets = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### Current Assets ####
    measures = ["AssetsCurrent"]
    most_recent_current_assets, _, yearly_current_assets = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### CASH ####
    measures = ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
    most_recent_cash_and_restricted, _, yearly_cash_and_restricted = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["CashAndCashEquivalentsAtCarryingValue", "Cash"]
    most_recent_cash, _, yearly_cash = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "RestrictedCashAndCashEquivalentsAtCarryingValue",
        "RestrictedCashAndCashEquivalents",
        "RestrictedCash",
        "RestrictedCashAndInvestmentsCurrent",
        "RestrictedCashCurrent"
    ]
    most_recent_restrictedcash, _, yearly_restrictedcash = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets(yearly_cash_and_restricted, [yearly_cash, yearly_restrictedcash], must_include=(0, ))

    #### Inventory ####
    measures = [
        "InventoryNet",
        "InventoryGross",
        "FIFOInventoryAmount",
        "InventoryLIFOReserve",
        "LIFOInventoryAmount",
    ]
    most_recent_inventory, _, yearly_inventory = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### Other current assets ####
    measures = [
        "OtherAssetsCurrent",
        "OtherAssetsMiscellaneousCurrent",
        "PrepaidExpenseAndOtherAssetsCurrent",
        "OtherAssetsFairValueDisclosure",
        "OtherAssetsMiscellaneous",
        "PrepaidExpenseAndOtherAssets"
    ]
    most_recent_other_current_assets, _, yearly_other_current_assets = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["PrepaidExpenseCurrent"]
    most_recent_prepaid_exp, _, yearly_prepaid_exp = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)
    measures = ["PrepaidInsurance"]
    most_recent_prepaid_ins, _, yearly_prepaid_ins = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)
    measures = ["PrepaidTaxes",
                "IncomeTaxesReceivable",
                "IncomeTaxReceivable"]
    most_recent_prepaid_tax, _, yearly_prepaid_tax = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)
    merge_subsets(yearly_other_current_assets, [yearly_prepaid_exp, yearly_prepaid_ins, yearly_prepaid_tax])

    #### Receivables ####
    measures = [
        # full receivables
        "AccountsAndOtherReceivablesNetCurrent",
        "AccountsNotesAndLoansReceivableNetCurrent",
        "ReceivablesNetCurrent",
        "NontradeReceivablesCurrent"]
    most_recent_receivables, _, yearly_receivables = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    # account receivables
    measures = ["AccountsReceivableNetCurrent",
        "AccountsReceivableNet",
        "AccountsReceivableGrossCurrent",
        "AccountsReceivableGross"]
    most_recent_ar, _, yearly_ar = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

        # loan receivables
    measures = ["LoansAndLeasesReceivableNetReportedAmount",
        "LoansAndLeasesReceivableNetOfDeferredIncome",
        "LoansReceivableFairValueDisclosure",
        "LoansAndLeasesReceivableGrossCarryingAmount"]
    most_recent_loans_rec, _, yearly_loans_rec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)
        
        # notes receivables
    measures = ["NotesReceivableNet",
        "NotesReceivableFairValueDisclosure",
        "NotesReceivableGross"]
    most_recent_notes_rec, _, yearly_notes_rec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets(yearly_receivables, [yearly_ar, yearly_loans_rec, yearly_notes_rec])

    measures = [
        # full
        "MarketableSecurities"
        "AvailableForSaleSecurities"]
    most_recent_securities, _, yearly_securities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

        # to sum high level
    measures = ["AvailableForSaleSecuritiesDebtSecurities"]
    most_recent_debtsecurities, _, yearly_debtsecurities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)
    measures = ["AvailableForSaleSecuritiesEquitySecurities"]
    most_recent_equitysecurities, _, yearly_equitysecurities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets(yearly_securities, [yearly_debtsecurities, yearly_equitysecurities])

    measures = ["DerivativeAssets",
                "DerivativeAssetsCurrent"]
    most_recent_derivatives, _, yearly_derivatives = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["HeldToMaturitySecurities",
                "HeldToMaturitySecuritiesFairValue",
                "HeldToMaturitySecuritiesCurrent",
                ]
    most_recent_held_securities, _, yearly_held_securities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesNoncurrent",
                "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent",
                ]
    most_recent_non_curr_sec, _, yearly_non_curr_sec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["MarketableSecuritiesCurrent"]
    most_recent_marksecurities_cur, _, yearly_marksecurities_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["ShortTermInvestments"]
    most_recent_st_inv, _, yearly_st_inv = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["MoneyMarketFundsAtCarryingValue"]
    most_recent_mm, _, yearly_mm = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesDebtSecuritiesCurrent"]
    most_recent_debtsecurities_cur, _, yearly_debtsecurities_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets(yearly_securities, [yearly_derivatives, yearly_held_securities, yearly_non_curr_sec,
                                      yearly_marksecurities_cur, yearly_st_inv, yearly_mm, yearly_debtsecurities_cur])

    merge_subsets(yearly_current_assets, [yearly_cash_and_restricted, yearly_inventory, yearly_other_current_assets,
                                          yearly_receivables, yearly_securities])

    ##### Non current assets ####
    measures = ["AssetsNoncurrent",
                "NoncurrentAssets"]
    most_recent_non_curr_asset, _, yearly_non_curr_asset = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### Equity investments ####
    measures = [
        "EquityMethodInvestmentAggregateCost",
        "EquityMethodInvestments",
        "InvestmentOwnedAtCost",
        "Investments",
        "InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures",
    ]
    most_recent_equity_investments, _, yearly_equity_investments = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "EquityMethodInvestmentsFairValueDisclosure",
        "InvestmentOwnedAtFairValue",
        "InvestmentsFairValueDisclosure",
    ]
    most_recent_equity_inv_fv, _, yearly_equity_inv_fv = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["EquitySecuritiesWithoutReadilyDeterminableFairValueAmount",]
    most_recent_equity_inv_notfv, _, yearly_equity_inv_notfv = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets(yearly_equity_investments, [yearly_equity_inv_fv, yearly_equity_inv_notfv])

    measures = ["MarketableSecuritiesNoncurrent",]
    most_recent_securities_non_curr, _, yearly_securities_non_curr = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    yearly_equity_investments_and_securities = {"dates":[], "values":[]}
    merge_subsets(yearly_equity_investments_and_securities, [yearly_equity_investments, yearly_securities_non_curr])

    #### Other financial assets ####
    measures = [
        "PrepaidExpenseNoncurrent",
        "PrepaidExpenseOtherNoncurrent",
    ]
    most_recent_prepaid_non_curr, _, yearly_prepaid_non_curr = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "RestrictedCashAndCashEquivalentsNoncurrent",
        "RestrictedCashAndInvestmentsNoncurrent",
        "RestrictedCashNoncurrent"
    ]
    most_recent_cash_non_curr, _, yearly_cash_non_curr = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["DerivativeAssetsNoncurrent", ]
    most_recent_derivatives_non_curr, _, yearly_derivatives_non_curr = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["EscrowDeposit"]
    most_recent_escrow, _, yearly_escrow = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    yearly_other_financial_assets = {"dates":[], "values":[]}
    merge_subsets(yearly_other_financial_assets, [yearly_prepaid_non_curr, yearly_cash_non_curr,
                                                  yearly_derivatives_non_curr, yearly_escrow])

    #### PP&E ####
    measures = [
        "PropertyPlantAndEquipmentNet",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization"
    ]
    most_recent_ppe, _, yearly_ppe = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### Investment property ####
    measures = [
        "RealEstateInvestments",
        "RealEstateInvestmentPropertyNet",
        "RealEstateInvestmentPropertyAtCost",
        "RealEstateHeldforsale"
    ]
    most_recent_property, _, yearly_property = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["InvestmentBuildingAndBuildingImprovements"]
    most_recent_buildings, _, yearly_buildings = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "LandAndLandImprovements",
        "Land",
    ]
    most_recent_land, _, yearly_land = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets(yearly_property, [yearly_buildings, yearly_land])

    merge_subsets(yearly_non_curr_asset, [yearly_property, yearly_ppe, yearly_other_financial_assets,
                                          yearly_equity_investments_and_securities])

    merge_subsets(yearly_assets, [yearly_current_assets, yearly_non_curr_asset])

    measures = [
        "UnrecognizedTaxBenefits",
        "UnrecognizedTaxBenefitsThatWouldImpactEffectiveTaxRate",
        "IncomeTaxesReceivableNoncurrent",
    ]
    most_recent_tax_benefit, _, yearly_tax_benefit = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### LIABILITIES ####
    measures = [
        "Liabilities",
        "LiabilitiesFairValueDisclosure",
        "LiabilitiesAssumed1",
    ]
    most_recent_liabilities, _, yearly_liabilities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### Debt ####
    measures = [

        # full debt
        "DebtLongtermAndShorttermCombinedAmount",
        "DebtInstrumentFaceAmount",
        "DebtInstrumentFairValue",
        "LongTermDebt",
        "LongTermDebtFairValue",
        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",

        "LineOfCredit",
        "LineOfCreditFacilityFairValueOfAmountOutstanding",
        "ConvertibleNotesPayable",
        "ConvertibleDebt",
        "ConvertibleDebtFairValueDisclosures",
        "MortgageLoansOnRealEstate",
        "OtherBorrowings",

        # long term debt
        "LongTermDebtNoncurrent",
        "DebtInstrumentCarryingAmount",
        "LongTermDebtAndCapitalLeaseObligations",
        
        "LongTermLineOfCredit",
        "LongTermNotesPayable",
        "ConvertibleLongTermNotesPayable",
        "ConvertibleDebtNoncurrent",

        # short term debt
        "DebtCurrent",
        "LongTermDebtCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        
        "BankOverdrafts",
        "BridgeLoan",
        "CommercialPaper",
        "CommercialPaperAtCarryingValue",
        "ConvertibleDebtCurrent",
        "LoansPayable",
        "LinesOfCreditCurrent",

    ]

    # TODO Complete Debt

    #### CURRENT Liabilities ####
    #### Non Current Liabilities ####
    #### EQUITY ####
    #### EQUITY + LIABILITIES ####

    #### CF Statement ####

def matrix_fin_values():

    docs = mongodb.get_collection_documents("financial_data")
    measures = [
        "OtherAssetsCurrent",
        "OtherAssetsFairValueDisclosure",
        "OtherAssetsMiscellaneous",
        "OtherAssetsMiscellaneousCurrent",
        "PrepaidExpenseAndOtherAssetsCurrent",
        "PrepaidExpenseCurrent",
        "PrepaidInsurance",
        "PrepaidTaxes"
    ]

    l = []

    i = 0
    for doc in docs:

        i+=1
        print(i)

        d = {"company": doc["entityName"]}

        for m in measures:
            try:
                num = len(doc["facts"]["us-gaap"][m]["units"]["USD"])
            except:
                num = 0
            d[m] = num

        l.append(d)

    df = pd.DataFrame(l)
    print(df.to_markdown())


def main():

    # download_cik_ticker_map()
    # read_cik_ticker_map()
    # download_all_cik_submissions(ATKR_CIK)
    # download_submissions_documents(ATKR_CIK)
    # download_document(_8K_URL, AAPL_CIK, "8-K", "2023-05-10")
    # parse_document(_8K_URL)
    # sections_summary(_8K_URL)

    # cik = cik_from_ticker("CLF")
    # # download_financial_data(cik)
    # read_financial_data(cik)

    # get_latest_filings("8-K", "2023-05-30")
    # print(cik_from_ticker("ATKR"))

    # print(df.to_markdown())
    # get_all_possible_measures_from_financial_data()
    # group_csv()
    # extract_company_financial_information(ATKR_CIK)

    # matrix_fin_values()

    # examine_financial_data("0000860730")
    # print()

    extract_company_financial_information(AAPL_CIK)



if __name__ == '__main__':
    main()