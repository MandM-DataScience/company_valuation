import traceback
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

import mongodb
from edgar_utils import company_from_cik, AAPL_CIK, download_all_cik_submissions, download_submissions_documents
from openai_interface import summarize_section
from postgresql import get_df_from_table, country_to_region


def sections_summary(url):
    """
    Summarize all sections of a document using openAI API.
    Upsert summary on mongodb (overwrite previous one, in case we make changes to openai_interface)
    :param url: url of the document, used as id on mongodb
    :return:
    """
    doc = mongodb.get_document("documents",url)
    parsed_doc = mongodb.get_document("parsed_documents", url)
    company = company_from_cik(doc["cik"])

    result = {"_id": doc["_id"],
              "name": company["name"],
              "ticker": company["ticker"],
              "form_type": doc["form_type"],
              "filing_date": doc["filing_date"]}

    for section_title, section_text in parsed_doc.items():

        # if no section to summarize, skip
        if section_title == "_id" or len(section_text) == 0:
            continue

        # get summary from openAI model
        summary = summarize_section(company, doc["form_type"], doc["filing_date"], section_title, section_text)
        result[section_title] = summary

    mongodb.upsert_document("items_summary", result)


def extract_segments(doc):
    """
    Extract segments information (industry, geographical) from document
    :param url: url of the document, used as id on mongodb
    :return: list of dictionaries {"date": date, "segment":{"axis":"member", ...}, "value": number, "measure": "measure/metric"}
    """

    # doc = mongodb.get_document("documents", url)
    page = doc["html"]
    soap = BeautifulSoup(page, features="html.parser")

    ix_resources = soap.find("ix:resources")

    if ix_resources is None:
        return

    contexts = ix_resources.findAll("xbrli:context")

    axis = [
        "srt:ProductOrServiceAxis",
        "us-gaap:StatementBusinessSegmentsAxis",
        "srt:ConsolidationItemsAxis",
        "srt:StatementGeographicalAxis",
    ]

    result = []

    for c in contexts:

        context_id = c["id"]
        s = c.find("xbrli:segment")

        if s is not None:

            members = s.find_all("xbrldi:explicitmember")
            if len(members) == 0:
                continue

            include = True
            for m in members:
                if m["dimension"] not in axis:
                    include = False
                    break
            if not include:
                continue

            try:
                period = c.find("xbrli:enddate").text
            except:
                period = c.find("xbrli:instant").text
            period = datetime.strptime(period, "%Y-%m-%d").date()

            element = soap.find("ix:nonfraction", attrs={"contextref": context_id})
            if element is None or "name" not in element.attrs:
                continue

            try:
                value = float(element.text.replace(",",""))
            except:
                continue

            segment = {}
            for m in members:
                segment[m["dimension"]] = m.text

            result.append({
                "date": period,
                "segment": segment,
                "value": value,
                "measure": element["name"]
            })

    return result


def map_geographic_area(string):
    if "other" in string and ("region" in string or "countr" in string or "continent" in string):
        return "Global"
    elif "foreign" in string:
        return "Global"
    elif "europe" in string:
        return "Western Europe"
    elif "asia" in string:
        return "Asia"
    elif "emea" in string:
        return "EMEA" # 70% western europe, 15% middle east, 15% africa
    elif "apac" in string:
        return "APAC" # 90% asia, 10% australia
    elif "lacc" in string:
        return "LACC" # 50% central & south america, 40% canada, 10% caribbean
    elif "centralandsouthamerica" in string or "southamerica" in string or "americas" in string:
        return "Central and South America"
    elif "africa" in string:
        return "Africa"
    elif "middleeast" in string:
        return "Middle East"
    elif "northamerica" in string:
        return "North America"


def geography_distribution(segments, ticker):

    df = pd.DataFrame(segments)

    if df.empty:
        return df

    df["segment"] = df["segment"].astype(str)

    # filter by geography segments
    df = df[(df["segment"].str.contains('srt:StatementGeographicalAxis'))&
        ~(df["segment"].str.contains("srt:ProductOrServiceAxis"))&
        ~(df["segment"].str.contains("us-gaap:StatementBusinessSegmentsAxis"))]

    # print(df.to_markdown())

    # filter by measure
    measures = list(df["measure"].unique())

    selected_measure = None

    for m in measures:
        if "revenue" in m.lower() and ticker in m.lower():
            selected_measure = m
            break

    if selected_measure is None:
        for m in [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
            "OperatingIncomeLoss",
            "IncomeLossFromContinuingOperationsBeforeInterestExpenseInterestIncomeIncomeTaxesExtraordinaryItemsNoncontrollingInterestsNet",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesForeign",
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic",
            "NetIncomeLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic",
            "NetIncomeLossAvailableToCommonStockholdersDiluted",
            "ComprehensiveIncomeNetOfTax",
            "IncomeLossFromContinuingOperations",
            "ProfitLoss",
            "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
            "IncomeLossFromSubsidiariesNetOfTax"
        ]:
            if f"us-gaap:{m}" in measures:
                selected_measure = f"us-gaap:{m}"
                break

    df = df[df["measure"] == selected_measure]

    df = df[df.groupby(["segment","measure"])['date'].transform('max') == df['date']]\
        .drop(["date","measure"], axis=1)

    # print(df.to_markdown())


    # if only 'srt:StatementGeographicalAxis'
    df["segment"] = df["segment"].apply(lambda x:
                                        x[x.find("'srt:StatementGeographicalAxis':")+len("'srt:StatementGeographicalAxis':"):]
                                        .split("}")[0].split(",")[0].split(":")[1].split("'")[0])

    # MAP SEGMENTS
    # 1st try and match countries
    country_stats = get_df_from_table("damodaran_country_stats", most_recent=True)[["country","alpha_2_code"]]
    df = pd.merge(df, country_stats, left_on="segment", right_on="alpha_2_code", how="left").drop("alpha_2_code", axis=1)

    # 2st try and map regions
    df["area"] = df["segment"].apply(lambda x: map_geographic_area(x.lower()))

    # manage the rest
    df = df[~(df["country"].isna())|~(df["area"].isna())]

    df["value"] /= df["value"].sum()
    df["country_area"] = df["country"].fillna(df["area"])

    aggregate_areas_df = pd.DataFrame([
        {"country_area": "EMEA", "part_area": "Western Europe", "area_percent": 0.7},
        {"country_area": "EMEA", "part_area": "Middle East", "area_percent": 0.15},
        {"country_area": "EMEA", "part_area": "Africa", "area_percent": 0.15},
        {"country_area": "APAC", "part_area": "Asia", "area_percent": 0.9},
        {"country_area": "APAC", "part_area": "Australia & New Zealand", "area_percent": 0.1},
        {"country_area": "LACC", "part_area": "Central and South America", "area_percent": 0.5},
        {"country_area": "LACC", "part_area": "Canada", "area_percent": 0.4},
        {"country_area": "LACC", "part_area": "Caribbean", "area_percent": 0.1},
    ])

    # print(df.to_markdown())
    df = pd.merge(df, aggregate_areas_df, how="left", left_on="country_area", right_on="country_area")
    df["part_area"] = df["part_area"].fillna(df["country_area"])
    df["area_percent"] = df["area_percent"].fillna(1)
    df = df.drop("country_area", axis=1)
    df = df.rename(columns={"part_area":"country_area"})
    df["value"] = df["value"] * df["area_percent"]
    df["region"] = df["country_area"].apply(lambda x: country_to_region[x])

    # print(df.to_markdown())

    return df.drop(["segment","country","area", "area_percent"], axis=1)


def try_geo_segments():

    # url = "https://www.sec.gov/Archives/edgar/data/1666138/000166613822000128/atkr-20220930.htm" # ATKR 10-k
    # url = "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm" # AAPL 10-K
    # url = "https://www.sec.gov/Archives/edgar/data/1800/000162828023004026/abt-20221231.htm" # ABT 10-K
    # url = "https://www.sec.gov/Archives/edgar/data/2098/000156459023003422/acu-10k_20221231.htm" # ACU 10-K
    # url = "https://www.sec.gov/Archives/edgar/data/4447/000162828023005059/hes-20221231.htm" # HES 10-K

    docs = mongodb.get_collection_documents("documents")
    for doc in docs:
        if doc["form_type"] != "10-K":
            continue

        print(doc["_id"])
        ticker = doc["_id"].split("/")[-1].split("-")[0]
        segments = extract_segments(doc)
        geography_distribution(segments, ticker)


    # segments = extract_segments(url)
    # geography_distribution(segments, "hes")


def get_last_document(cik, form_type):
    collection = mongodb.get_collection("documents")
    docs = collection.find({"cik": cik, "form_type": form_type})

    last_doc = None
    last_date = None
    for doc in docs:
        filing_date = datetime.strptime(doc["filing_date"], "%Y-%m-%d")
        if last_date is None or filing_date > last_date:
            last_date = filing_date
            last_doc = doc

    if last_doc is None:
        download_all_cik_submissions(cik)
        download_submissions_documents(cik)
        return get_last_document(cik, form_type)

    return last_doc

if __name__ == '__main__':

    # doc = get_last_document(AAPL_CIK, "10-K")
    # print(doc["_id"])

    try_geo_segments()