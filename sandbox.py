import mongodb
import pandas as pd

from quantitative_analysis import build_financial_df


def matrix_financial_metrics():

    """
    Plot a matrix to see how many companies have a certain financial metric
    :return:
    """

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

        i += 1
        if i % 100 == 0:
            print(f"{i} / {len(docs)}")

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

    shares_df = build_financial_df(doc, "EntityCommonStockSharesOutstanding", "shares", tax="dei")
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