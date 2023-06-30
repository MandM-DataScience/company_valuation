import math
import traceback

import pandas as pd
import numpy as np
import mongodb
from edgar_utils import ATKR_CIK, company_from_cik, AAPL_CIK, cik_from_ticker, download_financial_data
from postgresql import get_df_from_table, get_generic_info
from qualitative_analysis import get_last_document, extract_segments, geography_distribution, get_recent_docs, \
    sections_summary
from utils import parse_document, find_auditor
from valuation_helper import convert_currencies, get_target_info, get_normalized_info, get_dividends_info, \
    get_final_info, calculate_liquidation_value, dividends_valuation, fcff_valuation, get_status, summary_valuation, \
    r_and_d_amortization, get_growth_ttm, capitalize_rd, debtize_op_leases, get_roe_roc, get_spread_from_dscr, \
    company_complexity, company_share_diluition, get_company_type, currency_bond_yield, get_industry_data
from yahoo_finance import get_current_price_from_yahoo

EARNINGS_TTM = "EARNINGS_TTM"
EARNINGS_NORM = "EARNINGS_NORM"
GROWTH_FIXED = "GROWTH_FIXED"
GROWTH_TTM = "GROWTH_TTM"
GROWTH_NORM = "GROWTH_NORM"

STATUS_OK = "OK"
STATUS_NI = "NI"
STATUS_KO = "KO"

def build_financial_df(doc, measure, unit="USD", tax="us-gaap"):

    """
    Build a DataFrame from a company financial document (containing all history and all measures).
    DataFrame is built on a specific subsection of the document, identified with taxonomy, measure, unit
    :param doc: company financial document
    :param measure: measure we are interest in
    :param unit: unit of measure (usually is a single one for each measure)
    :param tax: taxonomy
    :return: DataFrame
    """

    try:
        data = doc["facts"][tax][measure]["units"][unit]
    except:
        return None

    df = pd.DataFrame(data)
    df["val"] = pd.to_numeric(df["val"])

    # for income statement or cashflow statement measures we have a start and end date to represent a period.
    # for example Revenues, we need to know the period (start-end) in which they have been generated.
    # for balance sheet measures we have only end date as they are snapshot in time.
    # for example Cash, we just know the amount in a certain date, there is no start-date period concept.

    try:
        if "start" in df.columns:
            df["start"] = pd.to_datetime(df["start"])

        df["end"] = pd.to_datetime(df["end"])
        df["filed"] = pd.to_datetime(df["filed"])
    except:
        return None

    # print(measure, unit, tax)
    # print(df)

    try:
        df = df[~df.frame.isna()]
    except:
        df = df[0:0]

    return df

def get_ttm_from_df(df):
    """
    Compute TTM (trailing twelve months) value from a DataFrame containing quarterly and annual values.
    :param ttm_df: DataFrame containing quarterly and annual values
    :return: ttm value, year of last annual value in DataFrame
    """

    # create a copy as we are going to edit and filter it
    ttm_df = df.copy()

    # Get last annual value
    try:
        # Keep only annual and quarterly periods
        ttm_df["period"] = (ttm_df["end"] - ttm_df["start"]).dt.days
        ttm_df = ttm_df[~(ttm_df.frame.str.contains("Q")) | ((ttm_df.frame.str.contains("Q")) & (ttm_df.period < 100))]
        last_yearly_row = ttm_df[ttm_df.period > 100].iloc[-1]
    except:
        return None, None

    # Get quarterly values AFTER the annual value
    post_quarterly_rows = ttm_df[ttm_df.index > last_yearly_row.name]

    # Get corresponding quarterly values BEFORE the annual value
    pre_frames = list(post_quarterly_rows.frame)
    pre_frames = [x[:2] + str(int(x[2:6]) - 1) + x[6:] for x in pre_frames]
    pre_quarterly_rows = ttm_df[ttm_df.frame.isin(pre_frames)]

    # TTM = annual value + quarterly values after - corresponding quarterly values before
    ttm = last_yearly_row.val + post_quarterly_rows.val.sum() - pre_quarterly_rows.val.sum()

    return ttm, last_yearly_row.end

def get_most_recent_value_from_df(df):
    """
    Get most recent value and date in DataFrame (last row)
    :param df: DataFrame containing quarterly and annual values
    :return: most recent value and date in DataFrame
    """
    return {"date":df.iloc[-1]["end"], "value":df.iloc[-1]["val"]}

def get_last_annual_report_date_and_fy(df):

    if df is None:
        return None, None

    year_df = df[~df.frame.str.contains("Q")]
    dates = list((year_df.frame.str.replace("CY", "")).astype(int))

    last_annual_report_date = year_df.iloc[-1].end if len(year_df) > 0 else None
    last_annual_report_fy = dates[-1] if len(dates) > 0 else None

    return last_annual_report_date, last_annual_report_fy

def get_quarter_of_annual_report(df, last_annual_report_date, last_annual_report_fy):

    if df is None:
        return None, None

    last_annual_report_row = df[df.end == last_annual_report_date]
    if last_annual_report_row.empty:
        return None, None

    # frame is a string CYXXXXQXI, we want the X between Q and I
    try:
        quarter_of_annual_report = last_annual_report_row.iloc[0]["frame"][7]
    except:
        print(last_annual_report_row)
        return None, None

    year_bs = int(last_annual_report_row.frame.iloc[0][2:6])
    years_diff = year_bs - last_annual_report_fy

    return quarter_of_annual_report, years_diff

def get_yearly_values_from_df(df, instant=False, quarter_of_annual_report=None, years_diff=0):

    """
    Get yearly data from DataFrame
    :param df: DataFrame containing quarterly and annual values
    :param instant: bool that indicates if the measure is instantaneous (snapshot), if True it means the measure is a
    balance sheet measure, otherwise it's an income statement/cashflow statement measure (period instead of snapshot)
    :param last_annual_report_date: in case of instant measure, we also need the last annual report date as we will
    need it to discern the "final year" figures from the rest
    :return: dict {
        "dates": [date1, date2, ..., dateN],
        "values": [val1, val2, ..., valN],
        "last_annual_report_date": date
    }
    """

    # create a copy as we are going to edit and filter it
    year_df = df.copy()

    # income statement / cashflow statement
    if not instant:

        # get only annual frames
        year_df = year_df[~year_df.frame.str.contains("Q")]
        dates = list((year_df.frame.str.replace("CY", "")).astype(int))

        return {"dates": dates,
                "values": list(year_df.val)}

    # balance sheet
    else:

        # keep only only rows with quarters of annual reports
        year_df = year_df[year_df.frame.str.contains(f"Q{quarter_of_annual_report}I")]
        year_df["frame"] = year_df.frame.str.replace("CY", "").str.replace(f"Q{quarter_of_annual_report}I","").astype(int) - years_diff

        return {"dates": list(year_df.frame),
                "values": list(year_df.val)}

def get_values_from_measures(doc, measures, get_ttm=True, get_most_recent=True, get_yearly=True, instant=False,
                             quarter_of_annual_report=None, years_diff=0, debug=False, unit="USD", tax="us-gaap"):

    """
    Retrieve requested financial values from company financial document (containing all history and all measures).
    Measures are interpreted in a hierarchical way, meaning that if we have a value for 2020 for the first measure,
    and a value for 2020 for the second measure, we are going to keep the first.
    This is done in order to account for different possible measures that represent the same metric but could be present
    in a company but not in another. The hierarchy is useful in case a company has more than one measure and we need
    to choose which one to keep
    :param doc: company financial document
    :param measures: measures we are interest in (in order of "importance")
    :param get_ttm: bool, whether to compute ttm value
    :param get_most_recent: bool, whether to compute most recent value
    :param get_yearly: bool, whether to compute yearly values
    :param instant: bool, indicates if the measures are instantaneous (snapshot, balance sheet) or not (period,
    income statement / cashflow statement)
    :param last_annual_report_date: date of last annual report, used for instant measures
    :param debug: bool, print debug statements
    :return: most recent value, ttm value, yearly values (0 or empty if not requested)
    """

    ttm = 0
    ttm_year = None
    most_recent = 0
    most_recent_date = None
    yearly = {"dates": [], "values": []}

    for m in measures:

        # Build the DataFrame
        df = build_financial_df(doc, m, unit, tax)

        # The df is None if the company does not have the measure m in its financial data
        if df is None or df.empty:
            continue

        if get_ttm:

            # Get TTM
            ttm_value_tmp, ttm_year_tmp = get_ttm_from_df(df)

            if ttm_value_tmp is not None:

                # We override ttm if we have a more recent value
                if ttm_year is None or ttm_year_tmp > ttm_year:
                    ttm = ttm_value_tmp
                    ttm_year = ttm_year_tmp

                if debug:
                    print(m, ttm_year_tmp, ttm_value_tmp)

        if get_most_recent:

            # Get most recent value
            most_recent_tmp = get_most_recent_value_from_df(df)

            if most_recent_tmp["value"] is not None:

                # We override most_recent_value if we have a more recent value
                if most_recent_date is None or most_recent_tmp["date"] > most_recent_date:
                    most_recent_date = most_recent_tmp["date"]
                    most_recent = most_recent_tmp["value"]

                if debug:
                    print(m, most_recent_tmp["date"], most_recent_tmp["value"])

        if get_yearly:

            # Get yearly values
            yearly_tmp = get_yearly_values_from_df(df, instant, quarter_of_annual_report, years_diff)

            if yearly_tmp is not None:

                # for each date
                for i, d in enumerate(yearly_tmp["dates"]):

                    # if we don't have it already (hierarchical), we add the values
                    if d not in yearly["dates"]:
                        yearly["dates"].append(d)
                        yearly["values"].append(yearly_tmp["values"][i])

                if debug:
                    print(m, yearly_tmp)

    # sort dates and values from the least recent to the most recent
    sort = sorted(zip(yearly["dates"], yearly["values"]))
    yearly["dates"] = [x for x, _ in sort]
    yearly["values"] = [x for _, x in sort]

    if debug:
        print("ttm", ttm)
        print("most recent", most_recent)
        print("yearly", yearly)

    return {"date":most_recent_date, "value":most_recent}, {"date":ttm_year, "value":ttm}, yearly

def merge_subsets_yearly(superset, subsets, must_include=None):

    """
    Sum multiple measures into a single one. Superset is the measure that should already represent the sum. Subsets are
    its components.
    This method is used when we don't have the aggregated measure or we don't have it for all the years where we have
    the disaggregated measures.
    For example we have Total Assets for 2020,2021,2022 + Current Assets and Non-Current Assets from 2019 to 2022.
    In this case we can build Total Assets also for 2019.
    :param superset: aggregated measure
    :param subsets: disaggregated measures to be summed
    :param must_include: we can pass a tuple in order to consider a summed value iff we all the measures in subsets
    with indexes included in 'must_include' have values. In the example above for example we can say to add the 2019
    value for Total Assets iff we have Current Assets for 2019. If we have only Non-Current assets we will not add the
    2019 value for Total Assets.
    :return:
    """

    to_add = {"dates":[],"values":[]}

    # if no must_include
    if must_include is None:

        # for each subset
        for s in subsets:

            # for each date in the subset
            for i, d in enumerate(s["dates"]):

                # if that date is not in superset
                if d not in superset["dates"]:

                    # if it's the first subset with that date, append the value
                    if d not in to_add["dates"]:
                        to_add["dates"].append(d)
                        to_add["values"].append(s["values"][i])

                    # else add the value to the existing one
                    else:
                        idx = to_add["dates"].index(d)
                        to_add["values"][idx] += s["values"][i]

    # if must_include
    else:

        if not isinstance(must_include, tuple):
            raise Exception("must_include must be a tuple")

        # get dates for the first must_include (all others in must_include must have the same dates for the date
        # to be included)
        tmp_dates = subsets[must_include[0]]["dates"]

        remove_dates = []

        # for each date
        for d in tmp_dates:

            # for each index in must_include
            for m in must_include:

                # if the subset does not have the date we remove it
                s = subsets[m]
                if d not in s["dates"]:
                    remove_dates.append(d)

        # keep only the dates where we have values for every must_include subset
        must_include_dates = [x for x in tmp_dates if x not in remove_dates and x not in superset["dates"]]

        # if none return
        if len(must_include_dates) == 0:
            return

        # set to 0 the values for each date
        for m in must_include_dates:
            to_add["dates"].append(m)
            to_add["values"].append(0)

        # for each subset, add the value for the dates
        for s in subsets:
            for i, d in enumerate(s["dates"]):
                if d in to_add["dates"]:
                    idx = to_add["dates"].index(d)
                    to_add["values"][idx] += s["values"][i]

    for i, d in enumerate(to_add["dates"]):
        superset["dates"].append(d)
        superset["values"].append(to_add["values"][i])

    # sort date and values in superset
    sort = sorted(zip(superset["dates"], superset["values"]))
    superset["dates"] = [x for x, _ in sort]
    superset["values"] = [x for _, x in sort]

def merge_subsets_most_recent(superset, subsets):

    """
    Sum multiple most recent (or ttm) measures into a single one. Superset is the measure that should already represent the sum. Subsets are
    its components.
    This method is used when we don't have the aggregated measure or we don't have it for all the years where we have
    the disaggregated measures.
    For example we have Total Assets for 2020,2021,2022 + Current Assets and Non-Current Assets from 2019 to 2022.
    In this case we can build Total Assets also for 2019.
    :param superset: aggregated measure
    :param subsets: disaggregated measures to be summed
    :return:
    """

    replace = False
    for s in subsets:
        if superset["date"] is None or (s["date"] is not None and s["date"] > superset["date"]):
            replace = True
            break

    if replace:

        dates = [x["date"] for x in subsets if x["date"] is not None]

        # we are here if neither the superset nor the subsets have any value
        if len(dates) == 0:
            return

        d = max(dates)

        superset["date"] = d
        superset["value"] = 0

        for s in subsets:
            if s["date"] == d:
                superset["value"] += s["value"]

def extract_shares(doc, quarter_of_annual_report, years_diff):
    """
    Extract number of shares from company financial document
    :param doc: company financial document
    :return: number of common shares outstanding (most recent and annual)
    """

    df = build_financial_df(doc, "EntityCommonStockSharesOutstanding", unit="shares", tax="dei")

    debug = False

    if debug:
        print(df.to_markdown())

    try:
        most_recent_shares = get_most_recent_value_from_df(df)
    except:
        most_recent_shares = {"date":None, "value":0}

    measures = ["CommonStockSharesOutstanding"]

    mr_common_shares, _, yearly_common_shares = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report,
        years_diff=years_diff, get_ttm=False,
        get_most_recent=True, debug=debug, unit="shares")

    measures = ["WeightedAverageNumberOfSharesOutstandingBasic"]

    mr_average_shares, _, yearly_average_shares = get_values_from_measures(
        doc, measures, instant=False, quarter_of_annual_report=quarter_of_annual_report,
        years_diff=years_diff, get_ttm=False,
        get_most_recent=True, debug=debug, unit="shares")

    merge_subsets_most_recent(most_recent_shares, [mr_common_shares])
    merge_subsets_most_recent(most_recent_shares, [mr_average_shares])

    try:
        yearly_shares = get_yearly_values_from_df(df, instant=True, quarter_of_annual_report=quarter_of_annual_report,
        years_diff=years_diff)

        merge_subsets_yearly(yearly_common_shares, [yearly_average_shares])
        merge_subsets_yearly(yearly_shares, [yearly_common_shares])

    except:
        merge_subsets_yearly(yearly_common_shares, [yearly_average_shares])
        yearly_shares = yearly_common_shares

    # in some filings the company report shares with a wrong unit of measure (million shares instead of thousand shares)

    try:
        max_num_shares = max(yearly_shares["values"])
    except:
        raise NoSharesException()

    yearly_shares["values"] = [x * 1000 if x / max_num_shares < 0.01 else x for x in yearly_shares["values"]]
    if most_recent_shares["value"] / max_num_shares < 0.01:
        most_recent_shares["value"] *= 1000
    return {
        "mr_shares": most_recent_shares,
        "shares": yearly_shares,
    }

class NoSharesException(Exception):
    pass

def extract_income_statement(doc):
    """
    Extract income statement measures from company financial document.
    Measures include:
    - revenue
    - R&D
    - net income
    - interest expense
    - gross profit
    - depreciation and amortization
    - EBIT
    :param doc: company financial document
    :return: dict with ttm and yearly measures
    """

    measures = [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet"
    ]
    _, ttm_revenue, yearly_revenue = get_values_from_measures(doc, measures, get_most_recent=False, debug=False)

    last_annual_report_date = None
    last_annual_report_fy = None
    for m in measures:
        df = build_financial_df(doc, m)
        if df is not None and not df.empty and "frame" in df.columns:
            annual_rd, annual_fy = get_last_annual_report_date_and_fy(df)
            if last_annual_report_date is None or (annual_rd is not None and annual_rd > last_annual_report_date):
                last_annual_report_date = annual_rd
                last_annual_report_fy = annual_fy

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

    merge_subsets_yearly(yearly_rd, [yearly_rd_not_inprocess, yearly_rd_inprocess])

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

    _, ttm_interest_expenses, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                                  debug=False)

    # Probably we don't need yearly interest expenses
    # measures = ["InterestExpenseDebt",
    #             "InterestExpenseDebtExcludingAmortization"]
    # _, ttm_ie_debt, yearly_ie_debt = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                           debug=False)
    #
    # measures = ["InterestExpenseLongTermDebt"]
    # _, ttm_ie_debt_lt, yearly_ie_debt_lt = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                                 debug=False)
    #
    # measures = ["InterestExpenseShortTermBorrowings"]
    # _, ttm_ie_debt_st, yearly_ie_debt_st = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                                 debug=False)
    # merge_subsets(yearly_ie_debt, [yearly_ie_debt_lt, yearly_ie_debt_st])
    #
    #
    # measures = ["InterestExpenseBorrowings"]
    # _, ttm_ie_borrowings, yearly_ie_borrowings = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                                       debug=False)
    # measures = ["InterestExpenseDeposits"]
    # _, ttm_ie_deposits, yearly_ie_deposits = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                                   debug=False)
    # measures = ["InterestExpenseOther"]
    # _, ttm_ie_others, yearly_ie_others = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                               debug=False)
    # measures = ["InterestExpenseRelatedParty"]
    # _, ttm_ie_related, yearly_ie_related = get_values_from_measures(doc, measures, get_most_recent=False,
    #                                                                 debug=False)
    #
    # merge_subsets(yearly_ie_borrowings, [yearly_ie_debt, yearly_ie_deposits, yearly_ie_others, yearly_ie_related])
    # merge_subsets(yearly_interest_expenses, [yearly_ie_borrowings])



    #### Gross Profit ####

    if ttm_interest_expenses == 0:

        measures = ["InterestExpenseBorrowings"]
        _, ttm_ie_borrowings, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                           debug=False)

        if ttm_ie_borrowings == 0:

            measures = ["InterestExpenseDebt",
                        "InterestExpenseDebtExcludingAmortization"]
            _, ttm_ie_debt, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                      debug=False)

            if ttm_ie_debt == 0:

                measures = ["InterestExpenseLongTermDebt"]
                _, ttm_ie_debt_lt, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                debug=False)
                measures = ["InterestExpenseShortTermBorrowings"]
                _, ttm_ie_debt_st, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                debug=False)
                ttm_ie_debt = ttm_ie_debt_lt + ttm_ie_debt_st


            measures = ["InterestExpenseDeposits"]
            _, ttm_ie_deposits, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                              debug=False)
            measures = ["InterestExpenseOther"]
            _, ttm_ie_others, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                          debug=False)
            measures = ["InterestExpenseRelatedParty"]
            _, ttm_ie_related, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_yearly=False,
                                                                            debug=False)

            ttm_ie_borrowings = ttm_ie_debt + ttm_ie_deposits + ttm_ie_others + ttm_ie_related

        ttm_interest_expenses = ttm_ie_borrowings

    measures = ["Gross Profit"]
    _, ttm_gross_profit, yearly_gross_profit = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)

    #### Depreciation ####
    measures = [
        "DepreciationDepletionAndAmortization",
        "DepreciationAmortizationAndAccretionNet"]
    _, _, yearly_depreciation_amortization = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                                  debug=False)

    measures = ["Depreciation"]
    _, _, yearly_depreciation = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                        debug=False)

    measures = ["AmortizationOfFinancingCostsAndDiscounts"]
    _, _, yearly_amortization_fincost_disc = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                                  debug=False)

    measures = ["AmortizationOfDebtDiscountPremium"]
    _, _, yearly_amortization_disc = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                  debug=False)
    measures = ["AmortizationOfFinancingCosts"]
    _, _, yearly_amortization_fincost = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                        debug=False)

    merge_subsets_yearly(yearly_amortization_fincost_disc, [yearly_amortization_disc, yearly_amortization_fincost])

    measures = ["AmortizationOfDeferredCharges"]
    _, _, yearly_amortization_charges = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                        debug=False)
    measures = ["AmortizationOfDeferredSalesCommissions"]
    _, _, yearly_amortization_comm = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                  debug=False)
    measures = ["AmortizationOfIntangibleAssets"]
    _, _, yearly_amortization_intan = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                    debug=False)

    yearly_amortization = {"dates":[], "values":[]}
    merge_subsets_yearly(yearly_amortization, [yearly_amortization_fincost_disc, yearly_amortization_charges,
                                               yearly_amortization_comm, yearly_amortization_intan])
    merge_subsets_yearly(yearly_depreciation_amortization, [yearly_depreciation, yearly_amortization])

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

    return {
        "ttm_revenue": ttm_revenue,
        "ttm_gross_profit": ttm_gross_profit,
        "ttm_ebit": ttm_ebit,
        "ttm_net_income": ttm_net_income,
        "ttm_interest_expenses": ttm_interest_expenses,
        "revenue": yearly_revenue,
        "gross_profit": yearly_gross_profit,
        "rd": yearly_rd,
        "ebit": yearly_ebit,
        "depreciation": yearly_depreciation_amortization,
        "net_income": yearly_net_income,
        "last_annual_report_date": last_annual_report_date,
        "last_annual_report_fy": last_annual_report_fy
    }

def extract_balance_sheet_current_assets(doc, quarter_of_annual_report, years_diff):
    """
    Extract balance sheet measures (Current Assets) from company financial document.
    Measures include:
    - cash
    - inventory
    - other assets
    - receivables
    - securities
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent and yearly measures
    """

    # No need for aggregate values
    # #### ASSETS ####
    # measures = ["Assets"]
    # most_recent_assets, _, yearly_assets = get_values_from_measures(
    #     doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)
    #
    # #### Current Assets ####
    # measures = ["AssetsCurrent"]
    # most_recent_current_assets, _, yearly_current_assets = get_values_from_measures(
    #     doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)



    #### Inventory ####

    #### Cash ####
    measures = ["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
    most_recent_cash_and_restricted, _, yearly_cash_and_restricted = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["CashAndCashEquivalentsAtCarryingValue", "Cash"]
    most_recent_cash, _, yearly_cash = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "RestrictedCashAndCashEquivalentsAtCarryingValue",
        "RestrictedCashAndCashEquivalents",
        "RestrictedCash",
        "RestrictedCashAndInvestmentsCurrent",
        "RestrictedCashCurrent"
    ]
    most_recent_restrictedcash, _, yearly_restrictedcash = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_cash_and_restricted, [yearly_cash, yearly_restrictedcash], must_include=(0,))

    if most_recent_cash_and_restricted["date"] is None \
            or (most_recent_cash["date"] is not None and most_recent_cash["date"] > most_recent_cash_and_restricted["date"]):
        most_recent_cash_and_restricted["date"] = most_recent_cash["date"]
        most_recent_cash_and_restricted["value"] = most_recent_cash["value"]

        if most_recent_restrictedcash["date"] == most_recent_cash["date"]:
            most_recent_cash_and_restricted["value"] += most_recent_restrictedcash["value"]

    #### Inventory ####
    measures = [
        "InventoryNet",
        "InventoryGross",
        "FIFOInventoryAmount",
        "InventoryLIFOReserve",
        "LIFOInventoryAmount",
    ]
    most_recent_inventory, _, yearly_inventory = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "RetailRelatedInventory",
        "RetailRelatedInventoryMerchandise"
    ]
    most_recent_inventory_retail, _, yearly_inventory_retail = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "EnergyRelatedInventory"
    ]
    most_recent_inventory_energy, _, yearly_inventory_energy = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "PublicUtilitiesInventory"
    ]
    most_recent_inventory_utilities, _, yearly_inventory_utilities = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "InventoryRealEstate"
    ]
    most_recent_inventory_re, _, yearly_inventory_re = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "AirlineRelatedInventory"
    ]
    most_recent_inventory_airline, _, yearly_inventory_airline = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_most_recent(most_recent_inventory,
                              [most_recent_inventory_retail, most_recent_inventory_airline,
                               most_recent_inventory_energy, most_recent_inventory_re, most_recent_inventory_utilities])
    merge_subsets_yearly(yearly_inventory, [yearly_inventory_retail, yearly_inventory_airline, yearly_inventory_energy,
                                            yearly_inventory_re, yearly_inventory_utilities])

    #### Other Assets ####
    measures = [
        "OtherAssetsCurrent",
        "OtherAssetsMiscellaneousCurrent",
        "PrepaidExpenseAndOtherAssetsCurrent",
        "OtherAssetsFairValueDisclosure",
        "OtherAssetsMiscellaneous",
        "PrepaidExpenseAndOtherAssets"
    ]
    most_recent_other_current_assets, _, yearly_other_current_assets = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["PrepaidExpenseCurrent"]
    most_recent_prepaid_exp, _, yearly_prepaid_exp = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)
    measures = ["PrepaidInsurance"]
    most_recent_prepaid_ins, _, yearly_prepaid_ins = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)
    measures = ["PrepaidTaxes",
                "IncomeTaxesReceivable",
                "IncomeTaxReceivable"]
    most_recent_prepaid_tax, _, yearly_prepaid_tax = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)
    merge_subsets_yearly(yearly_other_current_assets, [yearly_prepaid_exp, yearly_prepaid_ins, yearly_prepaid_tax])

    merge_subsets_most_recent(most_recent_other_current_assets,
                              [most_recent_prepaid_exp, most_recent_prepaid_ins, most_recent_prepaid_tax])

    #### Receivables ####
    measures = [
        "AccountsAndOtherReceivablesNetCurrent",
        "AccountsNotesAndLoansReceivableNetCurrent",
        "ReceivablesNetCurrent",
        "NontradeReceivablesCurrent"]
    most_recent_receivables, _, yearly_receivables = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AccountsReceivableNetCurrent",
                "AccountsReceivableNet",
                "AccountsReceivableGrossCurrent",
                "AccountsReceivableGross"]
    most_recent_ar, _, yearly_ar = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["LoansAndLeasesReceivableNetReportedAmount",
                "LoansAndLeasesReceivableNetOfDeferredIncome",
                "LoansReceivableFairValueDisclosure",
                "LoansAndLeasesReceivableGrossCarryingAmount"]
    most_recent_loans_rec, _, yearly_loans_rec = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["NotesReceivableNet",
                "NotesReceivableFairValueDisclosure",
                "NotesReceivableGross"]
    most_recent_notes_rec, _, yearly_notes_rec = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_receivables, [yearly_ar, yearly_loans_rec, yearly_notes_rec])
    merge_subsets_most_recent(most_recent_receivables,
                              [most_recent_ar, most_recent_loans_rec, most_recent_notes_rec])

    #### Securities ####
    measures = [
        "MarketableSecurities"
        "AvailableForSaleSecurities"]
    most_recent_securities, _, yearly_securities = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesDebtSecurities"]
    most_recent_debtsecurities, _, yearly_debtsecurities = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesEquitySecurities"]
    most_recent_equitysecurities, _, yearly_equitysecurities = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_securities, [yearly_debtsecurities, yearly_equitysecurities])
    merge_subsets_most_recent(most_recent_securities,
                              [most_recent_debtsecurities, most_recent_equitysecurities])

    measures = ["DerivativeAssets",
                "DerivativeAssetsCurrent"]
    most_recent_derivatives, _, yearly_derivatives = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["HeldToMaturitySecurities",
                "HeldToMaturitySecuritiesFairValue",
                "HeldToMaturitySecuritiesCurrent",
                ]
    most_recent_held_securities, _, yearly_held_securities = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesNoncurrent",
                "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent",
                ]
    most_recent_non_curr_sec, _, yearly_non_curr_sec = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["MarketableSecuritiesCurrent",
                "AvailableForSaleSecuritiesDebtSecuritiesCurrent"]
    most_recent_marksecurities_cur, _, yearly_marksecurities_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["ShortTermInvestments"]
    most_recent_st_inv, _, yearly_st_inv = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["MoneyMarketFundsAtCarryingValue"]
    most_recent_mm, _, yearly_mm = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_securities, [yearly_derivatives, yearly_held_securities, yearly_non_curr_sec,
                                             yearly_marksecurities_cur, yearly_st_inv, yearly_mm])
    merge_subsets_most_recent(most_recent_securities,
                              [most_recent_derivatives, most_recent_held_securities, most_recent_non_curr_sec,
                               most_recent_marksecurities_cur, most_recent_st_inv, most_recent_mm])

    # merge_subsets(yearly_current_assets, [yearly_cash_and_restricted, yearly_inventory, yearly_other_current_assets,
    #                                       yearly_receivables, yearly_securities])

    return {
        "mr_cash": most_recent_cash_and_restricted,
        "cash": yearly_cash_and_restricted,
        "mr_inventory": most_recent_inventory,
        "inventory": yearly_inventory,
        "mr_other_assets": most_recent_other_current_assets,
        "other_assets": yearly_other_current_assets,
        "mr_receivables": most_recent_receivables,
        "receivables": yearly_receivables,
        "mr_securities": most_recent_securities,
        "securities": yearly_securities
    }

def extract_balance_sheet_noncurrent_assets(doc, quarter_of_annual_report, years_diff):
    """
    Extract balance sheet measures (Non-Current Assets) from company financial document.
    Measures include:
    - equity investments
    - other financial assets
    - PP&E
    - investment property
    - tax benefits
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent measures
    """

    # ##### Non current assets ####
    # measures = ["AssetsNoncurrent",
    #             "NoncurrentAssets"]
    # most_recent_non_curr_asset, _, yearly_non_curr_asset = get_values_from_measures(
    #     doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    #### Equity investments ####

    #### Equity Investments ####
    measures = [
        "EquityMethodInvestmentAggregateCost",
        "EquityMethodInvestments",
        "InvestmentOwnedAtCost",
        "Investments",
        "InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures",
    ]
    most_recent_equity_investments, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False,
        debug=False)

    measures = [
        "EquityMethodInvestmentsFairValueDisclosure",
        "InvestmentOwnedAtFairValue",
        "InvestmentsFairValueDisclosure",
    ]
    most_recent_equity_inv_fv, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["EquitySecuritiesWithoutReadilyDeterminableFairValueAmount", ]
    most_recent_equity_inv_notfv, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    # merge_subsets_yearly(yearly_equity_investments, [yearly_equity_inv_fv, yearly_equity_inv_notfv])
    merge_subsets_most_recent(most_recent_equity_investments,
                              [most_recent_equity_inv_fv, most_recent_equity_inv_notfv])

    measures = ["MarketableSecuritiesNoncurrent"]
    most_recent_securities_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    # yearly_equity_investments_and_securities = {"dates": [], "values": []}
    # merge_subsets_yearly(yearly_equity_investments_and_securities, [yearly_equity_investments, yearly_securities_non_curr])

    merge_subsets_most_recent(most_recent_equity_investments, [most_recent_securities_non_curr])

    #### Other financial assets ####
    measures = [
        "PrepaidExpenseNoncurrent",
        "PrepaidExpenseOtherNoncurrent",
    ]
    most_recent_prepaid_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "RestrictedCashAndCashEquivalentsNoncurrent",
        "RestrictedCashAndInvestmentsNoncurrent",
        "RestrictedCashNoncurrent"
    ]
    most_recent_cash_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeAssetsNoncurrent", ]
    most_recent_derivatives_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["EscrowDeposit"]
    most_recent_escrow, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    # yearly_other_financial_assets = {"dates": [], "values": []}
    # merge_subsets_yearly(yearly_other_financial_assets, [yearly_prepaid_non_curr, yearly_cash_non_curr,
    #                                                      yearly_derivatives_non_curr, yearly_escrow])

    most_recent_other_financial_assets = {"date":None, "value":0}
    merge_subsets_most_recent(most_recent_other_financial_assets,
                              [most_recent_prepaid_non_curr, most_recent_cash_non_curr,
                               most_recent_derivatives_non_curr, most_recent_escrow])

    #### PP&E ####
    measures = [
        "PropertyPlantAndEquipmentNet",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization"
    ]
    most_recent_ppe, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    #### Investment property ####
    measures = [
        "RealEstateInvestments",
        "RealEstateInvestmentPropertyNet",
        "RealEstateInvestmentPropertyAtCost",
        "RealEstateHeldforsale"
    ]
    most_recent_property, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["InvestmentBuildingAndBuildingImprovements"]
    most_recent_buildings, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "LandAndLandImprovements",
        "Land",
    ]
    most_recent_land, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    # merge_subsets_yearly(yearly_property, [yearly_buildings, yearly_land])
    merge_subsets_most_recent(most_recent_property,
                              [most_recent_buildings, most_recent_land])

    # merge_subsets(yearly_non_curr_asset, [yearly_property, yearly_ppe, yearly_other_financial_assets,
    #                                       yearly_equity_investments_and_securities])
    # merge_subsets(yearly_assets, [yearly_current_assets, yearly_non_curr_asset])

    #### Tax Benefits ####
    measures = [
        "UnrecognizedTaxBenefits",
        "UnrecognizedTaxBenefitsThatWouldImpactEffectiveTaxRate",
        "IncomeTaxesReceivableNoncurrent",
    ]
    most_recent_tax_benefit, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    return {
        "mr_equity_investments": most_recent_equity_investments,
        "mr_other_financial_assets": most_recent_other_financial_assets,
        "mr_ppe": most_recent_ppe,
        "mr_investment_property": most_recent_property,
        "mr_tax_benefits": most_recent_tax_benefit
    }

def extract_balance_sheet_debt(doc, quarter_of_annual_report, years_diff):
    """
    Extract balance sheet DEBT measures (Current + Non-Current) from company financial document.
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent and yearly measures
    """

    # DEBT LONG + SHORT
    measures = ["DebtLongtermAndShorttermCombinedAmount"]
    most_recent_debt, _, yearly_debt = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["MortgageLoansOnRealEstate"]
    most_recent_mortgage, _, yearly_mortgage = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["OtherBorrowings"]
    most_recent_other_borr, _, yearly_other_borr = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    # DEBT SHORT
    measures = [
        "ShortTermBorrowings",
        "ShorttermDebtAverageOutstandingAmount",
        "ShorttermDebtFairValue",
    ]
    most_recent_debt_st, _, yearly_debt_st = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "CommercialPaper",
        "CommercialPaperAtCarryingValue",
    ]
    most_recent_cp, _, yearly_cp = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["BankOverdrafts"]
    most_recent_overdraft, _, yearly_overdraft = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["ShortTermBankLoansAndNotesPayable"]
    most_recent_loans_st, _, yearly_loans_st = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["BridgeLoan"]
    most_recent_bridge, _, yearly_bridge = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    # DEBT LONG
    measures = [
        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
        "LongTermDebt",
        "LongTermDebtFairValue",
        "DebtInstrumentFaceAmount",
        "DebtInstrumentCarryingAmount",
    ]
    most_recent_debt_lt, _, yearly_debt_lt = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "ConvertibleDebt",
        "ConvertibleNotesPayable",
    ]
    most_recent_convertible, _, yearly_convertible = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "LineOfCredit",
        "LineOfCreditFacilityFairValueOfAmountOutstanding",
    ]
    most_recent_revolver, _, yearly_revolver = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["LoansPayable"]
    most_recent_loans_pay, _, yearly_loans_pay = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["SecuredDebt"]
    most_recent_debt_sec, _, yearly_debt_sec = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["NotesPayable",
                "SeniorNotes"]
    most_recent_debt_notes, _, yearly_debt_notes = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["UnsecuredDebt"]
    most_recent_debt_unsec, _, yearly_debt_unsec = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    # DEBT LONG - CURRENT
    measures = [
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "LongTermDebtCurrent",
    ]
    most_recent_debt_lt_cur, _, yearly_debt_lt_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "ConvertibleDebtCurrent",
        "ConvertibleNotesPayableCurrent",
    ]
    most_recent_convertible_cur, _, yearly_convertible_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["LinesOfCreditCurrent"]
    most_recent_revolver_cur, _, yearly_revolver_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["NotesPayableCurrent",
                "SeniorNotesCurrent"]
    most_recent_notes_cur, _, yearly_notes_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["SecuredDebtCurrent"]
    most_recent_debt_sec_cur, _, yearly_debt_sec_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["UnsecuredDebtCurrent"]
    most_recent_debt_unsec_cur, _, yearly_debt_unsec_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    # DEBT LONG - NON CURRENT
    measures = [
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtNoncurrent",
    ]
    most_recent_debt_lt_noncur, _, yearly_debt_lt_noncur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "ConvertibleDebtNoncurrent",
        "ConvertibleLongTermNotesPayable",
    ]
    most_recent_convertible_noncur, _, yearly_convertible_noncur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["LongTermLineOfCredit"]
    most_recent_revolver_noncur, _, yearly_revolver_noncur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["LongTermNotesPayable",
                "SeniorLongTermNotes"]
    most_recent_notes_noncur, _, yearly_notes_noncur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["SecuredLongTermDebt"]
    most_recent_debt_sec_noncur, _, yearly_debt_sec_noncur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
         get_ttm=False, debug=False)

    measures = ["UnsecuredLongTermDebt"]
    most_recent_debt_unsec_noncur, _, yearly_debt_unsec_noncur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
         get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_debt_st, [yearly_cp, yearly_overdraft, yearly_bridge, yearly_loans_st])
    merge_subsets_yearly(yearly_debt_lt, [yearly_convertible, yearly_revolver, yearly_loans_pay, yearly_debt_sec,
                                          yearly_debt_notes, yearly_debt_unsec])
    merge_subsets_yearly(yearly_debt_lt_cur, [yearly_convertible_cur, yearly_revolver_cur, yearly_notes_cur,
                                              yearly_debt_sec_cur, yearly_debt_unsec_cur])
    merge_subsets_yearly(yearly_debt_lt_noncur, [yearly_convertible_noncur, yearly_revolver_noncur, yearly_notes_noncur,
                                                 yearly_debt_sec_noncur, yearly_debt_unsec_noncur])
    merge_subsets_yearly(yearly_debt_lt, [yearly_debt_lt_cur, yearly_debt_lt_noncur])
    merge_subsets_yearly(yearly_debt, [yearly_debt_lt, yearly_debt_st])
    merge_subsets_yearly(yearly_debt, [yearly_debt, yearly_mortgage, yearly_other_borr], (0,))

    merge_subsets_most_recent(most_recent_debt_st, [most_recent_cp, most_recent_overdraft, most_recent_bridge, most_recent_loans_st])
    merge_subsets_most_recent(most_recent_debt_lt, [most_recent_convertible, most_recent_revolver, most_recent_loans_pay, most_recent_debt_sec,
                                          most_recent_debt_notes, most_recent_debt_unsec])
    merge_subsets_most_recent(most_recent_debt_lt_cur, [most_recent_convertible_cur, most_recent_revolver_cur, most_recent_notes_cur,
                                              most_recent_debt_sec_cur, most_recent_debt_unsec_cur])
    merge_subsets_most_recent(most_recent_debt_lt_noncur, [most_recent_convertible_noncur, most_recent_revolver_noncur, most_recent_notes_noncur,
                                                 most_recent_debt_sec_noncur, most_recent_debt_unsec_noncur])
    merge_subsets_most_recent(most_recent_debt_lt, [most_recent_debt_lt_cur, most_recent_debt_lt_noncur])
    merge_subsets_most_recent(most_recent_debt, [most_recent_debt_lt, most_recent_debt_st])

    for m in [most_recent_mortgage, most_recent_other_borr]:
        if m["date"] == most_recent_debt["date"]:
            most_recent_debt["value"] += m["value"]

    return {
        "mr_debt": most_recent_debt,
        "debt": yearly_debt
    }

def extract_balance_sheet_liabilities(doc, quarter_of_annual_report, years_diff, most_recent_debt):
    """
    Extract balance sheet measures (Total Liabilities) from company financial document.
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent measures
    """

    measures = [
        "Liabilities",
        "LiabilitiesFairValueDisclosure",
        "LiabilitiesAssumed1",
    ]
    most_recent_liabilities, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeLiabilities"]
    most_recent_derivatives_liability, _, yearly_derivatives_liability = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = [
        "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent",
        "AccountsPayableCurrentAndNoncurrent",
    ]
    most_recent_ap, _, yearly_ap = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["DueToRelatedPartiesCurrentAndNoncurrent"]
    most_recent_due_related_parties, _, yearly_due_related_parties = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["DueToAffiliateCurrentAndNoncurrent"]
    most_recent_due_affiliates, _, yearly_due_affiliates = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    # yearly_liabilities_ex_debt = {"dates": [], "values": []}
    # merge_subsets_yearly(yearly_liabilities_ex_debt, [yearly_derivatives_liability, yearly_ap, yearly_due_related_parties,
    #                                                   yearly_due_affiliates])

    # Current
    measures = ["LiabilitiesCurrent"]
    most_recent_liabilities_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "AccountsPayableAndOtherAccruedLiabilitiesCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
    ]
    most_recent_ap_complete_cur, _, yearly_ap_complete_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AccountsPayableCurrent"]
    most_recent_ap_cur, _, yearly_ap_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AccountsPayableOtherCurrent"]
    most_recent_apother_cur, _, yearly_apother_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AccountsPayableRelatedPartiesCurrent"]
    most_recent_ap_rel_cur, _, yearly_ap_rel_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["AccountsPayableTradeCurrent"]
    most_recent_ap_trade_cur, _, yearly_ap_trade_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_ap_complete_cur, [yearly_ap_cur, yearly_apother_cur, yearly_ap_rel_cur,
                                                  yearly_ap_trade_cur])


    measures = ["DueToAffiliateCurrent"]
    most_recent_due_affiliates_cur, _, yearly_due_affiliates_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["DueToRelatedPartiesCurrent"]
    most_recent_due_related_cur, _, yearly_due_related_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["DerivativeLiabilitiesCurrent"]
    most_recent_derivatives_liability_cur, _, yearly_derivatives_liability_cur = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    # merge_subsets_yearly(yearly_liabilities_cur, [yearly_ap_complete_cur, yearly_due_affiliates_cur, yearly_due_related_cur,
    #                                               yearly_derivatives_liability_cur])

    # Non - Current
    measures = ["LiabilitiesNoncurrent"]
    most_recent_liabilities_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeLiabilitiesNoncurrent"]
    most_recent_derivatives_liability_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToAffiliateNoncurrent"]
    most_recent_due_affiliates_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
         get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToRelatedPartiesNoncurrent"]
    most_recent_due_related_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, get_yearly=False, debug=False)

    # merge_subsets_yearly(yearly_liabilities_noncur, [yearly_derivatives_liability_noncur, yearly_due_affiliates_noncur,
    #                                                  yearly_due_related_noncur])
    # merge_subsets_yearly(yearly_liabilities_ex_debt, [yearly_liabilities_cur, yearly_liabilities_noncur])
    # merge_subsets_yearly(yearly_liabilities, [yearly_liabilities_ex_debt, yearly_debt])

    merge_subsets_most_recent(most_recent_ap_complete_cur, [most_recent_ap_cur, most_recent_apother_cur,
                                                            most_recent_ap_rel_cur, most_recent_ap_trade_cur])
    merge_subsets_most_recent(most_recent_ap, [most_recent_ap_complete_cur])
    merge_subsets_most_recent(most_recent_derivatives_liability, [most_recent_derivatives_liability_cur,
                                                                  most_recent_derivatives_liability_noncur])
    merge_subsets_most_recent(most_recent_due_affiliates, [most_recent_due_affiliates_cur,
                                                                  most_recent_due_affiliates_noncur])
    merge_subsets_most_recent(most_recent_due_related_parties, [most_recent_due_related_cur,
                                                                  most_recent_due_related_noncur])

    merge_subsets_most_recent(most_recent_liabilities, [most_recent_liabilities_cur, most_recent_liabilities_noncur])
    combo_liabilities = {"date":None, "value":0}
    merge_subsets_most_recent(combo_liabilities, [most_recent_ap, most_recent_derivatives_liability,
                                                        most_recent_due_affiliates, most_recent_due_related_parties,
                                                        most_recent_debt])

    if most_recent_liabilities["date"] is None:
        most_recent_liabilities = combo_liabilities
    elif combo_liabilities["date"] is not None:
        if combo_liabilities["date"] > most_recent_liabilities["date"]:
            most_recent_liabilities = combo_liabilities
        elif combo_liabilities["date"] == most_recent_liabilities["date"] and combo_liabilities["value"] > most_recent_liabilities["value"]:
            most_recent_liabilities = combo_liabilities

    # for working capital
    merge_subsets_yearly(yearly_ap_complete_cur, [yearly_ap])
    merge_subsets_yearly(yearly_due_affiliates_cur, [yearly_due_affiliates])
    merge_subsets_yearly(yearly_due_related_cur, [yearly_due_related_parties])


    return {
        "mr_liabilities": most_recent_liabilities,
        "account_payable": yearly_ap_complete_cur,
        "due_to_affiliates": yearly_due_affiliates_cur,
        "due_to_related_parties": yearly_due_related_cur
    }

def extract_balance_sheet_equity(doc, last_annual_report_date, last_annual_report_fy):
    """
    Extract balance sheet EQUITY measures from company financial document.
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent and yearly measures
    """

    # measures = ["LiabilitiesAndStockholdersEquity"]
    # most_recent_liabilities_and_equity, _, yearly_liabilities_and_equity = get_values_from_measures(
    #     doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    df = build_financial_df(doc, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    quarter_of_annual_report, years_diff = get_quarter_of_annual_report(df, last_annual_report_date, last_annual_report_fy)

    if quarter_of_annual_report is None:
        df = build_financial_df(doc, "StockholdersEquity")
        quarter_of_annual_report, years_diff = get_quarter_of_annual_report(df, last_annual_report_date,
                                                                            last_annual_report_fy)

    measures = ["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
    most_recent_equity, _, yearly_equity = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["StockholdersEquity"]
    most_recent_equity_no_mi, _, yearly_equity_no_mi = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    measures = ["MinorityInterest"]
    most_recent_minority_interest, _, yearly_minority_interest = get_values_from_measures(
        doc, measures, instant=True, quarter_of_annual_report=quarter_of_annual_report, years_diff=years_diff,
        get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_equity, [yearly_equity_no_mi, yearly_minority_interest], (0,))

    if most_recent_equity["date"] is None or \
            (most_recent_equity_no_mi["date"] is not None and most_recent_equity_no_mi["date"] > most_recent_equity["date"]):
        merge_subsets_most_recent(most_recent_equity, [most_recent_equity_no_mi, most_recent_minority_interest])

    return {
        "mr_equity": most_recent_equity,
        "equity": yearly_equity,
        "mr_minority_interest": most_recent_minority_interest,
        "quarter_of_annual_report": quarter_of_annual_report,
        "years_diff": years_diff
    }

def extract_cashflow_statement(doc):
    """
    Extract cashflow statement measures from company financial document.
    Measures include:
    - dividends
    - CAPEX
    - net income
    - interest expense
    - gross profit
    - depreciation and amortization
    - EBIT
    :param doc: company financial document
    :return: dict with ttm and yearly measures
    """

    # DIVIDENDS
    measures = [
        "Dividends",
        "DividendsCash",
        "PaymentsOfDividends",
        "PaymentsOfOrdinaryDividends"
    ]
    _, ttm_dividends, yearly_dividends = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                  debug=False)

    measures = [
        "DividendsCommonStock",
        "DividendsCommonStockCash",
        "PaymentsOfDividendsCommonStock"
    ]
    _, ttm_dividends_cs, yearly_dividends_cs = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)

    measures = [
        "DividendsPreferredStock",
        "DividendsPreferredStockCash",
        "PaymentsOfDividendsPreferredStockAndPreferenceStock"
    ]
    _, ttm_dividends_ps, yearly_dividends_ps = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)
    merge_subsets_yearly(yearly_dividends, [yearly_dividends_cs, yearly_dividends_ps])
    merge_subsets_most_recent(ttm_dividends, [ttm_dividends_cs, ttm_dividends_ps])

    # CAPEX

    # Acquisition
    measures = ["BusinessAcquisitionCostOfAcquiredEntityTransactionCosts"]
    _, _, yearly_acquisition_costs = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                  debug=False)

    measures = [
        "PaymentsForPreviousAcquisition",
        "PaymentsForProceedsFromPreviousAcquisition",
    ]
    _, _, yearly_acquisition_adj = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                              debug=False)

    measures = [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "PaymentsToAcquireBusinessesGross",
    ]
    _, _, yearly_acquisition = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                      debug=False)

    measures = ["PaymentsToAcquireBusinessTwoNetOfCashAcquired"]
    _, _, yearly_acquisition2 = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                        debug=False)

    measures = ["PaymentsToAcquireInterestInSubsidiariesAndAffiliates"]
    _, _, yearly_sub_aff = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                              debug=False)

    measures = ["PaymentsToAcquireAdditionalInterestInSubsidiaries"]
    _, _, yearly_sub = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                      debug=False)

    measures = ["PaymentsToAcquireBusinessesAndInterestInAffiliates"]
    _, _, yearly_aff = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                      debug=False)

    merge_subsets_yearly(yearly_sub_aff, [yearly_sub, yearly_aff])

    measures = ["PaymentsToAcquireInterestInJointVenture"]
    _, _, yearly_jv = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                    debug=False)

    # PP&E
    measures = ["CapitalExpendituresIncurredButNotYetPaid"]
    _, _, yearly_capex_not_paid = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                            debug=False)

    measures = ["PaymentsForCapitalImprovements"]
    _, _, yearly_capex_imp = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                  debug=False)

    measures = ["PaymentsToAcquireOtherProductiveAssets"]
    _, _, yearly_productive_assets_other = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                              debug=False)

    measures = ["PaymentsToAcquireOtherPropertyPlantAndEquipment"]
    _, _, yearly_ppe_other = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                  debug=False)

    measures = ["PaymentsToAcquireProductiveAssets"]
    _, _, yearly_productive_assets = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                                  debug=False)

    measures = ["PaymentsToAcquirePropertyPlantAndEquipment"]
    _, _, yearly_ppe = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                      debug=False)

    measures = ["PaymentsForSoftware"]
    _, _, yearly_sw = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                    debug=False)

    measures = ["PaymentsToDevelopSoftware"]
    _, _, yearly_sw_dev = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                            debug=False)

    # Intangibles
    measures = ["PaymentsToAcquireIntangibleAssets"]
    _, _, yearly_intangibles = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
                                                                      debug=False)

    yearly_capex = {"dates": [], "values": []}
    merge_subsets_yearly(yearly_capex, [yearly_acquisition_costs, yearly_acquisition_adj, yearly_acquisition,
                                        yearly_acquisition2, yearly_sub_aff, yearly_jv, yearly_capex_not_paid,
                                        yearly_capex_imp, yearly_productive_assets_other, yearly_ppe_other,
                                        yearly_productive_assets,
                                        yearly_ppe, yearly_sw, yearly_sw_dev, yearly_intangibles])

    return {
        "ttm_dividends": ttm_dividends,
        "dividends": yearly_dividends,
        "capex": yearly_capex
    }

def extract_operating_leases(doc):
    """
    Extract operating leases measures from company financial document.
    :param doc: company financial document
    :return: dict with most recent measures
    """

    # Last year expenses
    measures = [
        "OperatingLeasePayments",
        "OperatingLeaseCost",
        "OperatingLeaseExpense",
    ]
    _, mr_op_leases_expense, _ = get_values_from_measures(doc, measures, get_ttm=True, get_most_recent=False, get_yearly=False, debug=False)

    # Next year expenses
    measures = [
        "LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths",
        "OperatingLeasesFutureMinimumPaymentsDueCurrent",
        "LesseeOperatingLeaseLiabilityPaymentsDueNextRollingTwelveMonths",
    ]
    mr_op_leases_next_year, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    # Next 2year expenses
    measures = [
        "LesseeOperatingLeaseLiabilityPaymentsDueYearTwo",
        "OperatingLeasesFutureMinimumPaymentsDueInTwoYears",
        "LesseeOperatingLeaseLiabilityPaymentsDueInRollingYearTwo",
    ]
    mr_op_leases_next_2year, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    # Next 3year expenses
    measures = [
        "LesseeOperatingLeaseLiabilityPaymentsDueYearThree",
        "OperatingLeasesFutureMinimumPaymentsDueInThreeYears",
        "LesseeOperatingLeaseLiabilityPaymentsDueInRollingYearThree",
    ]
    mr_op_leases_next_3year, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    # Next 4year expenses
    measures = [
        "LesseeOperatingLeaseLiabilityPaymentsDueYearFour",
        "OperatingLeasesFutureMinimumPaymentsDueInFourYears",
        "LesseeOperatingLeaseLiabilityPaymentsDueInRollingYearFour",
    ]
    mr_op_leases_next_4year, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    # Next 5year expenses
    measures = [
        "LesseeOperatingLeaseLiabilityPaymentsDueYearFive",
        "OperatingLeasesFutureMinimumPaymentsDueInFiveYears",
        "LesseeOperatingLeaseLiabilityPaymentsDueInRollingYearFive",
    ]
    mr_op_leases_next_5year, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    # After 5year expenses
    measures = [
        "LesseeOperatingLeaseLiabilityPaymentsDueAfterYearFive",
        "OperatingLeasesFutureMinimumPaymentsDueThereafter",
        "LesseeOperatingLeaseLiabilityPaymentsDueAfterRollingYearFive",
    ]
    mr_op_leases_after_5year, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    return {
        "mr_op_leases_expense": mr_op_leases_expense,
        "mr_op_leases_next_year": mr_op_leases_next_year,
        "mr_op_leases_next_2year": mr_op_leases_next_2year,
        "mr_op_leases_next_3year": mr_op_leases_next_3year,
        "mr_op_leases_next_4year": mr_op_leases_next_4year,
        "mr_op_leases_next_5year": mr_op_leases_next_5year,
        "mr_op_leases_after_5year": mr_op_leases_after_5year
    }

def extract_options(doc):
    """
    Extract options measures from company financial document.
    :param doc: company financial document
    :return: dict with most recent measures
    """

    # Last year expenses
    measures = [
        "EmployeeServiceShareBasedCompensationNonvestedAwardsTotalCompensationCostNotYetRecognized",
    ]
    mr_sbc, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    measures = [
        # options
        "EmployeeServiceShareBasedCompensationNonvestedAwardsTotalCompensationCostNotYetRecognizedStockOptions",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingIntrinsicValue",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsVestedAndExpectedToVestOutstandingAggregateIntrinsicValue",
        "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsVestedAndExpectedToVestExercisableAggregateIntrinsicValue",
        "SharebasedCompensationArrangementBySharebasedPaymentAwardOptionsExercisableIntrinsicValue1",
    ]
    mr_sbc_options, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    measures = [
        # non-options
        "EmployeeServiceShareBasedCompensationNonvestedAwardsTotalCompensationCostNotYetRecognizedShareBasedAwardsOtherThanOptions",
        "SharebasedCompensationArrangementBySharebasedPaymentAwardEquityInstrumentsOtherThanOptionsAggregateIntrinsicValueOutstanding",
        "SharebasedCompensationArrangementBySharebasedPaymentAwardEquityInstrumentsOtherThanOptionsAggregateIntrinsicValueNonvested",
    ]
    mr_sbc_non_options, _, _ = get_values_from_measures(doc, measures, get_ttm=False, get_yearly=False, debug=False)

    merge_subsets_most_recent(mr_sbc, [mr_sbc_options, mr_sbc_non_options])

    return {
        "mr_sbc": mr_sbc,
    }

def extract_company_financial_information(cik):

    """
    Extract financial data required for valuation from company financial document
    :param cik: company cik
    :return: dict with income statement and balance sheet metrics
    """

    try:
        doc = mongodb.get_document("financial_data", cik)
    except:
        download_financial_data(cik)
        doc = mongodb.get_document("financial_data", cik)

    income_statement_measures = extract_income_statement(doc)
    last_annual_report_date = income_statement_measures["last_annual_report_date"]
    last_annual_report_fy = income_statement_measures["last_annual_report_fy"]

    equity = extract_balance_sheet_equity(doc, last_annual_report_date, last_annual_report_fy)
    quarter_of_annual_report = equity["quarter_of_annual_report"]
    years_diff = equity["years_diff"]

    shares = extract_shares(doc, quarter_of_annual_report, years_diff)
    current_assets = extract_balance_sheet_current_assets(doc, quarter_of_annual_report, years_diff)
    non_current_assets = extract_balance_sheet_noncurrent_assets(doc, quarter_of_annual_report, years_diff)
    debt = extract_balance_sheet_debt(doc, quarter_of_annual_report, years_diff)
    liabilities = extract_balance_sheet_liabilities(doc, quarter_of_annual_report, years_diff, debt["mr_debt"])
    cashflow_statement_measures = extract_cashflow_statement(doc)
    leases = extract_operating_leases(doc)
    options = extract_options(doc)

    return {
        **shares,
        **income_statement_measures,
        **current_assets,
        **non_current_assets,
        **debt,
        **liabilities,
        **equity,
        **cashflow_statement_measures,
        **leases,
        **options
    }

def get_selected_years(data, key, start, end):
    """
    Get the values corresponding to selected years from a dictionary {"key": {"dates":[],"values":[]}}
    :param data: dictionary {"key": {"dates":[],"values":[]}}
    :param key: the key of the dictionary that we want to extract the selected years
    :param start: initial year
    :param end: final year
    :return: list of values corresponding to selected years (or 0 if year not found)
    """

    r = []

    for y in range(start, end + 1, 1):
        try:
            idx = data[key]["dates"].index(y)
            r.append(data[key]["values"][idx] / 1000)
        except ValueError:
            r.append(0)

    return r

def null_valuation(price_per_share=0):

    fcff_value = div_value = liquidation_per_share = -1
    fcff_delta = div_delta = liquidation_delta = 10
    status = STATUS_KO

    return price_per_share, fcff_value, div_value, fcff_delta, div_delta, liquidation_per_share, liquidation_delta, status

def valuation(cik, years=5, recession_probability = 0.5, qualitative=False, debug=False):
    """
    Compute valuation for company. Valuation is done following principles teached by Prof. Damodaran in his Valuation
    Course (FCFF Valuation and Dividends Valuation).
    We build 4 different scenarios for both FCFF and Dividends Valuation:
    1. Earnings TTM & Historical Growth
    2. Earnings Normalized & Historical Growth
    3. Earnings TTM & Growth TTM
    4. Earnings Normalized & Growth Normalized
    Each scenario is also run with a recession hypothesis.
    We compute a median value for FCFF, Recession FCFF, Dividends, Recession Dividends and then compute 2
    Expected Values based on the recession_probability.
    These 2 values are then used to compute the final valuation (value/share) skewing the result towards the lowest
    value (to be conservative).

    :param cik: company cik
    :param years: how many financial years to consider in the valuation
    :param debug:
    :return: price_per_share (current price/share), fcff_value (FCFF EV), div_value (Dividends EV),
    fcff_delta premium(discount) on shares, div_delta premium(discount) on shares, status
    (OK if company is underpriced, NI if company is correctly priced, KO is company is overpriced)
    """

    # Check if we have financial data

    # Check if we have submissions (at least the last 10k)

    try:
        download_financial_data(cik)
        data = extract_company_financial_information(cik)
    except NoSharesException:
        print(cik, "no shares")
        return null_valuation()
    except StopIteration:
        print(cik, "no financial data")
        return null_valuation()

    if debug:
        print(data)
        print()

    try:
        final_year = data["revenue"]["dates"][-1]
        initial_year = final_year - years + 1
    except:
        print(cik, "no revenue")
        return null_valuation()

    erp = get_df_from_table("damodaran_erp")
    erp = erp[erp["date"] == erp["date"].max()]["value"].iloc[0]

    company_info = company_from_cik(cik)
    ticker = company_info["ticker"]

    price_per_share = get_current_price_from_yahoo(ticker)
    if price_per_share is None:
        print(ticker, "delisted")
        return null_valuation()

    try:
        company_name, country, industry, region = get_generic_info(ticker)
    except IndexError:
        print(ticker, "not found in db")
        return null_valuation()

    yahoo_equity_ticker = get_df_from_table("yahoo_equity_tickers", f"where symbol = '{ticker}'", most_recent=True).iloc[0]
    db_curr = yahoo_equity_ticker["currency"]
    db_financial_curr = yahoo_equity_ticker["financial_currency"]

    damodaran_bond_spread = get_df_from_table("damodaran_bond_spread", most_recent=True)
    damodaran_bond_spread["greater_than"] = pd.to_numeric(damodaran_bond_spread["greater_than"])
    damodaran_bond_spread["less_than"] = pd.to_numeric(damodaran_bond_spread["less_than"])

    doc = get_last_document(cik, "10-K")

    if doc is not None:
        segments = extract_segments(doc)
        geo_segments_df = geography_distribution(segments, ticker)
    else:
        geo_segments_df = None

    country_stats = get_df_from_table("damodaran_country_stats", most_recent=True)

    tax_rate = 0
    country_default_spread = 0
    country_risk_premium = 0

    if geo_segments_df is None or geo_segments_df.empty:
        try:
            filter_df = country_stats[country_stats["country"] == country.replace(" ", "")].iloc[0]
        except:
            filter_df = country_stats[country_stats["country"] == "Global"].iloc[0]
        tax_rate = float(filter_df["tax_rate"])
        country_default_spread = float(filter_df["adjusted_default_spread"])
        country_risk_premium = float(filter_df["country_risk_premium"])

    else:
        for _, row in geo_segments_df.iterrows():
            percent = row["value"]
            search_key = row["country_area"]

            try:
                filter_df = country_stats[country_stats["country"] == search_key.replace(" ", "")].iloc[0]
            except:
                filter_df = country_stats[country_stats["country"] == "Global"].iloc[0]

            t = float(filter_df["tax_rate"])
            cds = float(filter_df["adjusted_default_spread"])
            crp = float(filter_df["country_risk_premium"])

            tax_rate += t * percent
            country_default_spread += cds * percent
            country_risk_premium += crp * percent

    final_erp = float(erp) + country_risk_premium

    # print("country", country)
    # print("db_fin_cur", db_financial_curr)

    try:
        alpha_3_code = country_stats[country_stats["country"] == country.replace(" ", "")].iloc[0]["alpha_3_code"]
    except:
        alpha_3_code = None
    riskfree = currency_bond_yield(db_financial_curr, alpha_3_code, country_stats)

    if riskfree == -1:
        print(ticker, "no riskfree")
        return null_valuation(price_per_share)

    if debug:
        print("===== GENERAL INFORMATION =====\n")
        print("ticker", ticker)
        print("cik", cik)
        print("company_name", company_name)
        print("country", country)
        print("region", region)
        print("industry", industry)
        print("financial currency", db_financial_curr)
        print("riskfree", riskfree)
        print("erp", erp)
        print("\n\n")

    mr_shares = data["mr_shares"]["value"] / 1000
    shares = get_selected_years(data, "shares", initial_year, final_year)

    # CONVERT CURRENCY
    fx_rate = None

    if db_curr is None or db_curr.strip() == "":
        return null_valuation(price_per_share)
    if db_financial_curr is None or db_financial_curr.strip() == "":
        return null_valuation(price_per_share)

    # they are different
    if db_curr != db_financial_curr:
        fx_rate = convert_currencies(db_curr, db_financial_curr)

    fx_rate_financial_USD = 1
    if db_financial_curr != "USD":
        fx_rate_financial_USD = convert_currencies("USD", db_financial_curr)

    ttm_revenue = data["ttm_revenue"]["value"] / 1000
    ttm_ebit = data["ttm_ebit"]["value"] / 1000
    ttm_net_income = data["ttm_net_income"]["value"] / 1000
    ttm_dividends = data["ttm_dividends"]["value"] / 1000
    ttm_interest_expense = data["ttm_interest_expenses"]["value"] / 1000
    mr_cash = data["mr_cash"]["value"] / 1000
    mr_securities = data["mr_securities"]["value"] / 1000
    mr_debt = data["mr_debt"]["value"] / 1000
    mr_equity = data["mr_equity"]["value"] / 1000
    ebit = get_selected_years(data, "ebit", initial_year, final_year)
    net_income = get_selected_years(data, "net_income", initial_year, final_year)
    dividends = get_selected_years(data, "dividends", initial_year, final_year)
    capex = get_selected_years(data, "capex", initial_year, final_year)
    depreciation = get_selected_years(data, "depreciation", initial_year, final_year)
    equity_bv = get_selected_years(data, "equity", initial_year, final_year)
    cash = get_selected_years(data, "cash", initial_year, final_year)
    securities = get_selected_years(data, "securities", initial_year, final_year)

    debt_bv = get_selected_years(data, "debt", initial_year, final_year)

    revenue = get_selected_years(data, "revenue", initial_year-1, final_year)
    revenue_growth = []
    revenue_delta = []
    for i in range(len(revenue) - 1):

        if revenue[i] < 0:
            print("negative revenue")
            return null_valuation(price_per_share)

        revenue_delta.append(revenue[i + 1] - revenue[i])
        try:
            revenue_growth.append(revenue[i + 1] / revenue[i] - 1)
        except:
            revenue_growth.append(0)

    # drop 1st element we don't need
    revenue = revenue[1:]
    revenue_growth = revenue_growth[1:]

    try:
        r_and_d_amortization_years = r_and_d_amortization[industry]
    except:
        print(f"\n#######\nCould not find industry: {industry} mapping. "
              f"Check r_and_d_amortization dictionary.\n#######\n")
        r_and_d_amortization_years = 5
    r_and_d = get_selected_years(data, "rd", final_year - r_and_d_amortization_years, final_year)
    while len(r_and_d) < years:
        r_and_d.insert(0, 0)

    ebit_r_and_d_adj, tax_benefit, r_and_d_unamortized, r_and_d_amortization_cy = \
        capitalize_rd(r_and_d, r_and_d_amortization_years, tax_rate, years)

    ttm_ebit_adj = ttm_ebit + ebit_r_and_d_adj[-1]
    ebit_adj = [sum(x) for x in zip(ebit, ebit_r_and_d_adj)]
    ttm_net_income_adj = ttm_net_income + ebit_r_and_d_adj[-1]
    net_income_adj = [sum(x) for x in zip(net_income, ebit_r_and_d_adj)]
    mr_equity_adj = mr_equity + r_and_d_unamortized[-1]
    equity_bv_adj = [sum(x) for x in zip(equity_bv, r_and_d_unamortized)]
    capex_adj = [sum(x) for x in zip(capex, r_and_d[-years:])]
    depreciation_adj = [sum(x) for x in zip(depreciation, r_and_d_amortization_cy)]
    ebit_after_tax = [sum(x) for x in zip([x * (1 - tax_rate) for x in ebit_adj], tax_benefit)]

    leases = [
        data["mr_op_leases_expense"]["value"] / 1000,
        data["mr_op_leases_next_year"]["value"] / 1000,
        data["mr_op_leases_next_2year"]["value"] / 1000,
        data["mr_op_leases_next_3year"]["value"] / 1000,
        data["mr_op_leases_next_4year"]["value"] / 1000,
        data["mr_op_leases_next_5year"]["value"] / 1000,
        data["mr_op_leases_after_5year"]["value"] / 1000,
    ]
    last_year_leases = max([i for i, x in enumerate(leases) if x != 0], default=-1)

    if last_year_leases != -1:
        ebit_op_adj, int_exp_op_adj, debt_adj, tax_benefit_op, company_default_spread = \
            debtize_op_leases(ttm_interest_expense, ttm_ebit_adj, damodaran_bond_spread, riskfree, country_default_spread,
                          leases, last_year_leases, tax_rate, revenue_growth)

        ttm_ebit_adj += ebit_op_adj[-1]
        ttm_interest_expense_adj = ttm_interest_expense + int_exp_op_adj
        mr_debt_adj = mr_debt + debt_adj[-1]
        ebit_adj = [sum(x) for x in zip(ebit_adj, ebit_op_adj)]
        debt_bv_adj = [sum(x) for x in zip(debt_bv, debt_adj)]
        ebit_after_tax = [sum(x) for x in zip(ebit_after_tax, tax_benefit_op)]

        ttm_ebit_after_tax = ttm_ebit_adj * (1 - tax_rate) + tax_benefit[-1] + tax_benefit_op[-1]

        # print("tax benefit rd", tax_benefit)
        # print("tax benefit op leas", tax_benefit_op)

    # no leases
    else:
        ttm_interest_expense_adj = ttm_interest_expense
        mr_debt_adj = mr_debt
        debt_bv_adj = debt_bv
        company_default_spread = get_spread_from_dscr(12.5, damodaran_bond_spread)
        ttm_ebit_after_tax = ttm_ebit_adj * (1 - tax_rate) + tax_benefit[-1]


    cost_of_debt = riskfree + country_default_spread + company_default_spread

    mr_cash_and_securities = mr_cash + mr_securities
    cash_and_securities = [sum(x) for x in zip(cash, securities)]

    # print("CASH", cash)
    # print("SECURITIES", securities)
    # print("CASH+SEC", cash_and_securities)

    # consider EPS/dividends as with most recent number of shares (to account for splits and buybacks)
    eps = [x/mr_shares for x in net_income]
    eps_adj = [x/mr_shares for x in net_income_adj]
    dividends = [x/mr_shares for x in dividends]

    # WC = inventory + receivables + other assets - payables - due to affiliates - due to related
    l = {}
    for i in ["inventory","receivables","other_assets","account_payable","due_to_affiliates","due_to_related_parties"]:
        val = get_selected_years(data, i, initial_year-1, final_year)
        l[i] = val

    df = pd.DataFrame(l)
    df["wc"] = df["inventory"] + df["receivables"] + df["other_assets"] - df["account_payable"] \
               - df["due_to_affiliates"] - df["due_to_related_parties"]
    df["delta_wc"] = df["wc"].diff(1)
    df = df.dropna()

    working_capital = df["wc"].to_list()
    delta_wc = df["delta_wc"].to_list()

    ttm_eps = ttm_net_income / mr_shares
    ttm_eps_adj = ttm_net_income_adj / mr_shares

    print(working_capital, "=>", delta_wc)
    print(capex, "=>", capex_adj)
    print(depreciation_adj)

    reinvestment = []
    for i in range(len(capex)):
        reinvestment.append(capex_adj[i] + delta_wc[i] - depreciation_adj[i])

    equity_mkt = mr_shares * price_per_share
    if fx_rate is not None:
        equity_mkt /= fx_rate

    debt_mkt = ttm_interest_expense_adj * (1 - (1 + cost_of_debt) ** -6) / cost_of_debt + mr_debt_adj / (
                1 + cost_of_debt) ** 6

    target_sales_capital, industry_payout, pbv, unlevered_beta, target_operating_margin, target_debt_equity = \
        get_industry_data(industry, region, geo_segments_df, revenue, ebit_adj, revenue_delta, reinvestment,
                          equity_mkt, debt_mkt, equity_bv_adj, debt_bv_adj, mr_equity_adj, mr_debt_adj)

    mr_original_min_interest = data["mr_minority_interest"]["value"] / 1000
    mr_minority_interest = mr_original_min_interest * pbv

    # print("PBV", pbv)
    # print(mr_minority_interest)

    mr_tax_benefits = data["mr_tax_benefits"]["value"] / 1000
    mr_sbc = data["mr_sbc"]["value"] / 1000

    if debug:
        print("===== Last Available Data =====\n")
        print("Outstanding Shares", mr_shares)
        print("Price/Share (price currency)", price_per_share)
        print("FX Rate:", 1 if fx_rate is None else fx_rate)
        print("FX Rate USD:", fx_rate_financial_USD)
        print("ttm_revenue", ttm_revenue)
        print("ttm_ebit", ttm_ebit, "=>", ttm_ebit_adj)
        print("ttm_net_income", ttm_net_income, "=>", ttm_net_income_adj)
        print("ttm_dividends", ttm_dividends)
        # print("ttm_eps", ttm_eps, "=>", ttm_eps_adj)
        print("ttm_interest_expense", ttm_interest_expense, "=>", ttm_interest_expense_adj)
        print("tax_credit", mr_tax_benefits)
        # print("minority_interest", mr_original_min_interest, "=>", mr_minority_interest)
        # print("cash&securities", mr_cash_and_securities)
        # print("BV of debt", mr_debt, "=>", mr_debt_adj)
        # print("BV of equity", mr_equity, "=>", mr_equity_adj)
        print("\n\n")
        print("===== Historical Data =====\n")
        print("initial_year", initial_year)
        print("revenue", revenue)
        print("revenue_delta", revenue_delta)
        print("ebit", ebit, "=>", ebit_adj)
        # print("ebit_after_tax_adj", ebit_after_tax)
        print("net_income", net_income, "=>", net_income_adj)
        # print("eps", eps, "=>", eps_adj)
        print("dividends", dividends)
        print("working_capital", working_capital)
        print("delta_WC", delta_wc)
        print("capex", capex, "=>", capex_adj)
        print("depreciation", depreciation, "=>", depreciation_adj)
        print("shares_outstanding", shares)
        print("equity_bv", equity_bv, "=>", equity_bv_adj)
        print("cash&securities", cash_and_securities)
        print("debt_bv", debt_bv, "=>", debt_bv_adj)
        print("\n\n")
        print("===== R&D =====")
        print("r_and_d", r_and_d)
        print("amortization_years", r_and_d_amortization_years)
        print("\n===== Operating Leases =====")
        print("leases", leases)
        print("\n===== Segments =====\n")
        if geo_segments_df is None:
            print("10-K not found. Check annual report on company website.")
        else:
            print(geo_segments_df.to_markdown())
        print("\n===== Options =====")
        print("mr_sbc", mr_sbc)
        print("\n\n")

    roc_last, reinvestment_last, growth_last, roe_last, reinvestment_eps_last, growth_eps_last = \
        get_growth_ttm(ttm_ebit_after_tax, ttm_net_income_adj, mr_equity_adj, mr_debt_adj, mr_cash_and_securities,
                       reinvestment, ttm_dividends, industry_payout)

    roe, roc = get_roe_roc(equity_bv_adj, debt_bv_adj, cash_and_securities, ebit_after_tax, net_income_adj)

    cagr, target_levered_beta, target_cost_of_equity, target_cost_of_debt, target_cost_of_capital = \
        get_target_info(revenue, ttm_revenue, country_default_spread, tax_rate, final_erp, riskfree,
                        unlevered_beta, damodaran_bond_spread, company_default_spread, target_debt_equity)

    revenue_5y, ebit_5y, operating_margin_5y, sales_capital_5y, roc_5y, reinvestment_5y, growth_5y, \
    net_income_5y, roe_5y, reinvestment_eps_5y, growth_eps_5y = \
        get_normalized_info(revenue, ebit_adj, revenue_delta, reinvestment, target_sales_capital,
                        ebit_after_tax, industry_payout, cagr, net_income_adj, roe, dividends, eps_adj, roc)

    eps_5y, payout_5y = get_dividends_info(eps_adj, dividends)

    survival_prob, debt_equity, \
    levered_beta, cost_of_equity, equity_weight, debt_weight, cost_of_capital = \
        get_final_info(riskfree, cost_of_debt, equity_mkt, debt_mkt, unlevered_beta,
                   tax_rate, final_erp, company_default_spread)

    mr_receivables = data["mr_receivables"]["value"] / 1000
    mr_inventory = data["mr_inventory"]["value"] / 1000
    mr_other_current_assets = data["mr_other_assets"]["value"] / 1000
    mr_ppe = data["mr_ppe"]["value"] / 1000
    mr_property = data["mr_investment_property"]["value"] / 1000
    mr_equity_investments = data["mr_equity_investments"]["value"] / 1000
    mr_total_liabilities = data["mr_liabilities"]["value"] / 1000

    try:
        liquidation_value = calculate_liquidation_value(mr_cash, mr_receivables, mr_inventory, mr_securities,
                                                        mr_other_current_assets, mr_property,
                                                        mr_ppe, mr_equity_investments, mr_total_liabilities, equity_mkt,
                                                        mr_debt, mr_equity, mr_original_min_interest,
                                                        mr_minority_interest, debug=debug)
    except:
        print(traceback.format_exc())
        liquidation_value = 0

    if debug:
        print("===== Growth =====\n")
        print("cagr", round(cagr,4))
        print("riskfree", round(riskfree,4))
        print("\n\n")
        print("===== Model Helper Calculation =====\n")
        print("roc_last", round(roc_last,4))
        print("reinvestment_last", round(reinvestment_last,4))
        print("growth_last", round(growth_last,4))
        print("ROC history", roc)
        print("roc_5y", round(roc_5y,4))
        print("Reinvestment history", reinvestment)
        print("reinvestment_5y", round(reinvestment_5y,4))
        print("growth_5y", round(growth_5y,4))
        print("revenue_5y", revenue_5y)
        print("ebit_5y", ebit_5y)
        print("roe_last", round(roe_last,4))
        print("reinvestment_eps_last", round(reinvestment_eps_last,4))
        print("growth_eps_last", round(growth_eps_last,4))
        print("sales_capital_5y", round(sales_capital_5y,4))
        print("roe_5y", round(roe_5y,4))
        print("reinvestment_eps_5y", round(reinvestment_eps_5y,4))
        print("growth_eps_5y", round(growth_eps_5y,4))
        print("eps_5y", round(eps_5y,4))
        print("payout_5y", round(payout_5y,4))
        print("industry_payout", round(industry_payout,4))
        print("target_sales_capital", round(target_sales_capital,4))
        print("\n\n")
        print("===== Recap Info =====\n")
        print("country_default_spread", round(country_default_spread,4))
        print("country_risk_premium", round(country_risk_premium,4))
        print("riskfree", round(riskfree,4))
        print("final_erp", round(final_erp,4))
        print("unlevered_beta", round(unlevered_beta,4))
        print("tax_rate", round(tax_rate,4))
        print("levered_beta", round(levered_beta,4))
        print("cost_of_equity", round(cost_of_equity,4))
        print("cost_of_debt", round(cost_of_debt,4))
        print("equity_weight", round(equity_weight,4))
        print("debt_weight", round(debt_weight,4))
        print("cost_of_capital", round(cost_of_capital,4))
        print("equity_mkt", round(equity_mkt,2))
        print("debt_mkt", round(debt_mkt,2))
        print("debt_equity", round(debt_equity,4))
        print("equity_bv_adj", round(mr_equity_adj,2))
        print("debt_bv_adj", round(mr_debt_adj,2))
        print("ebit_adj", round(ttm_ebit_adj,2))
        print("company_default_spread", round(company_default_spread,4))
        print("survival_prob", round(survival_prob,4))
        print("liquidation value", round(liquidation_value, 2))
        print("\n\n")
        print("===== Other Model inputs =====\n")
        print("operating_margin_5y", round(operating_margin_5y,4))
        print("target_operating_margin", round(target_operating_margin,4))
        print("target_debt_equity", round(target_debt_equity,4))
        print("target_levered_beta", round(target_levered_beta,4))
        print("target_cost_of_equity", round(target_cost_of_equity,4))
        print("target_cost_of_debt", round(target_cost_of_debt,4))
        print("target_cost_of_capital", round(target_cost_of_capital,4))
        print("\n\n")


    stock_value_div_ttm_fixed = dividends_valuation(EARNINGS_TTM, GROWTH_FIXED, cagr, growth_eps_5y, growth_5y,
                                                    riskfree, industry_payout, cost_of_equity,
                                                    target_cost_of_equity, growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug)
    stock_value_div_norm_fixed = dividends_valuation(EARNINGS_NORM, GROWTH_FIXED, cagr, growth_eps_5y, growth_5y,
                                                     riskfree, industry_payout, cost_of_equity,
                                                     target_cost_of_equity, growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug)
    stock_value_div_ttm_ttm = dividends_valuation(EARNINGS_TTM, GROWTH_TTM, cagr, growth_eps_5y, growth_5y, riskfree,
                                                  industry_payout, cost_of_equity, target_cost_of_equity,
                                                  growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug)
    stock_value_div_norm_norm = dividends_valuation(EARNINGS_NORM, GROWTH_NORM, cagr, growth_eps_5y, growth_5y, riskfree,
                                                    industry_payout, cost_of_equity,
                                                    target_cost_of_equity, growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug)
    stock_value_div_ttm_fixed_recession = dividends_valuation(EARNINGS_TTM, GROWTH_FIXED, cagr, growth_eps_5y, growth_5y,
                                                    riskfree, industry_payout, cost_of_equity,
                                                    target_cost_of_equity, growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug, recession=True)
    stock_value_div_norm_fixed_recession = dividends_valuation(EARNINGS_NORM, GROWTH_FIXED, cagr, growth_eps_5y, growth_5y,
                                                     riskfree, industry_payout, cost_of_equity,
                                                     target_cost_of_equity, growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug, recession=True)
    stock_value_div_ttm_ttm_recession = dividends_valuation(EARNINGS_TTM, GROWTH_TTM, cagr, growth_eps_5y, growth_5y, riskfree,
                                                  industry_payout, cost_of_equity, target_cost_of_equity,
                                                  growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug, recession=True)
    stock_value_div_norm_norm_recession = dividends_valuation(EARNINGS_NORM, GROWTH_NORM, cagr, growth_eps_5y, growth_5y, riskfree,
                                                    industry_payout, cost_of_equity,
                                                    target_cost_of_equity, growth_eps_last, eps_5y, payout_5y, ttm_eps_adj,
                                                    reinvestment_eps_last, fx_rate, debug=debug, recession=True)

    stock_value_fcff_ttm_fixed = fcff_valuation(EARNINGS_TTM, GROWTH_FIXED, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug)
    stock_value_fcff_norm_fixed = fcff_valuation(EARNINGS_NORM, GROWTH_FIXED, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                 target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                 debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                 target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                 liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug)
    stock_value_fcff_ttm_ttm = fcff_valuation(EARNINGS_TTM, GROWTH_TTM, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                              target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                              debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                              target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                              liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug)
    stock_value_fcff_norm_norm = fcff_valuation(EARNINGS_NORM, GROWTH_NORM, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug)
    stock_value_fcff_ttm_fixed_recession = fcff_valuation(EARNINGS_TTM, GROWTH_FIXED, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                          target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                          debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                          target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                          liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug, recession=True)
    stock_value_fcff_norm_fixed_recession = fcff_valuation(EARNINGS_NORM, GROWTH_FIXED, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                           target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                           debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                           target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                           liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug, recession=True)
    stock_value_fcff_ttm_ttm_recession = fcff_valuation(EARNINGS_TTM, GROWTH_TTM, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                        target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                        debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                        target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                        liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug, recession=True)
    stock_value_fcff_norm_norm_recession = fcff_valuation(EARNINGS_NORM, GROWTH_NORM, cagr, riskfree, ttm_revenue, ttm_ebit_adj,
                                                          target_operating_margin, mr_tax_benefits, tax_rate, sales_capital_5y, target_sales_capital,
                                                          debt_equity, target_debt_equity, unlevered_beta, final_erp, cost_of_debt,
                                                          target_cost_of_debt, mr_cash, mr_securities, debt_mkt, mr_minority_interest, survival_prob, mr_shares,
                                                          liquidation_value, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=debug, recession=True)

    fcff_values_list = [stock_value_fcff_ttm_fixed, stock_value_fcff_norm_fixed, stock_value_fcff_ttm_ttm,
                       stock_value_fcff_norm_norm]
    fcff_recession_values_list = [stock_value_fcff_ttm_fixed_recession, stock_value_fcff_norm_fixed_recession,
                                              stock_value_fcff_ttm_ttm_recession, stock_value_fcff_norm_norm_recession]
    div_values_list = [stock_value_div_ttm_fixed, stock_value_div_norm_fixed, stock_value_div_ttm_ttm,
                       stock_value_div_norm_norm]
    div_recession_values_list = [stock_value_div_ttm_fixed_recession, stock_value_div_norm_fixed_recession,
                                             stock_value_div_ttm_ttm_recession, stock_value_div_norm_norm_recession]

    fcff_value = summary_valuation(fcff_values_list)
    fcff_recession_value = summary_valuation(fcff_recession_values_list)
    ev_fcff = fcff_value * (1 - recession_probability) + fcff_recession_value * recession_probability
    div_value = summary_valuation(div_values_list)
    div_recession_value = summary_valuation(div_recession_values_list)
    ev_dividends = div_value * (1 - recession_probability) + div_recession_value * recession_probability

    liquidation_per_share = liquidation_value / mr_shares

    if fx_rate is not None:
        fcff_value *= fx_rate
        div_value *= fx_rate
        liquidation_per_share *= fx_rate

    fcff_delta = price_per_share / ev_fcff - 1 if fcff_value > 0 else 10
    div_delta = price_per_share / ev_dividends - 1 if div_value > 0 else 10
    liquidation_delta = price_per_share / liquidation_per_share - 1 if liquidation_per_share > 0 else 10

    market_cap_USD = equity_mkt * fx_rate_financial_USD
    if market_cap_USD < 50 * 10 ** 3:
        company_size = "Nano"
    elif market_cap_USD < 300 * 10 ** 3:
        company_size = "Micro"
    elif market_cap_USD < 2 * 10 ** 6:
        company_size = "Small"
    elif market_cap_USD < 10 * 10 ** 6:
        company_size = "Medium"
    elif market_cap_USD < 200 * 10 ** 6:
        company_size = "Large"
    else:
        company_size = "Mega"

    complexity = company_complexity(doc, industry, company_size)
    dilution = company_share_diluition(shares)

    inventory = get_selected_years(data, "inventory", initial_year-1, final_year)
    receivables = get_selected_years(data, "receivables", initial_year-1, final_year)
    company_type = get_company_type(revenue_growth, mr_debt_adj, equity_mkt, liquidation_value, operating_margin_5y, industry)
    auditor = find_auditor(doc)

    if debug:
        print("===== Risk Assessment =====\n")
        print("MKT CAP USD: ", market_cap_USD)
        print("company_size", company_size)
        print("company complexity", complexity)
        print("share dilution", round(dilution, 4))
        print("revenue", revenue)
        print("inventory", inventory)
        print("receivables", receivables)
        print("company_type", company_type)
        print("Auditor", auditor)
        print()

    status = get_status(fcff_delta, div_delta, liquidation_delta, country, region, company_size, company_type, dilution, complexity,
                        revenue, receivables, inventory, debug)

    if debug:
        print("FCFF values")
        print([round(x, 2) for x in fcff_values_list])
        print("\nFCFF values w/ Recession")
        print([round(x, 2) for x in fcff_recession_values_list])
        print("\n\nDiv values")
        print([round(x, 2) for x in div_values_list])
        print("\nDiv values w/ Recession")
        print([round(x, 2) for x in div_recession_values_list])

        print("\n\n\n")

        print("Price per Share", price_per_share)
        print("FCFF Result", ev_fcff)
        print("FCFF Deviation", fcff_delta)
        print("Dividends Result", ev_dividends)
        print("Dividends Deviation", div_delta)
        print("Status", status)


    if qualitative and doc is not None:
        recent_docs = get_recent_docs(cik, doc["filing_date"])
        for d in recent_docs:

            print("##############")
            print(d["form_type"], d["filing_date"], d["_id"])
            print("##############\n")

            if not mongodb.check_document_exists("parsed_documents", d["_id"]):
                parse_document(d, d["form_type"])

            parsed_doc = mongodb.get_document("parsed_documents", d["_id"])

            if not mongodb.check_document_exists("items_summary", d["_id"]):
                sections_summary(parsed_doc)

            summary_doc = mongodb.get_document("items_summary", d["_id"])

            for k, v in summary_doc.items():
                if isinstance(v, list):
                    print(f"=== {k} ===")
                    for el in v:
                        print(el)
                    print()

            print("\n")


    return price_per_share, fcff_value, div_value, fcff_delta, div_delta, liquidation_per_share, liquidation_delta, status

if __name__ == '__main__':
    cik = cik_from_ticker("BLDR")
    if cik != -1:
        valuation(cik, debug=True, years=6)
