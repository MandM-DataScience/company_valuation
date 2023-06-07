import pandas as pd
import mongodb
from edgar_utils import ATKR_CIK


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
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"])

    df["end"] = pd.to_datetime(df["end"])
    df["filed"] = pd.to_datetime(df["filed"])
    df = df[~df.frame.isna()]
    return df

def get_ttm_from_df(df):
    """
    Compute TTM (trailing twelve months) value from a DataFrame containing quarterly and annual values.
    :param ttm_df: DataFrame containing quarterly and annual values
    :return: ttm value, year of last annual value in DataFrame
    """

    # create a copy as we are going to edit and filter it
    ttm_df = df.copy()

    # Keep only annual and quarterly periods
    ttm_df["period"] = (ttm_df["end"] - ttm_df["start"]).dt.days
    ttm_df = ttm_df[~(ttm_df.frame.str.contains("Q")) | ((ttm_df.frame.str.contains("Q")) & (ttm_df.period < 100))]

    # Get last annual value
    last_yearly_row = ttm_df[ttm_df.period > 100].iloc[-1]

    # Get quarterly values AFTER the annual value
    post_quarterly_rows = ttm_df[ttm_df.index > last_yearly_row.name]

    # Get corresponding quarterly values BEFORE the annual value
    pre_frames = list(post_quarterly_rows.frame)
    pre_frames = [x[:2] + str(int(x[2:6]) - 1) + x[6:] for x in pre_frames]
    pre_quarterly_rows = ttm_df[ttm_df.frame.isin(pre_frames)]

    # TTM = annual value + quarterly values after - corresponding quarterly values before
    ttm = last_yearly_row.val + post_quarterly_rows.val.sum() - pre_quarterly_rows.val.sum()

    return ttm, last_yearly_row.name

def get_most_recent_value_from_df(df):
    """
    Get most recent value and date in DataFrame (last row)
    :param df: DataFrame containing quarterly and annual values
    :return: most recent value and date in DataFrame
    """
    return df.iloc[-1]["val"], df.iloc[-1]["end"]

def get_yearly_values_from_df(df, instant=False, last_annual_report_date=None):

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

        return {"dates": list((year_df.frame.str.replace("CY", "")).astype(int)),
                "values": list(year_df.val),
                "last_annual_report_date": year_df.iloc[-1].end if len(year_df) > 0 else None}

    # balance sheet
    else:

        # in this case we need last_annual_report_date to know what is the quarter of annual reports
        # (every company can have a different fiscal year)
        if last_annual_report_date is None:
            return

        last_annual_report_row = year_df[year_df.end == last_annual_report_date]
        if last_annual_report_row.empty:
            return

        # frame is a string CYXXXXQXI, we want the X between Q and I
        quarter_of_annual_report = last_annual_report_row.iloc[0]["frame"][7]

        # keep only only rows with quarters of annual reports
        year_df = year_df[year_df.frame.str.contains(f"Q{quarter_of_annual_report}I")]

        return {"dates": list((year_df.frame.str.replace("CY", "")
                               .str.replace(f"Q{quarter_of_annual_report}I","")).astype(int)),
                "values": list(year_df.val),
                "last_annual_report_date": year_df.iloc[-1].end}

def get_values_from_measures(doc, measures, get_ttm=True, get_most_recent=True, get_yearly=True, instant=False,
                             last_annual_report_date=None, debug=False):

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
        df = build_financial_df(doc, m)

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
            most_recent_value_tmp, most_recent_date_tmp = get_most_recent_value_from_df(df)

            if most_recent_value_tmp is not None:

                # We override most_recent_value if we have a more recent value
                if most_recent_date is None or most_recent_date_tmp > most_recent_date:
                    most_recent_date = most_recent_date_tmp
                    most_recent = most_recent_value_tmp

                if debug:
                    print(m, most_recent_date_tmp, most_recent_value_tmp)

        if get_yearly:

            # Get yearly values
            yearly_tmp = get_yearly_values_from_df(df, instant, last_annual_report_date)

            if yearly_tmp is not None:

                # for each date
                for i, d in enumerate(yearly_tmp["dates"]):

                    # if we don't have it already (hierarchical), we add the values
                    if d not in yearly["dates"]:
                        yearly["dates"].append(d)
                        yearly["values"].append(yearly_tmp["values"][i])

                # update last_annual_report_date to the most recent one
                if last_annual_report_date is None or yearly_tmp["last_annual_report_date"] > last_annual_report_date:
                        last_annual_report_date = yearly_tmp["last_annual_report_date"]

                if debug:
                    print(m, yearly_tmp)

    # sort dates and values from the least recent to the most recent
    sort = sorted(zip(yearly["dates"], yearly["values"]))
    yearly["dates"] = [x for x, _ in sort]
    yearly["values"] = [x for _, x in sort]
    yearly["last_annual_report_date"] = last_annual_report_date

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
    :return: superset with the added values (if any)
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

    replace = False
    for s in subsets:
        if superset["date"] is None or s["date"] > superset["date"]:
            replace = True
            break

    if replace:

        d = max([x["date"] for x in subsets])
        superset["date"] = d
        superset["value"] = 0

        for s in subsets:
            if s["date"] == d:
                superset["value"] += s["value"]

def extract_shares(doc):
    """
    Extract number of shares from company financial document
    :param doc: company financial document
    :return: number of common shares outstanding
    """
    df = build_financial_df(doc, "EntityCommonStockSharesOutstanding", unit="shares", tax="dei")
    return get_most_recent_value_from_df(df)

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

    _, ttm_interest_expenses, _ = get_values_from_measures(doc, measures, get_most_recent=False, get_ttm=False,
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
        "net_income": yearly_net_income
    }

def extract_balance_sheet_current_assets(doc, last_annual_report_date):
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

    merge_subsets_yearly(yearly_cash_and_restricted, [yearly_cash, yearly_restrictedcash], must_include=(0,))

    if most_recent_cash["date"] > most_recent_cash_and_restricted["date"]:
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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)
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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["AccountsReceivableNetCurrent",
                "AccountsReceivableNet",
                "AccountsReceivableGrossCurrent",
                "AccountsReceivableGross"]
    most_recent_ar, _, yearly_ar = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["LoansAndLeasesReceivableNetReportedAmount",
                "LoansAndLeasesReceivableNetOfDeferredIncome",
                "LoansReceivableFairValueDisclosure",
                "LoansAndLeasesReceivableGrossCarryingAmount"]
    most_recent_loans_rec, _, yearly_loans_rec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["NotesReceivableNet",
                "NotesReceivableFairValueDisclosure",
                "NotesReceivableGross"]
    most_recent_notes_rec, _, yearly_notes_rec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_receivables, [yearly_ar, yearly_loans_rec, yearly_notes_rec])
    merge_subsets_most_recent(most_recent_receivables,
                              [most_recent_ar, most_recent_loans_rec, most_recent_notes_rec])

    #### Securities ####
    measures = [
        "MarketableSecurities"
        "AvailableForSaleSecurities"]
    most_recent_securities, _, yearly_securities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesDebtSecurities"]
    most_recent_debtsecurities, _, yearly_debtsecurities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["AvailableForSaleSecuritiesEquitySecurities"]
    most_recent_equitysecurities, _, yearly_equitysecurities = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_securities, [yearly_debtsecurities, yearly_equitysecurities])
    merge_subsets_most_recent(most_recent_securities,
                              [most_recent_debtsecurities, most_recent_equitysecurities])

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

    merge_subsets_yearly(yearly_securities, [yearly_derivatives, yearly_held_securities, yearly_non_curr_sec,
                                             yearly_marksecurities_cur, yearly_st_inv, yearly_mm, yearly_debtsecurities_cur])
    merge_subsets_most_recent(most_recent_securities,
                              [most_recent_derivatives, most_recent_held_securities, most_recent_non_curr_sec,
                               most_recent_marksecurities_cur, most_recent_st_inv, most_recent_mm,
                               most_recent_debtsecurities_cur])

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

def extract_balance_sheet_noncurrent_assets(doc, last_annual_report_date):
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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False,
        debug=False)

    measures = [
        "EquityMethodInvestmentsFairValueDisclosure",
        "InvestmentOwnedAtFairValue",
        "InvestmentsFairValueDisclosure",
    ]
    most_recent_equity_inv_fv, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["EquitySecuritiesWithoutReadilyDeterminableFairValueAmount", ]
    most_recent_equity_inv_notfv, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    # merge_subsets_yearly(yearly_equity_investments, [yearly_equity_inv_fv, yearly_equity_inv_notfv])
    merge_subsets_most_recent(most_recent_equity_investments,
                              [most_recent_equity_inv_fv, most_recent_equity_inv_notfv])

    measures = ["MarketableSecuritiesNoncurrent"]
    most_recent_securities_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    yearly_equity_investments_and_securities = {"dates": [], "values": []}
    # merge_subsets_yearly(yearly_equity_investments_and_securities, [yearly_equity_investments, yearly_securities_non_curr])

    if most_recent_securities_non_curr["date"] > most_recent_equity_investments["date"]:
        most_recent_equity_investments["date"] = most_recent_securities_non_curr["date"]
        most_recent_equity_investments["value"] = most_recent_securities_non_curr["value"]


    #### Other financial assets ####
    measures = [
        "PrepaidExpenseNoncurrent",
        "PrepaidExpenseOtherNoncurrent",
    ]
    most_recent_prepaid_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "RestrictedCashAndCashEquivalentsNoncurrent",
        "RestrictedCashAndInvestmentsNoncurrent",
        "RestrictedCashNoncurrent"
    ]
    most_recent_cash_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeAssetsNoncurrent", ]
    most_recent_derivatives_non_curr, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["EscrowDeposit"]
    most_recent_escrow, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    #### Investment property ####
    measures = [
        "RealEstateInvestments",
        "RealEstateInvestmentPropertyNet",
        "RealEstateInvestmentPropertyAtCost",
        "RealEstateHeldforsale"
    ]
    most_recent_property, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["InvestmentBuildingAndBuildingImprovements"]
    most_recent_buildings, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "LandAndLandImprovements",
        "Land",
    ]
    most_recent_land, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    return {
        "mr_equity_investments": most_recent_equity_investments,
        "mr_other_financial_assets": most_recent_other_financial_assets,
        "mr_ppe": most_recent_ppe,
        "mr_investment_property": most_recent_property,
        "mr_tax_benefits": most_recent_tax_benefit
    }

def extract_balance_sheet_debt(doc, last_annual_report_date):
    """
    Extract balance sheet DEBT measures (Current + Non-Current) from company financial document.
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent and yearly measures
    """

    # DEBT LONG + SHORT
    measures = ["DebtLongtermAndShorttermCombinedAmount"]
    most_recent_debt, _, yearly_debt = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["MortgageLoansOnRealEstate"]
    most_recent_mortgage, _, yearly_mortgage = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["OtherBorrowings"]
    most_recent_other_borr, _, yearly_other_borr = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    # DEBT SHORT
    measures = [
        "ShortTermBorrowings",
        "ShorttermDebtAverageOutstandingAmount",
        "ShorttermDebtFairValue",
    ]
    most_recent_debt_st, _, yearly_debt_st = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "CommercialPaper",
        "CommercialPaperAtCarryingValue",
    ]
    most_recent_cp, _, yearly_cp = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["BankOverdrafts"]
    most_recent_overdraft, _, yearly_overdraft = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["ShortTermBankLoansAndNotesPayable"]
    most_recent_loans_st, _, yearly_loans_st = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["BridgeLoan"]
    most_recent_bridge, _, yearly_bridge = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    # DEBT LONG
    measures = [
        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
        "LongTermDebt",
        "LongTermDebtFairValue",
        "DebtInstrumentFaceAmount",
        "DebtInstrumentCarryingAmount",
    ]
    most_recent_debt_lt, _, yearly_debt_lt = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "ConvertibleDebt",
        "ConvertibleNotesPayable",
    ]
    most_recent_convertible, _, yearly_convertible = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "LineOfCredit",
        "LineOfCreditFacilityFairValueOfAmountOutstanding",
    ]
    most_recent_revolver, _, yearly_revolver = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["LoansPayable"]
    most_recent_loans_pay, _, yearly_loans_pay = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["SecuredDebt"]
    most_recent_debt_sec, _, yearly_debt_sec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["NotesPayable",
                "SeniorNotes"]
    most_recent_debt_notes, _, yearly_debt_notes = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["UnsecuredDebt"]
    most_recent_debt_unsec, _, yearly_debt_unsec = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    # DEBT LONG - CURRENT
    measures = [
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "LongTermDebtCurrent",
    ]
    most_recent_debt_lt_cur, _, yearly_debt_lt_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "ConvertibleDebtCurrent",
        "ConvertibleNotesPayableCurrent",
    ]
    most_recent_convertible_cur, _, yearly_convertible_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["LinesOfCreditCurrent"]
    most_recent_revolver_cur, _, yearly_revolver_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["NotesPayableCurrent",
                "SeniorNotesCurrent"]
    most_recent_notes_cur, _, yearly_notes_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["SecuredDebtCurrent"]
    most_recent_debt_sec_cur, _, yearly_debt_sec_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["UnsecuredDebtCurrent"]
    most_recent_debt_unsec_cur, _, yearly_debt_unsec_cur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    # DEBT LONG - NON CURRENT
    measures = [
        "LongTermDebtAndCapitalLeaseObligations",
        "LongTermDebtNoncurrent",
    ]
    most_recent_debt_lt_noncur, _, yearly_debt_lt_noncur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = [
        "ConvertibleDebtNoncurrent",
        "ConvertibleLongTermNotesPayable",
    ]
    most_recent_convertible_noncur, _, yearly_convertible_noncur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["LongTermLineOfCredit"]
    most_recent_revolver_noncur, _, yearly_revolver_noncur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["LongTermNotesPayable",
                "SeniorLongTermNotes"]
    most_recent_notes_noncur, _, yearly_notes_noncur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["SecuredLongTermDebt"]
    most_recent_debt_sec_noncur, _, yearly_debt_sec_noncur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["UnsecuredLongTermDebt"]
    most_recent_debt_unsec_noncur, _, yearly_debt_unsec_noncur = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

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

def extract_balance_sheet_liabilities(doc, last_annual_report_date, most_recent_debt):
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
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeLiabilities"]
    most_recent_derivatives_liability, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent",
        "AccountsPayableCurrentAndNoncurrent",
    ]
    most_recent_ap, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToRelatedPartiesCurrentAndNoncurrent"]
    most_recent_due_related_parties, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToAffiliateCurrentAndNoncurrent"]
    most_recent_due_affiliates, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    # yearly_liabilities_ex_debt = {"dates": [], "values": []}
    # merge_subsets_yearly(yearly_liabilities_ex_debt, [yearly_derivatives_liability, yearly_ap, yearly_due_related_parties,
    #                                                   yearly_due_affiliates])

    # Current
    measures = ["LiabilitiesCurrent"]
    most_recent_liabilities_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = [
        "AccountsPayableAndOtherAccruedLiabilitiesCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
    ]
    most_recent_ap_complete_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["AccountsPayableCurrent"]
    most_recent_ap_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["AccountsPayableOtherCurrent"]
    most_recent_apother_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["AccountsPayableRelatedPartiesCurrent"]
    most_recent_ap_rel_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["AccountsPayableTradeCurrent"]
    most_recent_ap_trade_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    # merge_subsets_yearly(yearly_ap_complete_cur, [yearly_ap_cur, yearly_apother_cur, yearly_ap_rel_cur,
    #                                               yearly_ap_trade_cur])


    measures = ["DueToAffiliateCurrent"]
    most_recent_due_affiliates_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToRelatedPartiesCurrent"]
    most_recent_due_related_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeLiabilitiesCurrent"]
    most_recent_derivatives_liability_cur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    # merge_subsets_yearly(yearly_liabilities_cur, [yearly_ap_complete_cur, yearly_due_affiliates_cur, yearly_due_related_cur,
    #                                               yearly_derivatives_liability_cur])

    # Non - Current
    measures = ["LiabilitiesNoncurrent"]
    most_recent_liabilities_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DerivativeLiabilitiesNoncurrent"]
    most_recent_derivatives_liability_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToAffiliateNoncurrent"]
    most_recent_due_affiliates_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

    measures = ["DueToRelatedPartiesNoncurrent"]
    most_recent_due_related_noncur, _, _ = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, get_yearly=False, debug=False)

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
    merge_subsets_most_recent(most_recent_liabilities, [most_recent_ap, most_recent_derivatives_liability,
                                                        most_recent_due_affiliates, most_recent_due_related_parties,
                                                        most_recent_debt])

    return {
        "mr_liabilities": most_recent_liabilities
    }

def extract_balance_sheet_equity(doc, last_annual_report_date):
    """
    Extract balance sheet EQUITY measures from company financial document.
    :param doc: company financial document
    :param last_annual_report_date: date of last annual report
    :return: dict with most recent and yearly measures
    """

    # measures = ["LiabilitiesAndStockholdersEquity"]
    # most_recent_liabilities_and_equity, _, yearly_liabilities_and_equity = get_values_from_measures(
    #     doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
    most_recent_equity, _, yearly_equity = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["StockholdersEquity"]
    most_recent_equity_no_mi, _, yearly_equity_no_mi = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    measures = ["MinorityInterest"]
    most_recent_minority_interest, _, yearly_minority_interest = get_values_from_measures(
        doc, measures, instant=True, last_annual_report_date=last_annual_report_date, get_ttm=False, debug=False)

    merge_subsets_yearly(yearly_equity, [yearly_equity_no_mi, yearly_minority_interest], (0,))

    if most_recent_equity_no_mi["date"] > most_recent_equity["date"]:
        merge_subsets_most_recent(most_recent_equity, [most_recent_equity_no_mi, most_recent_minority_interest])

    return {
        "mr_equity": most_recent_equity,
        "equity": yearly_equity,
        "mr_minority_interest": most_recent_minority_interest
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
    ]
    _, ttm_dividends, yearly_dividends = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                  debug=False)

    measures = [
        "DividendsCommonStock",
        "DividendsCommonStockCash",
    ]
    _, ttm_dividends_cs, yearly_dividends_cs = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)

    measures = [
        "DividendsPreferredStock",
        "DividendsPreferredStockCash",
    ]
    _, ttm_dividends_ps, yearly_dividends_ps = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)
    merge_subsets_yearly(yearly_dividends, [yearly_dividends_cs, yearly_dividends_ps])
    merge_subsets_most_recent(ttm_dividends, [ttm_dividends_cs, ttm_dividends_ps])

    # CAPEX

    # Acquisition
    measures = ["BusinessAcquisitionCostOfAcquiredEntityTransactionCosts"]
    _, ttm_acquisition_costs, yearly_acquisition_costs = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                  debug=False)

    measures = [
        "PaymentsForPreviousAcquisition",
        "PaymentsForProceedsFromPreviousAcquisition",
    ]
    _, ttm_acquisition_adj, yearly_acquisition_adj = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                              debug=False)

    measures = [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "PaymentsToAcquireBusinessesGross",
    ]
    _, ttm_acquisition, yearly_acquisition = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                      debug=False)

    measures = ["PaymentsToAcquireBusinessTwoNetOfCashAcquired"]
    _, ttm_acquisition2, yearly_acquisition2 = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                        debug=False)

    measures = ["PaymentsToAcquireInterestInSubsidiariesAndAffiliates"]
    _, ttm_sub_aff, yearly_sub_aff = get_values_from_measures(doc, measures, get_most_recent=False,
                                                              debug=False)

    measures = ["PaymentsToAcquireAdditionalInterestInSubsidiaries"]
    _, ttm_sub, yearly_sub = get_values_from_measures(doc, measures, get_most_recent=False,
                                                      debug=False)

    measures = ["PaymentsToAcquireBusinessesAndInterestInAffiliates"]
    _, ttm_aff, yearly_aff = get_values_from_measures(doc, measures, get_most_recent=False,
                                                      debug=False)

    merge_subsets_yearly(yearly_sub_aff, [yearly_sub, yearly_aff])
    merge_subsets_most_recent(ttm_sub_aff, [ttm_sub, ttm_aff])

    measures = ["PaymentsToAcquireInterestInJointVenture"]
    _, ttm_jv, yearly_jv = get_values_from_measures(doc, measures, get_most_recent=False,
                                                    debug=False)

    # PP&E
    measures = ["CapitalExpendituresIncurredButNotYetPaid"]
    _, ttm_capex_not_paid, yearly_capex_not_paid = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                            debug=False)

    measures = ["PaymentsForCapitalImprovements"]
    _, ttm_capex_imp, yearly_capex_imp = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                  debug=False)

    measures = ["PaymentsToAcquireOtherProductiveAssets"]
    _, ttm_productive_assets_other, yearly_productive_assets_other = get_values_from_measures(doc, measures,
                                                                                              get_most_recent=False,
                                                                                              debug=False)

    measures = ["PaymentsToAcquireOtherPropertyPlantAndEquipment"]
    _, ttm_ppe_other, yearly_ppe_other = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                  debug=False)

    measures = ["PaymentsToAcquireProductiveAssets"]
    _, ttm_productive_assets, yearly_productive_assets = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                                  debug=False)

    measures = ["PaymentsToAcquirePropertyPlantAndEquipment"]
    _, ttm_ppe, yearly_ppe = get_values_from_measures(doc, measures, get_most_recent=False,
                                                      debug=False)

    measures = ["PaymentsForSoftware"]
    _, ttm_sw, yearly_sw = get_values_from_measures(doc, measures, get_most_recent=False,
                                                    debug=False)

    measures = ["PaymentsToDevelopSoftware"]
    _, ttm_sw_dev, yearly_sw_dev = get_values_from_measures(doc, measures, get_most_recent=False,
                                                            debug=False)

    # Intangibles
    measures = ["PaymentsToAcquireIntangibleAssets"]
    _, ttm_intangibles, yearly_intangibles = get_values_from_measures(doc, measures, get_most_recent=False,
                                                                      debug=False)

    yearly_capex = {"dates": [], "values": []}
    merge_subsets_yearly(yearly_capex, [yearly_acquisition_costs, yearly_acquisition_adj, yearly_acquisition,
                                        yearly_acquisition2, yearly_sub_aff, yearly_jv, yearly_capex_not_paid,
                                        yearly_capex_imp, yearly_productive_assets_other, yearly_ppe_other,
                                        yearly_productive_assets,
                                        yearly_ppe, yearly_sw, yearly_sw_dev, yearly_intangibles])

    ttm_capex = {"date":None, "value":0}
    merge_subsets_most_recent(ttm_capex, [ttm_acquisition_costs, ttm_acquisition_adj, ttm_acquisition,
                                        ttm_acquisition2, ttm_sub_aff, ttm_jv, ttm_capex_not_paid,
                                        ttm_capex_imp, ttm_productive_assets_other, ttm_ppe_other,
                                        ttm_productive_assets,
                                        ttm_ppe, ttm_sw, ttm_sw_dev, ttm_intangibles])

    return {
        "ttm_dividends": ttm_dividends,
        "dividends": yearly_dividends,
        "ttm_capex": ttm_capex,
        "capex": yearly_capex
    }

def extract_company_financial_information(cik):

    """
    Extract financial data required for valuation from company financial document
    :param cik: company cik
    :return: dict with income statement and balance sheet metrics
    """

    doc = mongodb.get_document("financial_data", cik)
    shares, _ = extract_shares(doc)

    income_statement_measures = extract_income_statement(doc)
    last_annual_report_date = income_statement_measures["yearly_revenue"]["last_annual_report_date"]
    current_assets = extract_balance_sheet_current_assets(doc, last_annual_report_date)
    non_current_assets = extract_balance_sheet_noncurrent_assets(doc, last_annual_report_date)
    debt = extract_balance_sheet_debt(doc, last_annual_report_date)
    liabilities = extract_balance_sheet_liabilities(doc, last_annual_report_date, debt["mr_debt"])
    equity = extract_balance_sheet_equity(doc, last_annual_report_date)
    cashflow_statement_measures = extract_cashflow_statement(doc)

    return {
        **income_statement_measures,
        **current_assets,
        **non_current_assets,
        **debt,
        **liabilities,
        **equity,
        **cashflow_statement_measures
    }

def valuation(cik):
    data = extract_company_financial_information(cik)
    print(data)

if __name__ == '__main__':
    valuation(ATKR_CIK)
