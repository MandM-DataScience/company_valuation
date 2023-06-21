import time
from statistics import median

import requests
from forex_python.converter import CurrencyRates, RatesNotAvailableError
from urllib3.exceptions import ProtocolError
import numpy as np
from yahoo_finance import get_current_price_from_yahoo

r_and_d_amortization = {
    'Advertising': 2,
    'Aerospace/Defense': 10,
    'Air Transport': 10,
    'Apparel': 3,
    'Auto & Truck': 10,
    'Auto Parts': 5,
    'Bank (Money Center)': 2,
    'Banks (Regional)': 2,
    'Beverage (Alcoholic)': 3,
    'Beverage (Soft)': 3,
    "Broadcasting": 10,
    "Brokerage & Investment Banking": 3,
    'Building Materials': 5,
    'Construction Supplies': 5,
    "Business & Consumer Services": 5,
    'Cable TV': 10,
    'Chemical (Basic)': 10,
    'Chemical (Diversified)': 10,
    'Chemical (Specialty)': 10,
    'Coal & Related Energy': 5,
    'Computer & Peripherals': 5,
    'Computer Services': 3,
    'Diversified': 5,
    "Drugs (Biotechnology)": 10,
    "Drugs (Pharmaceutical)": 10,
    'Education': 3,
    'Electrical Equipment': 10,
    'Electronics (Consumer & Office)': 5,
    'Electronics (General)': 5,
    "Engineering/Construction": 10,
    'Entertainment': 3,
    'Environmental & Waste Services': 5,
    "Farming/Agriculture": 10,
    'Financial Svcs. (Non-bank & Insurance)': 2,
    'Food Processing': 3,
    'Food Wholesalers': 3,
    'Furn/Home Furnishings': 3,
    "Green & Renewable Energy": 10,
    "Healthcare Products": 5,
    "Healthcare Support Services": 3,
    'Heathcare Information and Technology': 3,
    'Homebuilding': 5,
    "Hospitals/Healthcare Facilities": 10,
    'Hotel/Gaming': 3,
    'Household Products': 3,
    "Information Services": 3,
    'Insurance (General)': 3,
    'Insurance (Life)': 3,
    'Insurance (Prop/Cas.)': 3,
    'Investments & Asset Management': 3,
    'Machinery': 10,
    'Metals & Mining': 5,
    'Office Equipment & Services': 5,
    "Oil/Gas (Integrated)": 10,
    "Oil/Gas (Production and Exploration)": 10,
    "Oil/Gas Distribution": 10,
    "Oilfield Svcs/Equip.": 5,
    'Packaging & Container': 5,
    'Paper/Forest Products': 10,
    "Power": 10,
    "Precious Metals": 5,
    'Petroleum (Integrated)': 5,
    'Petroleum (Producing)': 5,
    'Precision Instrument': 5,
    'Publishing & Newspapers': 3,
    'R.E.I.T.': 3,
    "Real Estate (Development)": 5,
    "Real Estate (General/Diversified)": 5,
    "Real Estate (Operations & Services)": 5,
    'Recreation': 5,
    'Reinsurance': 3,
    'Restaurant/Dining': 2,
    'Retail (Special Lines)': 2,
    'Retail (Building Supply)': 2,
    'Retail (General)': 2,
    "Retail (Automotive)": 2,
    "Retail (Distributors)": 2,
    "Retail (Grocery and Food)": 2,
    "Retail (Online)": 2,
    "Rubber& Tires": 5,
    'Semiconductor': 5,
    'Semiconductor Equip': 5,
    'Shipbuilding & Marine': 10,
    'Shoe': 3,
    "Software (Entertainment)": 3,
    "Software (Internet)": 3,
    "Software (System & Application)": 3,
    'Steel': 5,
    "Telecom (Wireless)": 5,
    'Telecom. Equipment': 10,
    'Telecom. Services': 5,
    'Tobacco': 5,
    'Toiletries/Cosmetics': 3,
    'Transportation': 5,
    'Transportation (Railroads)': 5,
    'Trucking': 5,
    'Utility (General)': 10,
    'Utility (Water)': 10

}

def convert_currencies(currency, financial_currency):

    if (financial_currency == "GBP" and ("GBp" in currency or "0.01" in currency)) \
            or (financial_currency == "ZAR" and ("ZAC" in currency or "ZAc" in currency or "0.01" in currency)) \
            or (financial_currency == "ILS" and ("ILA" in currency or "0.01" in currency)):
        fx_rate = 100

    else:

        c = CurrencyRates()

        rates = None
        num_retry = 0
        max_retry = 5
        while (rates is None and num_retry < max_retry):
            if num_retry > 0:
                print("# retry = " + str(num_retry) + ", get forex rates")

            if num_retry > 0:
                time.sleep(0.5 * num_retry)
            try:
                rates = c.get_rates(financial_currency)
            except requests.exceptions.ConnectionError:
                pass
            except ProtocolError:
                pass
            except RatesNotAvailableError:
                break

            num_retry += 1

        multiplier = 1
        if financial_currency == "USD" and ("GBp" in currency):
            currency = "GBP"
            multiplier = 100
        if financial_currency == "USD" and ("ZAc" in currency or "ZAC" in currency):
            currency = "ZAR"
            multiplier = 100
        if financial_currency == "USD" and ("ILA" in currency):
            currency = "ILS"
            multiplier = 100

        if rates is not None and currency in rates:
            fx_rate = rates[currency]
        else:
            if financial_currency == "USD":
                fx_rate = get_current_price_from_yahoo(f"{currency}=X")
            else:
                fx_rate = get_current_price_from_yahoo(f"{financial_currency}{currency}=X")
            # print("FX Yahoo", fx_rate)
            # raise Exception("mine")

        fx_rate *= multiplier

    # if debug:
    #     print("CONVERT ", financial_currency, "=>", currency, ": x", fx_rate)

    return fx_rate

def capitalize_rd(r_and_d, r_and_d_amortization_years, tax_rate, years):

    print("DEBUG R&D")
    print(r_and_d)
    print(r_and_d_amortization_years)
    print(tax_rate)
    print(years)

    # last element does not amortize this year
    r_and_d_amortization_cy = [sum(i * 1 / r_and_d_amortization_years for i in r_and_d[:-1])]

    # first element is fully amortized after this year
    r_and_d_unamortized = [sum(i[0] * i[1] for i in
                               zip(r_and_d[-r_and_d_amortization_years:],
                                   np.linspace(1 / r_and_d_amortization_years, 1, r_and_d_amortization_years)))]

    ebit_r_and_d_adj = [r_and_d[-1] - r_and_d_amortization_cy[0]]
    tax_benefit = [ebit_r_and_d_adj[0] * tax_rate]

    while len(r_and_d) < years:
        r_and_d.insert(0, 0)

    print("r_and_d after inserting 0", r_and_d)

    r_and_d_growth = []
    for i in range(len(r_and_d) - 1):
        try:
            r_and_d_growth.append(r_and_d[i + 1] / r_and_d[i] - 1)
        except:
            r_and_d_growth.append(0)

    # first element is the growth between most recent year and the year before that
    r_and_d_growth.reverse()

    # print(r_and_d)
    # print(r_and_d_growth)

    for i in range(years - 1):
        g = r_and_d_growth[i]
        if g == -1:
            tax_benefit.append(0)
            ebit_r_and_d_adj.append(0)
            r_and_d_unamortized.append(0)
            r_and_d_amortization_cy.append(0)
        else:
            tax_benefit.append(tax_benefit[-1] / (1 + g))
            ebit_r_and_d_adj.append(ebit_r_and_d_adj[-1] / (1 + g))
            r_and_d_unamortized.append(r_and_d_unamortized[-1] / (1 + g))
            r_and_d_amortization_cy.append(r_and_d_amortization_cy[-1] / (1 + g))

    # reverse order
    for l in [ebit_r_and_d_adj, tax_benefit, r_and_d_unamortized, r_and_d_amortization_cy]:
        l.reverse()

    print("r_and_d", r_and_d)
    print("r_and_d_amortization_years", r_and_d_amortization_years)
    print("r_and_d_growth", r_and_d_growth)
    print("r_and_d_amortization_cy", r_and_d_amortization_cy)
    print("r_and_d_unamortized", r_and_d_unamortized)
    print("ebit_r_and_d_adj", ebit_r_and_d_adj)
    print("tax_benefit", tax_benefit)

    return ebit_r_and_d_adj, tax_benefit, r_and_d_unamortized, r_and_d_amortization_cy

def get_spread_from_dscr(interest_coverage_ratio, damodaran_bond_spread):
    spread = damodaran_bond_spread[(interest_coverage_ratio >= damodaran_bond_spread["greater_than"]) &
                                   (interest_coverage_ratio < damodaran_bond_spread["less_than"])].iloc[0]
    return float(spread["spread"])

def debtize_op_leases(ttm_interest_expense, ttm_ebit_adj, damodaran_bond_spread, riskfree, country_default_spread,
                      leases, last_year_leases, tax_rate, revenue_growth):

    int_exp_op_adj = 0
    ttm_ebit_op_adj = 0
    debt_adj = [0]
    interest_coverage_ratio = 12.5
    company_default_spread = -1
    visited_icr = []
    done = False

    # CYCLE
    while not done:

        helper_interest_expense_adj = ttm_interest_expense + int_exp_op_adj
        helper_ebit_adj = ttm_ebit_adj + ttm_ebit_op_adj

        try:
            if helper_interest_expense_adj > 0:
                interest_coverage_ratio = helper_ebit_adj / helper_interest_expense_adj
        except:
            interest_coverage_ratio = 12.5

        spread = get_spread_from_dscr(interest_coverage_ratio, damodaran_bond_spread)

        if spread == company_default_spread:
            done = True

        else:

            if interest_coverage_ratio in visited_icr:
                done = True
            visited_icr.append(interest_coverage_ratio)

            company_default_spread = spread
            cost_of_debt = riskfree + country_default_spread + company_default_spread
            pv_leases = []
            for i in range(1, len(leases)):
                pv_leases.append(leases[i] / (1 + cost_of_debt) ** i)

            debt_adj = sum(pv_leases)
            if last_year_leases > 0:
                op_leases_depreciation = debt_adj / last_year_leases
            else:
                op_leases_depreciation = 0

            # update helper_ebit
            ttm_ebit_op_adj = leases[0] - op_leases_depreciation

            # update helper_interest
            int_exp_op_adj = leases[0] * (1 - 1 / (1 + cost_of_debt))

        print(interest_coverage_ratio, spread, company_default_spread, cost_of_debt)

    ebit_op_adj = [ttm_ebit_op_adj]
    tax_benefit_op = [ebit_op_adj[0] * tax_rate]
    debt_adj = [debt_adj]

    # first element is the growth between most year and the year before that
    revenue_growth.reverse()

    for g in revenue_growth:
        tax_benefit_op.append(tax_benefit_op[-1] / (1 + g))
        ebit_op_adj.append(ebit_op_adj[-1] / (1 + g))
        debt_adj.append(debt_adj[-1] / (1 + g))

    # restore revenue_growth
    revenue_growth.reverse()

    # reverse order
    for l in [tax_benefit_op, ebit_op_adj, debt_adj]:
        l.reverse()

    # print("leases", leases)
    # print("cost of debt", cost_of_debt)
    # print("pv_leases", pv_leases)
    # print("ttm_ebit_op_adj", ttm_ebit_op_adj)
    # print("tax_benefit_op", tax_benefit_op)
    # print("debt_adj", debt_adj)
    # print("years dep", last_year_leases)
    # print("depreciation", op_leases_depreciation)
    # print("helper_ebit_adj", helper_ebit_adj)
    # print("helper_interest_expense_adj", helper_interest_expense_adj)

    return ebit_op_adj, int_exp_op_adj, debt_adj, tax_benefit_op, company_default_spread

def get_growth_ttm(ttm_ebit_after_tax, ttm_net_income_adj, mr_equity_adj, mr_debt_adj, mr_cash_and_securities,
                 reinvestment, ttm_dividends, industry_payout):

    try:
        # print("ROC LAST")
        # print(ttm_ebit_after_tax)
        # print(mr_debt_adj)
        # print(mr_equity_adj)
        # print(mr_cash_and_securities)
        roc_last = ttm_ebit_after_tax / (mr_debt_adj + mr_equity_adj - mr_cash_and_securities)
    except:
        roc_last = 0

    try:
        reinvestment_last = reinvestment[-1] / ttm_ebit_after_tax
    except:
        reinvestment_last = 0

    if reinvestment_last < 0:
        reinvestment_last = 1 - industry_payout

    growth_last = roc_last * reinvestment_last

    try:
        roe_last = ttm_net_income_adj / mr_equity_adj
    except:
        roe_last = 0

    try:
        reinvestment_eps_last = 1 - ttm_dividends / ttm_net_income_adj
    except:
        reinvestment_eps_last = 0

    growth_eps_last = roe_last * reinvestment_eps_last

    return roc_last, reinvestment_last, growth_last, roe_last, reinvestment_eps_last, growth_eps_last

def get_roe_roc(equity_bv_adj, debt_bv_adj, cash_and_securities, ebit_after_tax, net_income_adj):
    roc = []
    roe = []
    avg_equity = sum(equity_bv_adj) / len(equity_bv_adj)
    for i in range(len(equity_bv_adj)):

        invested_capital = debt_bv_adj[i] + equity_bv_adj[i] - cash_and_securities[i]
        if invested_capital <= 0:
            roc.append(0)
        else:
            try:
                roc.append(ebit_after_tax[i] / invested_capital)
            except:
                roc.append(0)

        if equity_bv_adj[i] > 0:
            eq = equity_bv_adj[i]
        else:
            eq = avg_equity
        try:
            roe.append(net_income_adj[i] / eq)
        except:
            roe.append(0)
    return roe, roc

def get_target_info(revenue, ttm_revenue, country_default_spread, tax_rate, final_erp, riskfree,
                    unlevered_beta, damodaran_bond_spread, company_default_spread, target_debt_equity):

    cagr = None
    if ttm_revenue != revenue[-1]:
        ttm = True
    else:
        ttm = False

    if ttm:
        rev_list = revenue + [ttm_revenue]
    else:
        rev_list = revenue

    if rev_list[0] > 0:
        first_index = 0
        first_revenue = rev_list[0]
    elif rev_list[1] > 0:
        first_index = 1
        first_revenue = rev_list[1]
    elif rev_list[2] > 0:
        first_index = 2
        first_revenue = rev_list[2]
    else:
        cagr = 0
        print("error CAGR 0 - no revenue first 3 years")


    if cagr is None:

        years_diff = -1
        for i in rev_list:
            if i > 0:
                years_diff += 1

        # Simple CAGR
        simple_cagr = (rev_list[-1] / first_revenue) ** (1/(years_diff)) - 1
        capped_simple_cagr = max(min(simple_cagr,0.3),-0.2)

        # CAGR from start
        cagr_from_start_list = []
        for i in range(1, years_diff+1):
            cagr_from_start_list.append((rev_list[first_index+i] / first_revenue) ** (1/i) - 1)

        abs_cagr_from_start = [abs(x) for x in cagr_from_start_list]
        cagr_from_start_sorted = [x for _, x in sorted(zip(abs_cagr_from_start, cagr_from_start_list), reverse=True)]

        value_sum, weight_sum = (0,0)
        for idx, value in enumerate(cagr_from_start_sorted):
            weight = 2**idx
            weight_sum += weight
            value_sum += value * weight

        cagr_from_start = value_sum / weight_sum
        capped_cagr_from_start = max(min(cagr_from_start,0.3),-0.2)

        # CAGR from end
        cagr_from_end_list = []
        for i in range(years_diff):
            cagr_from_end_list.append((rev_list[-1] / rev_list[first_index+i]) ** (1 / (years_diff-i)) - 1)

        abs_cagr_from_end = [abs(x) for x in cagr_from_end_list]
        cagr_from_end_sorted = [x for _, x in sorted(zip(abs_cagr_from_end, cagr_from_end_list), reverse=True)]

        value_sum, weight_sum = (0, 0)
        for idx, value in enumerate(cagr_from_end_sorted):
            weight = 2 ** idx
            weight_sum += weight
            value_sum += value * weight

        cagr_from_end = value_sum / weight_sum
        capped_cagr_from_end = max(min(cagr_from_end, 0.3), -0.2)

        print("rev_list", rev_list)
        print("first_revenue", first_revenue)
        print("simple_cagr", simple_cagr)
        print("capped_simple_cagr", capped_simple_cagr)
        print("cagr_from_start_list", cagr_from_start_list)
        print("cagr_from_start", cagr_from_start)
        print("capped_cagr_from_start", capped_cagr_from_start)
        print("cagr_from_end_list", cagr_from_end_list)
        print("cagr_from_end", cagr_from_end)
        print("capped_cagr_from_end", capped_cagr_from_end)

        cagr_3_values = [capped_simple_cagr, capped_cagr_from_start, capped_cagr_from_end]
        cagr_3_values.sort(reverse=True)

        value_sum, weight_sum = (0, 0)
        for idx, value in enumerate(cagr_3_values):
            weight = 2 ** idx
            weight_sum += weight
            value_sum += value * weight

        cagr = value_sum / weight_sum

    spread_list = list(damodaran_bond_spread["spread"].unique())
    spread_list = [float(x) for x in spread_list]
    spread_list.sort()

    debt_improvement_offset = 2
    idx = spread_list.index(company_default_spread)
    idx -= debt_improvement_offset
    if idx < 0:
        idx = 0
    target_company_default_spread = float(spread_list[idx])

    target_levered_beta = unlevered_beta * (1+ (1-tax_rate) * target_debt_equity)
    target_cost_of_equity = riskfree + final_erp * target_levered_beta
    target_cost_of_debt = riskfree + country_default_spread + target_company_default_spread
    target_cost_of_capital = target_cost_of_debt * (1-tax_rate) * target_debt_equity / (target_debt_equity + 1) + \
                             target_cost_of_equity * 1 / (1 + target_debt_equity)

    return cagr, target_levered_beta, target_cost_of_equity, target_cost_of_debt, target_cost_of_capital

def get_normalized_info(revenue, ebit_adj, revenue_delta, reinvestment, target_sales_capital,
                        ebit_after_tax, industry_payout, cagr, net_income_adj, roe, dividends, eps_adj, roc):

    weights = [2**x for x in range(len(revenue))]
    sum_weights = sum(weights)
    revenue_5y = sum(i[0] * i[1] for i in zip(revenue, weights)) / sum_weights
    ebit_5y = sum(i[0] * i[1] for i in zip(ebit_adj, weights)) / sum_weights

    try:
        operating_margin_5y = ebit_5y / revenue_5y
    except:
        operating_margin_5y = 0

    # print("## SALES CAPITAL ##")
    # print(revenue_delta)
    # print(sum(revenue_delta))
    # print(reinvestment)
    # print(sum(reinvestment))
    # print(sum(revenue_delta) / sum(reinvestment))
    # print("target", target_sales_capital)

    try:
        sales_capital_5y = sum(revenue_delta) / sum(reinvestment)
        if sales_capital_5y <= 0:
            sales_capital_5y = target_sales_capital
    except:
        sales_capital_5y = target_sales_capital

    roc_5y = sum(i[0] * i[1] for i in zip(roc, weights)) / sum_weights

    try:
        reinvestment_5y = sum(reinvestment) / sum(ebit_after_tax)
        if reinvestment_5y <= 0:
            reinvestment_5y = 1 - industry_payout
    except:
        reinvestment_5y = 1 - industry_payout

    try:
        growth_5y = roc_5y * reinvestment_5y
    except:
        growth_5y = cagr

    net_income_5y = sum(i[0] * i[1] for i in zip(net_income_adj, weights)) / sum_weights
    roe_5y = sum(i[0] * i[1] for i in zip(roe, weights)) / sum_weights

    try:
        reinvestment_eps_5y = 1 - sum(dividends) / sum(eps_adj)
    except:
        reinvestment_eps_5y = reinvestment_5y
    growth_eps_5y = roe_5y * reinvestment_eps_5y

    return revenue_5y, ebit_5y, operating_margin_5y, sales_capital_5y, roc_5y, reinvestment_5y, growth_5y, \
           net_income_5y, roe_5y, reinvestment_eps_5y, growth_eps_5y

def get_dividends_info(eps_adj, dividends):

    weights = [2 ** x for x in range(len(eps_adj))]
    sum_weights = sum(weights)

    eps_5y = sum(i[0] * i[1] for i in zip(eps_adj, weights)) / sum_weights
    try:
        payout_5y = sum(dividends) / sum(eps_adj)
    except:
        payout_5y = 0
    if payout_5y < 0:
        payout_5y = 0
    return eps_5y, payout_5y

def get_final_info(riskfree, cost_of_debt, equity_mkt, debt_mkt, unlevered_beta,
                   tax_rate, final_erp, company_default_spread):

    survival_prob = (1 - company_default_spread) ** 10

    try:
        debt_equity = debt_mkt / equity_mkt
    except:
        debt_equity = 0.5

    levered_beta = unlevered_beta * (1 + (1 - tax_rate) * debt_equity)

    cost_of_equity = riskfree + levered_beta * final_erp
    try:
        equity_weight = equity_mkt / (equity_mkt + debt_mkt)
    except:
        equity_weight = 0.5
    debt_weight = 1 - equity_weight
    cost_of_capital = cost_of_equity * equity_weight + cost_of_debt * (1 - tax_rate) * debt_weight

    return survival_prob, debt_equity, \
           levered_beta, cost_of_equity, equity_weight, debt_weight, cost_of_capital

def calculate_liquidation_value(cash, receivables, inventory, securities, other_current_assets, mr_property, ppe,
                                equity_investments, total_liabilities, equity_mkt, mr_debt, mr_equity, mr_original_min_interest,
                                minority_interest, debug=True):

    percent_minority_interest = minority_interest / equity_mkt
    # market_liquidation = equity_mkt + debt_mkt - mr_debt
    # if market_liquidation < 0:
    #     market_liquidation = 0

    damodaran_liquidation = cash + securities + mr_property + (other_current_assets + inventory + receivables + ppe) * 0.75 + \
                            equity_investments * 0.5 - total_liabilities
    if damodaran_liquidation < 0:
        damodaran_liquidation = 0
    net_net_wc_liquidation = cash + receivables + inventory + securities + mr_property + other_current_assets - total_liabilities
    if net_net_wc_liquidation < 0:
        net_net_wc_liquidation = 0

    _sorted = sorted([damodaran_liquidation, net_net_wc_liquidation], reverse=True)

    value_sum, weight_sum = (0, 0)
    for idx, value in enumerate(_sorted):
        weight = 2 ** idx
        weight_sum += weight
        value_sum += value * weight

    liquidation_value = value_sum / weight_sum * (1-percent_minority_interest)

    if debug:
        print("===== Liquidation Value =====\n")
        print("cash", cash)
        print("securities", securities)
        print("receivables", receivables)
        print("inventory", inventory)
        print("other_current_assets_ms", other_current_assets)
        print("property", mr_property)
        print("ppe", ppe)
        print("equity_investments", equity_investments)
        print()
        print("total_liabilities", total_liabilities)
        # print("percent_minority_interest", percent_minority_interest)
        # print("equity_mkt", equity_mkt)
        # print("debt_mkt", debt_mkt)
        print("debt_bv", mr_debt)
        print("equity_bv", mr_equity)
        print("minority_interest", mr_original_min_interest, "=>", minority_interest)
        # print("market_liquidation", market_liquidation)
        print("damodaran_liquidation", damodaran_liquidation)
        print("net_net_wc_liquidation", net_net_wc_liquidation)
        print("liquidation_value", liquidation_value)
        print("\n\n")

    return liquidation_value

EARNINGS_TTM = "EARNINGS_TTM"
EARNINGS_NORM = "EARNINGS_NORM"
GROWTH_FIXED = "GROWTH_FIXED"
GROWTH_TTM = "GROWTH_TTM"
GROWTH_NORM = "GROWTH_NORM"

def dividends_valuation(earnings_type, growth_type, cagr, growth_eps_5y, growth_5y, riskfree,
                        industry_payout, cost_of_equity, target_cost_of_equity,
                        growth_eps_last, eps_5y, payout_5y, ttm_eps_adj, reinvestment_eps_last, fx_rate, debug=True, recession=False):

    final_growth = riskfree

    if growth_5y != 0:
        final_growth = riskfree * growth_eps_5y / growth_5y
        if final_growth <= 0:
            final_growth = riskfree
        else:
            final_growth = riskfree * max(min(growth_eps_5y / growth_5y, 2), 0.5)

    if growth_type == GROWTH_FIXED:
        if growth_5y == 0:
            initial_growth = cagr
        else:
            initial_growth = cagr * growth_eps_5y / growth_5y

            if initial_growth <= 0:
                initial_growth = cagr
            else:
                initial_growth = cagr * max(min(growth_eps_5y / growth_5y, 2),0.5)

    elif growth_type == GROWTH_TTM:
        initial_growth = growth_eps_last
    else:
        initial_growth = growth_eps_5y

    growth_history = np.linspace(initial_growth, final_growth, 11)

    if recession:
        growth_history[3:6] = 0

    if earnings_type == EARNINGS_TTM:
        initial_eps = ttm_eps_adj
    else:
        initial_eps = eps_5y

    eps_history = []

    for i in range(len(growth_history)):
        if initial_eps < 0 and  i < 6:
            if i == 0:
                eps_history.append(initial_eps + abs(initial_eps) / 5)
            else:
                eps_history.append(eps_history[i-1] + abs(initial_eps) / 5)
        else:
            if i == 0:
                eps_history.append(initial_eps * (1+growth_history[i]))
            else:
                eps_history.append(eps_history[i-1] * (1+growth_history[i]))

    if earnings_type == EARNINGS_TTM:
        try:
            initial_payout = 1 - reinvestment_eps_last
        except:
            initial_payout = payout_5y
    else:
        initial_payout = payout_5y

    final_payout = industry_payout
    payout_history = np.linspace(initial_payout, final_payout, 12)
    payout_history = payout_history[1:]

    if recession:
        payout_history[3:6] = 0

    dps_history = []
    for i in range(len(eps_history)):
        dps_history.append(eps_history[i] * payout_history[i])

    initial_coe = cost_of_equity
    final_coe = target_cost_of_equity
    cost_of_equity_history = np.linspace(initial_coe, final_coe, 12)
    cost_of_equity_history = cost_of_equity_history[1:]

    cumulative_coe = []
    for i in range(len(cost_of_equity_history)):
        if i == 0:
            cumulative_coe.append(1+cost_of_equity_history[i])
        else:
            cumulative_coe.append(cumulative_coe[i-1] * (1+cost_of_equity_history[i]))

    present_value = []
    for i in range(len(cumulative_coe)-1):
        present_value.append(dps_history[i] / cumulative_coe[i])

    terminal_value = eps_history[-1] * payout_history[-1] / \
                     (cost_of_equity_history[-1] - growth_history[-1])
    terminal_pv = terminal_value / cumulative_coe[-1]

    stock_value_price_curr = sum(present_value) + terminal_pv

    if fx_rate is not None:
        stock_value = stock_value_price_curr * fx_rate
    else:
        stock_value = stock_value_price_curr

    if debug:

        for i in [growth_history, eps_history, payout_history, dps_history, cost_of_equity_history, cumulative_coe, present_value]:
            for idx, j in enumerate(i):
                i[idx] = round(j,4)

        print(f"===== Dividends Valuation - {earnings_type} + {growth_type} + recession:{recession} =====\n")
        print("expected_growth", growth_history)
        print("earnings_per_share", eps_history)
        print("payout_ratio", payout_history)
        print("dividends_per_share", dps_history)
        print("cost_of_equity", cost_of_equity_history)
        print("cumulative_cost_equity", cumulative_coe)
        print("present_value",present_value)
        print("terminal_value",round(terminal_value,2))
        print("PV of terminal_value", round(terminal_pv,2))
        print("stock value (price curr)", round(stock_value_price_curr,2))
        print("stock value (fin curr)", round(stock_value, 2))
        print("\n\n")

    return stock_value

def fcff_valuation(earnings_type, growth_type, cagr, riskfree, ttm_revenue, ttm_ebit_adj, target_operating_margin, tax_benefits,
                   tax_rate, sales_capital_5y, target_sales_capital, debt_equity, target_debt_equity, unlevered_beta,
                   final_erp, cost_of_debt, target_cost_of_debt, mr_cash, mr_securities, debt_mkt, minority_interest, survival_prob,
                   share_issued, ko_proceeds, growth_last, growth_5y, revenue_5y, ebit_5y, fx_rate, mr_property, mr_sbc, debug=True, recession=False):

    # earnings ttm + growth fixed

    if growth_type == GROWTH_FIXED:
        initial_growth = cagr
    elif growth_type == GROWTH_TTM:
        initial_growth = growth_last
    else:
        initial_growth = growth_5y
    final_growth = riskfree

    growth_history = np.linspace(initial_growth, final_growth, 11)

    if recession:
        growth_history[3] = -0.1
        growth_history[4] = -0.2
        growth_history[5] = 0.4

    if earnings_type == EARNINGS_TTM:
        initial_revenue = ttm_revenue
    else:
        initial_revenue = revenue_5y

    revenue_history = []
    for i in range(len(growth_history)):
        if i == 0:
            revenue_history.append(initial_revenue * (1+growth_history[i]))
        else:
            revenue_history.append(revenue_history[i-1] * (1+growth_history[i]))

    if earnings_type == EARNINGS_TTM:
        if ttm_revenue == 0:
            initial_margin = 0
        else:
            initial_margin = ttm_ebit_adj / ttm_revenue
    else:
        if revenue_5y == 0:
            initial_margin = 0
        else:
            initial_margin = ebit_5y / revenue_5y

    final_margin = target_operating_margin
    margin_history = np.linspace(initial_margin, final_margin, 12)[1:]

    if initial_margin < 0:
        check_margin = initial_margin / 5

        if check_margin * 4 > margin_history[0]:

            for i in range(len(margin_history)):
                if i < 5:
                    margin_history[i] = initial_margin - check_margin * (i+1)
                else:
                    margin_history[i] = final_margin / 6 * (i-4)

    if recession:
        margin_history[3] *= 0.5
        margin_history[4] *= 0.25
        margin_history[5] *= 0.5

    ebit_history = []
    for i in range(len(revenue_history)):
        ebit_history.append(revenue_history[i] * margin_history[i])

    residual_tax_benefits = tax_benefits
    tax_history = []
    ebit_after_tax_history = []
    for i in range(len(ebit_history)):
        e = ebit_history[i]
        if e < 0:
            tax_history.append(0)
        else:
            tax_history.append(max(0, (e * tax_rate) - residual_tax_benefits))
            residual_tax_benefits = max(0, residual_tax_benefits - (e * tax_rate))

        ebit_after_tax_history.append(ebit_history[i] - tax_history[i])

    initial_sales_capital = sales_capital_5y
    final_sales_capital = target_sales_capital
    sales_capital_history = np.linspace(initial_sales_capital, final_sales_capital, 12)[1:]

    reinvestment_history = []
    fcff_history = []
    for i in range(len(revenue_history)):
        if i == 0:
            delta_revenue = revenue_history[i] - initial_revenue
        else:
            delta_revenue = revenue_history[i] - revenue_history[i-1]
        reinvestment_history.append(delta_revenue / sales_capital_history[i])

        if recession and i in [3,4]:
            reinvestment_history[i] = reinvestment_history[i-1]

        fcff_history.append(ebit_after_tax_history[i] - reinvestment_history[i])

    # initial_debt_ratio = debt_weight
    # final_debt_ratio = target_debt_equity / (1+target_debt_equity)
    # debt_ratio_history = np.linspace(initial_debt_ratio, final_debt_ratio, 12)[1:]

    debt_equity_history = np.linspace(debt_equity, target_debt_equity, 12)[1:]

    initial_cost_of_debt = cost_of_debt
    final_cost_of_debt = target_cost_of_debt
    cost_of_debt_history = np.linspace(initial_cost_of_debt, final_cost_of_debt, 12)[1:]

    debt_ratio_history = []
    beta_history = []
    cost_of_equity_history = []
    cost_of_capital_history = []
    cumulative_wacc_history = []
    present_value_history = []
    for i in range(len(debt_equity_history)):
        debt_ratio_history.append(debt_equity_history[i] / (debt_equity_history[i] + 1))
        # debt_equity_history.append(debt_ratio_history[i] / (1-debt_ratio_history[i]))
        beta_history.append(unlevered_beta * (1+(1-tax_rate)*debt_equity_history[i]))
        cost_of_equity_history.append(riskfree + final_erp * beta_history[i])
        cost_of_capital_history.append(cost_of_equity_history[i] * (1-debt_ratio_history[i])
                                       + cost_of_debt_history[i] * (1-tax_rate) * debt_ratio_history[i])

        if i == 0:
            cumulative_wacc_history.append(1 + cost_of_capital_history[i])
        else:
            cumulative_wacc_history.append(cumulative_wacc_history[i-1] * (1 + cost_of_capital_history[i]))

        present_value_history.append(fcff_history[i] / cumulative_wacc_history[i])

    terminal_value = fcff_history[-1] / (cost_of_capital_history[-1] - growth_history[-1])
    terminal_value_pv = terminal_value / cumulative_wacc_history[-1]

    firm_value = sum(present_value_history[:-1]) + terminal_value_pv + mr_cash + mr_securities + mr_property

    equity_value = firm_value - debt_mkt - minority_interest - mr_sbc

    try:
        stock_value_price_curr = (equity_value * survival_prob + ko_proceeds * (1-survival_prob)) / share_issued
    except:
        stock_value_price_curr = 0

    if fx_rate is not None:
        stock_value = stock_value_price_curr * fx_rate
    else:
        stock_value = stock_value_price_curr

    if debug:

        # for i in [growth_history, revenue_history, margin_history, ebit_history, tax_history, ebit_after_tax_history,
        #           sales_capital_history, reinvestment_history, fcff_history, debt_ratio_history, cost_of_debt_history,
        #           cost_of_equity_history, cost_of_capital_history, cumulative_wacc_history, present_value_history]:
        #     for idx, j in enumerate(i):
        #         i[idx] = round(j,4)

        print(f"===== FCFF Valuation - {earnings_type} + {growth_type} recession:{recession} =====\n")
        print("expected_growth", growth_history)
        print("revenue", revenue_history)
        print("margin", margin_history)
        print("ebit", ebit_history)
        print("tax_history", tax_history)
        print("ebit_after_tax", ebit_after_tax_history)
        print("sales_capital", sales_capital_history)
        print("reinvestment", reinvestment_history)
        print("FCFF", fcff_history)
        print("debt ratio", debt_ratio_history)
        print("debt2equity", debt_equity_history)
        print("beta", beta_history)
        print("cost_of_equity", cost_of_equity_history)
        print("cost_of_debt", cost_of_debt_history)
        print("cost_of_capital", cost_of_capital_history)
        print("cumulative WACC", cumulative_wacc_history)
        print("present value", present_value_history)
        print("terminal value", round(terminal_value,2))
        print("PV of FCFF during growth", sum(present_value_history[:-1]))
        print("PV of terminal value", round(terminal_value_pv,2))
        print("Value of operating assets", sum(present_value_history[:-1])+terminal_value_pv)
        print("Value of cash and property", mr_cash + mr_securities + mr_property)
        print("firm value", round(firm_value,2))
        print("debt outstanding", round(debt_mkt,2))
        print("equity value", round(equity_value,2))
        print("stock value (price curr)", round(stock_value_price_curr,2))
        print("stock value (fin curr)", round(stock_value, 2))
        print("\n\n")

    return stock_value

def summary_valuation(valuations):

    sorted = valuations.copy()
    sorted.sort()

    count_negative = 0
    for val in sorted:
        if val < 0:
            count_negative += 1

    if count_negative > 1:
        result = 0
    elif count_negative > 0:
        result = sorted[1]
    else:
        second_highest = sorted[2]
        third_highest = sorted[1]
        if second_highest / third_highest > 10:
            result = 0
        else:
            max_val = max(sorted)
            min_val = min(sorted)
            if min_val == 0 or max_val / min_val > 3:
                result = sorted[1]
            else:
                result = median(sorted)

    return result

def get_status(fcff_delta, div_delta, liquidation_delta, country, region, size, debug):

    STATUS_OK = "OK"
    STATUS_NI = "NI"
    STATUS_KO = "KO"

    t = 0.2
    if liquidation_delta < 0:
        t = 0

    if country == "United States":
        t += 0
    elif region in ["US","EU","Japan","Rest"]:
        t += 0.1
    else:
        t += 0.2

    if size == "Mega":
        t += 0.05
    elif size == "Large":
        t += 0.08
    elif size == "Medium":
        t += 0.11
    elif size == "Small":
        t += 0.14
    elif size == "Micro":
        t += 0.17
    else:
        t += 0.2

    if fcff_delta < -t:
        if div_delta < -t:
            status = STATUS_OK
        else:
            status = STATUS_NI
    elif fcff_delta < 0:
        if div_delta < 0:
            status = STATUS_NI
        else:
            status = STATUS_KO
    else:
        if div_delta < -t:
            status = STATUS_NI
        else:
            status = STATUS_KO

    if debug:
        print("Base 20%: ", liquidation_delta>0, " - liquidation delta", liquidation_delta)
        print("Country:", country, "Region", region)
        print("Company Size:", size)
        print("THRESHOLD = ", t)
    return status