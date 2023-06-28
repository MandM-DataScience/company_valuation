import re
import sys
import time
from datetime import datetime
from statistics import median

import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from forex_python.converter import CurrencyRates, RatesNotAvailableError
from unidecode import unidecode
from urllib3.exceptions import ProtocolError
import numpy as np

import mongodb
from investing_com import get_10y_bond_yield
from postgresql import get_df_from_table
from yahoo_finance import get_current_price_from_yahoo
import math
import pandas as pd

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
industry_complexity = {
    'Advertising': 1,
    'Apparel': 1,
    'Auto & Truck': 1,
    'Auto Parts': 1,
    'Beverage (Alcoholic)': 1,
    'Beverage (Soft)': 1,
    'Building Materials': 1,
    'Construction Supplies': 1,
    'Food Wholesalers': 1,
    'Furn/Home Furnishings': 1,
    'Household Products': 1,
    'Retail (Building Supply)': 1,
    'Retail (General)': 1,
    "Retail (Automotive)": 1,
    "Retail (Distributors)": 1,
    "Retail (Grocery and Food)": 1,
    'Tobacco': 1,
    'Toiletries/Cosmetics': 1,
    'Restaurant/Dining': 1,
    "Rubber& Tires": 1,
    'Shoe': 1,

    "Business & Consumer Services": 2,
    'Cable TV': 2,
    'Air Transport': 2,
    'Coal & Related Energy': 2,
    'Computer & Peripherals': 2,
    'Computers/Peripherals': 2,
    'Education': 2,
    'Electrical Equipment': 2,
    'Entertainment': 2,
    'Homebuilding': 2,
    "Hospitals/Healthcare Facilities": 2,
    'Hotel/Gaming': 2,
    'Food Processing': 2,
    'Office Equipment & Services': 2,
    'Packaging & Container': 2,
    'Paper/Forest Products': 2,

    "Real Estate (Development)": 2,
    "Real Estate (General/Diversified)": 2,
    "Real Estate (Operations & Services)": 2,
    'Recreation': 2,
    'Retail (Special Lines)': 2,
    "Retail (Online)": 2,
    'Publishing & Newspapers': 2,
    'Steel': 2,
    'Electronics (Consumer & Office)': 2,
    'Electronics (General)': 2,
    'R.E.I.T.': 2,
    'Transportation': 2,
    'Transportation (Railroads)': 2,
    'Trucking': 2,

    "Broadcasting": 3,
    'Metals & Mining': 3,
    "Precious Metals": 3,
    'Aerospace/Defense': 3,
    'Chemical (Basic)': 3,
    'Chemical (Diversified)': 3,
    'Computer Services': 3,
    "Engineering/Construction": 3,
    "Farming/Agriculture": 3,
    'Heathcare Information and Technology': 3,
    "Information Services": 3,
    'Insurance (General)': 3,
    'Insurance (Life)': 3,
    'Insurance (Prop/Cas.)': 3,
    'Investments & Asset Management': 3,
    'Machinery': 3,
    'Utility (General)': 3,
    'Utility (Water)': 3,
    "Software (Entertainment)": 3,
    "Software (Internet)": 3,
    "Software (System & Application)": 3,
    'Reinsurance': 3,
    'Semiconductor': 3,
    "Power": 3,
    'Telecom. Services': 3,
    'Shipbuilding & Marine': 3,
    "Telecom (Wireless)": 3,
    'Telecom. Equipment': 3,
    "Healthcare Products": 3,
    "Healthcare Support Services": 3,


    'Bank (Money Center)': 4,
    'Banks (Regional)': 4,
    "Brokerage & Investment Banking": 4,
    'Financial Svcs. (Non-bank & Insurance)': 4,
    'Environmental & Waste Services': 4,
    "Green & Renewable Energy": 4,
    "Oil/Gas (Integrated)": 4,
    "Oil/Gas (Production and Exploration)": 4,
    "Oil/Gas Distribution": 4,
    "Oilfield Svcs/Equip.": 4,
    'Petroleum (Integrated)': 4,
    'Petroleum (Producing)': 4,
    'Precision Instrument': 4,
    'Semiconductor Equip': 4,


    'Chemical (Specialty)': 5,
    'Diversified': 5,
    "Drugs (Biotechnology)": 5,
    "Drugs (Pharmaceutical)": 5,

}
industry_cyclical = {
    'Advertising': True,
    'Apparel': False,
    'Auto & Truck': True,
    'Auto Parts': True,
    'Beverage (Alcoholic)': False,
    'Beverage (Soft)': False,
    'Building Materials': True,
    'Construction Supplies': True,
    'Food Wholesalers': False,
    'Furn/Home Furnishings': True,
    'Household Products': False,
    'Retail (Building Supply)': True,
    'Retail (General)': False,
    "Retail (Automotive)": True,
    "Retail (Distributors)": True,
    "Retail (Grocery and Food)": False,
    'Tobacco': False,
    'Toiletries/Cosmetics': False,
    'Restaurant/Dining': True,
    "Rubber& Tires": True,
    'Shoe': False,

    "Business & Consumer Services": True,
    'Cable TV': False,
    'Air Transport': True,
    'Coal & Related Energy': True,
    'Computer & Peripherals': True,
    'Computers/Peripherals': True,
    'Education': False,
    'Electrical Equipment': True,
    'Entertainment': True,
    'Homebuilding': True,
    "Hospitals/Healthcare Facilities": False,
    'Hotel/Gaming': True,
    'Food Processing': False,
    'Office Equipment & Services': True,
    'Packaging & Container': True,
    'Paper/Forest Products': True,

    "Real Estate (Development)": True,
    "Real Estate (General/Diversified)": True,
    "Real Estate (Operations & Services)": True,
    'Recreation': True,
    'Retail (Special Lines)': True,
    "Retail (Online)": True,
    'Publishing & Newspapers': False,
    'Steel': True,
    'Electronics (Consumer & Office)': True,
    'Electronics (General)': True,
    'R.E.I.T.': True,
    'Transportation': True,
    'Transportation (Railroads)': True,
    'Trucking': True,

    "Broadcasting": False,
    'Metals & Mining': True,
    "Precious Metals": True,
    'Aerospace/Defense': False,
    'Chemical (Basic)': True,
    'Chemical (Diversified)': True,
    'Computer Services': True,
    "Engineering/Construction": True,
    "Farming/Agriculture": False,
    'Heathcare Information and Technology': False,
    "Information Services": True,
    'Insurance (General)': False,
    'Insurance (Life)': False,
    'Insurance (Prop/Cas.)': False,
    'Investments & Asset Management': True,
    'Machinery': True,
    'Utility (General)': False,
    'Utility (Water)': False,
    "Software (Entertainment)": True,
    "Software (Internet)": True,
    "Software (System & Application)": True,
    'Reinsurance': True,
    'Semiconductor': True,
    "Power": False,
    'Telecom. Services': False,

    'Bank (Money Center)': False,
    'Banks (Regional)': False,
    "Brokerage & Investment Banking": True,
    'Financial Svcs. (Non-bank & Insurance)': True,
    'Environmental & Waste Services': False,
    "Green & Renewable Energy": False,
    "Healthcare Products": False,
    "Healthcare Support Services": False,
    "Oil/Gas (Integrated)": True,
    "Oil/Gas (Production and Exploration)": True,
    "Oil/Gas Distribution": True,
    "Oilfield Svcs/Equip.": True,
    'Petroleum (Integrated)': True,
    'Petroleum (Producing)': True,
    'Precision Instrument': True,
    'Semiconductor Equip': True,
    'Shipbuilding & Marine': True,
    "Telecom (Wireless)": False,
    'Telecom. Equipment': False,

    'Chemical (Specialty)': False,
    'Diversified': False,
    "Drugs (Biotechnology)": False,
    "Drugs (Pharmaceutical)": False,

}

def company_complexity(doc, industry, company_size):
    base = industry_complexity[industry]

    # no 10-K
    if doc is None:
        length_modifier = 1

    else:
        soup = BeautifulSoup(doc["html"], 'html.parser')
        body_text = unidecode(soup.body.get_text(separator=" "))
        body_text = re.sub('\n', ' ', body_text)
        body_text = re.sub(' +', ' ', body_text)
        length = len(body_text)

        if length > 1.4 * 10 ** 6:
            length_modifier = 4
        elif length > 1.1 * 10 ** 6:
            length_modifier = 3
        elif length > 8 * 10 ** 5:
            length_modifier = 2
        elif length > 5 * 10 ** 5:
            length_modifier = 1
        else:
            length_modifier = 0

    if company_size == "Large" or company_size == "Mega":
        company_size_modifier = 1
    else:
        company_size_modifier = 0

    return min(5, max(1, base + length_modifier + company_size_modifier))

def company_share_diluition(shares):

    first_idx = 0
    for i, s in enumerate(shares):
        if s > 0:
            first_idx = i
            break

    l = shares[first_idx:]
    years = len(l) - 1
    return (l[-1] / l[0]) ** (1 / years) - 1

def get_company_type(revenue_growth, mr_debt_adj, equity_mkt, liquidation_value, operating_margin_5y, industry):

    fast_grower = False
    stalward = False
    slow_grower = False
    declining = False
    turn_around = False
    asset_play = False
    cyclical = industry_cyclical[industry]

    avg_revenue_growth = np.mean(revenue_growth)
    if avg_revenue_growth > 0.15:
        fast_grower = True
    elif avg_revenue_growth > 0.07:
        stalward = True
    elif avg_revenue_growth > -0.02:
        slow_grower = True
    else:
        declining = True

    # high debt + liquidation > debt + low margins
    if mr_debt_adj > equity_mkt * 2 and liquidation_value > mr_debt_adj and operating_margin_5y < 0.05:
        turn_around = True

    if liquidation_value > equity_mkt:
        asset_play = True

    return {
        "fast_grower": fast_grower,
        "stalward": stalward,
        "slow_grower": slow_grower,
        "declining": declining,
        "turn_around": turn_around,
        "asset_play": asset_play,
        "cyclical": cyclical
    }

def convert_currencies(db_curr, db_financial_curr, currency=None, financial_currency=None):

    if (currency is not None and financial_currency is not None):
        financial_currency = financial_currency.replace("Currency in ", "")

    else:
        currency = db_curr
        financial_currency = db_financial_curr

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
        if "GBp" in currency:
            currency = "GBP"
            multiplier = 100
        if "ZAc" in currency or "ZAC" in currency:
            currency = "ZAR"
            multiplier = 100
        if "ILA" in currency:
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

    # print("DEBUG R&D")
    # print(r_and_d)
    # print(r_and_d_amortization_years)
    # print(tax_rate)
    # print(years)

    # last element does not amortize this year

    r_and_d = r_and_d[-r_and_d_amortization_years-1:]
    while len(r_and_d) < years:
        r_and_d.insert(0, 0)

    r_and_d_amortization_cy = [sum(i * 1 / r_and_d_amortization_years for i in r_and_d[:-1])]

    # first element is fully amortized after this year
    r_and_d_unamortized = [sum(i[0] * i[1] for i in
                               zip(r_and_d[-r_and_d_amortization_years:],
                                   np.linspace(1 / r_and_d_amortization_years, 1, r_and_d_amortization_years)))]

    ebit_r_and_d_adj = [r_and_d[-1] - r_and_d_amortization_cy[0]]
    tax_benefit = [ebit_r_and_d_adj[0] * tax_rate]

    # print("r_and_d after inserting 0", r_and_d)

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

    for g in r_and_d_growth:
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

    # print("r_and_d", r_and_d)
    # print("r_and_d_amortization_years", r_and_d_amortization_years)
    # print("r_and_d_growth", r_and_d_growth)
    # print("r_and_d_amortization_cy", r_and_d_amortization_cy)
    # print("r_and_d_unamortized", r_and_d_unamortized)
    # print("ebit_r_and_d_adj", ebit_r_and_d_adj)
    # print("tax_benefit", tax_benefit)

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
                interest_coverage_ratio = min(99999, helper_ebit_adj / helper_interest_expense_adj)
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

        # print(interest_coverage_ratio, spread, company_default_spread, cost_of_debt)

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

    if (mr_debt_adj + mr_equity_adj - mr_cash_and_securities) > 0:
        # print("ROC LAST")
        # print(ttm_ebit_after_tax)
        # print(mr_debt_adj)
        # print(mr_equity_adj)
        # print(mr_cash_and_securities)
        roc_last = ttm_ebit_after_tax / (mr_debt_adj + mr_equity_adj - mr_cash_and_securities)
    else:
        roc_last = 0

    if ttm_ebit_after_tax > 0:
        reinvestment_last = reinvestment[-1] / ttm_ebit_after_tax
    else:
        reinvestment_last = 0

    if reinvestment_last < 0:
        reinvestment_last = 1 - industry_payout

    growth_last = roc_last * reinvestment_last

    if mr_equity_adj > 0:
        roe_last = ttm_net_income_adj / mr_equity_adj
    else:
        roe_last = 0

    if ttm_net_income_adj > 0:
        reinvestment_eps_last = 1 - ttm_dividends / ttm_net_income_adj
    else:
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
    if abs(ttm_revenue/revenue[-1] - 1) > 0.0001:
        ttm = True
    else:
        ttm = False

    if ttm:
        rev_list = revenue + [ttm_revenue]
    else:
        rev_list = revenue

    first_index = -1

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

    if first_index >= 0:
        for i in rev_list[first_index:]:
            if i <= 0:
                cagr = 0


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

        # print("rev_list", rev_list)
        # print("first_revenue", first_revenue)
        # print("simple_cagr", simple_cagr)
        # print("capped_simple_cagr", capped_simple_cagr)
        # print("cagr_from_start_list", cagr_from_start_list)
        # print("cagr_from_start", cagr_from_start)
        # print("capped_cagr_from_start", capped_cagr_from_start)
        # print("cagr_from_end_list", cagr_from_end_list)
        # print("cagr_from_end", cagr_from_end)
        # print("capped_cagr_from_end", capped_cagr_from_end)

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

def currency_bond_yield(currency, alpha_3_code, country_stats):

    currency_10y_bond, mother_country = get_10y_bond_yield(currency)

    if currency_10y_bond is not None:

        filter_df = country_stats[country_stats["country"] == mother_country.replace(" ", "")].iloc[0]
        country_default_spread = float(filter_df["adjusted_default_spread"])

        #10y yield currency - default risk mother currency
        riskfree = currency_10y_bond - country_default_spread

    else:

        if alpha_3_code is None:
            return -1

        us_10y_bond, _ = get_10y_bond_yield("USD")

        filter_df = country_stats[country_stats["country"] == "UnitedStates"].iloc[0]
        us_cds = float(filter_df["adjusted_default_spread"])

        riskfree_us = us_10y_bond - us_cds

        current_year_date = datetime.now().date().replace(day=1) - relativedelta(months=2)
        last_year_date = current_year_date - relativedelta(years=1)

        cpi_data = get_df_from_table("oecd_financial", f"where location IN ('{alpha_3_code}','USA') "
                                                       f"and indicator='CPI' "
                                                       f"and date in ('{last_year_date.strftime('%Y-%m-%d')}',"
                                                       f"'{current_year_date.strftime('%Y-%m-%d')}')")

        inflation_us = cpi_data[cpi_data["location"] == "USA"]
        inflation_us = inflation_us[inflation_us["date"] == current_year_date]["value"].iloc[0] / \
                       inflation_us[inflation_us["date"] == last_year_date]["value"].iloc[0] - 1

        inflation_country = cpi_data[cpi_data["location"] == alpha_3_code]
        if inflation_country.empty:
            inflation_country = inflation_us
        else:
            inflation_country = inflation_country[inflation_country["date"] == current_year_date]["value"].iloc[0] / \
                                inflation_country[inflation_country["date"] == last_year_date]["value"].iloc[0] - 1

        riskfree = riskfree_us * float(inflation_country) / float(inflation_us)
        print("10y bond yield not found - inflation_country", inflation_country, "inflation_us", inflation_us)

    return riskfree

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

def get_industry_parameter(df, industry, region, parameter, debug=True):

    region_waterfall = {
        "Europe": "US",
        "US": "Global",
        "Japan": "Global",
        "China": "emerg",
        "India": "emerg",
        "emerg": "Global",
        "Rest": "US"
    }

    value = None

    try:
        series = df[df["region"] == region].iloc[0]
        value = series[parameter]
    except:
        pass

    # print(industry, region, parameter, value)

    if value is None or math.isnan(value):
        if region in region_waterfall:
            if debug:
                print("value not found for ", industry, region, parameter)
                print("searching now in region ", region_waterfall[region])
            return get_industry_parameter(df, industry, region_waterfall[region], parameter)
        else:
            print("*** ERROR DAMODARAN_INDUSTRY_DATA: ", industry, region, parameter)
            if parameter == "sales_capital":
                value = 1
            elif parameter == "cash_return":
                value = 0
            elif parameter == "pbv":
                value = 1
            elif parameter == "unlevered_beta":
                value = 1
            elif parameter == "opmargin_adjusted":
                value = 0.05
            elif parameter == "debt_equity":
                value = 1
            else:
                value = 0
            print("using default value ", value)
            return value
    else:
        return float(value)

def get_industry_data(industry, region, geo_segments_df, revenue, ebit_adj, revenue_delta, reinvestment, equity_mkt, debt_mkt,
                          equity_bv_adj, debt_bv_adj, mr_equity_adj, mr_debt_adj, min_std=0.1, max_std=1):

    # TAKE 1/3 value from last year
    # 2/3 value from this year

    columns = ["industry_name","region","sales_capital","cash_return","unlevered_beta","opmargin_adjusted","debt_equity","pbv"]
    df_last_year = get_df_from_table("damodaran_industry_data", f"where industry_name = '{industry}' and "
                                                                f"created_at = (SELECT MAX(created_at) "
                                                                f"FROM damodaran_industry_data "
                                                                f"WHERE created_at < date_trunc('year',now()))")[columns]
    df = get_df_from_table("damodaran_industry_data", f"where industry_name = '{industry}'", most_recent=True)[columns]

    # print(df_last_year)
    # print(df)
    df = pd.merge(df, df_last_year, left_on=["industry_name", 'region'],
                  right_on=["industry_name", 'region'], how="left")

    for x in [i for i in columns if i not in ["industry_name","region"]]:
        # if there is no last year value take the most recent
        df[x+"_y"] = df[x+"_y"].fillna(df[x+"_x"])
        # TAKE 1/3 value from last year
        # 2/3 value from this year
        df[x] = df[x+"_y"] * 1/3 + df[x+"_x"] * 2/3

    industry_sales_capital = 0
    industry_payout = 0
    pbv = 0
    unlevered_beta = 0
    industry_operating_margin = 0
    industry_debt_equity = 0

    debug=True

    if geo_segments_df is None or geo_segments_df.empty:
        industry_sales_capital = get_industry_parameter(df, industry, region, "sales_capital", debug=debug)
        industry_payout = min(1, get_industry_parameter(df, industry, region, "cash_return", debug=debug))
        pbv = get_industry_parameter(df, industry, region, "pbv", debug=debug)
        unlevered_beta = get_industry_parameter(df, industry, region, "unlevered_beta", debug=debug)
        industry_operating_margin = get_industry_parameter(df, industry, region, "opmargin_adjusted", debug=debug)
        industry_debt_equity = get_industry_parameter(df, industry, region, "debt_equity", debug=debug)

    else:
        for _, row in geo_segments_df.iterrows():
            percent = row["value"]
            r = row["region"]

            tsc = get_industry_parameter(df, industry, r, "sales_capital", debug=debug)
            ip = min(1, get_industry_parameter(df, industry, r, "cash_return", debug=debug))
            p = get_industry_parameter(df, industry, r, "pbv", debug=debug)
            ub = get_industry_parameter(df, industry, r, "unlevered_beta", debug=debug)
            tom = get_industry_parameter(df, industry, r, "opmargin_adjusted", debug=debug)
            tde = get_industry_parameter(df, industry, r, "debt_equity", debug=debug)

            industry_sales_capital += tsc * percent
            industry_payout = min(1, industry_payout + ip * percent)
            pbv += p * percent
            unlevered_beta += ub * percent
            industry_operating_margin += tom * percent
            industry_debt_equity += tde * percent

    operating_margin = []
    debt_equity = []
    sales_capital = []

    try:
        sales_capital_5y = sum(revenue_delta) / sum(reinvestment)
        if sales_capital_5y <= 0:
            sales_capital_5y = industry_sales_capital
    except:
        sales_capital_5y = industry_sales_capital

    for i in range(len(revenue)):
        if revenue[i] > 0:
            operating_margin.append(ebit_adj[i] / revenue[i])
        else:
            operating_margin.append(0)

        num = (debt_bv_adj[i] * (debt_mkt/mr_debt_adj)) if mr_debt_adj > 0 else debt_bv_adj[i]
        den = (equity_bv_adj[i] * (equity_mkt/mr_equity_adj)) if mr_equity_adj > 0 else equity_bv_adj[i]
        if den > 0 and num / den > 0:
            debt_equity.append(num/den)
        else:
            debt_equity.append(0)

        try:
            if revenue_delta[i] / reinvestment[i] > 0:
                sales_capital.append(revenue_delta[i] / reinvestment[i])
            else:
                sales_capital.append(sales_capital_5y)
        except:
            sales_capital.append(sales_capital_5y)

    weights = [x+1 for x in range(len(revenue))]
    sum_weights = sum(weights)

    # print(debt_bv_adj)
    # print(debt_mkt)
    # print(mr_debt_adj)
    # print(equity_bv_adj)
    # print(equity_mkt)
    # print(mr_equity_adj)
    # print(debt_equity)

    om_company = sum(i[0] * i[1] for i in zip(operating_margin, weights)) / sum_weights
    de_company = sum(i[0] * i[1] for i in zip(debt_equity, weights)) / sum_weights
    sc_company = sum(i[0] * i[1] for i in zip(sales_capital, weights)) / sum_weights

    std_om_company = np.std(operating_margin)
    std_de_company = np.std(debt_equity)
    std_sc_company = np.std(sales_capital)

    if om_company != 0:
        om_industry_weight = max(0, min(1, ((std_om_company / om_company) - min_std) / (max_std - min_std)))
    else:
        om_industry_weight = 1

    if de_company != 0:
        de_industry_weight = max(0, min(1, ((std_de_company / de_company) - min_std) / (max_std - min_std)))
    else:
        de_industry_weight = 1

    if sc_company != 0:
        sc_industry_weight = max(0, min(1, ((std_sc_company / sc_company) - min_std) / (max_std - min_std)))
    else:
        sc_industry_weight = 1

    target_sales_capital = sc_industry_weight * industry_sales_capital + (1 - sc_industry_weight) * sc_company
    target_debt_equity = de_industry_weight * industry_debt_equity + (1 - de_industry_weight) * de_company
    target_operating_margin = om_industry_weight * industry_operating_margin + (1 - om_industry_weight) * om_company

    # print("DEBUG TARGETS")
    # print(sales_capital)
    # print(operating_margin)
    # print(debt_equity)
    # print("sc_company",sc_company,"industry_sales_capital",industry_sales_capital,"std_sc_company",std_sc_company,"sc_industry_weight",sc_industry_weight,"target_sales_capital",target_sales_capital)
    # print("om_company",om_company,"industry_operating_margin",industry_operating_margin,"std_om_company",std_om_company,"om_industry_weight",om_industry_weight,"target_operating_margin",target_operating_margin)
    # print("de_company",de_company,"industry_debt_equity",industry_debt_equity,"std_de_company",std_de_company,"de_industry_weight",de_industry_weight,"target_debt_equity",target_debt_equity)

    return target_sales_capital, industry_payout, pbv, unlevered_beta, target_operating_margin, target_debt_equity

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
    sorted = [0 if math.isnan(x) else x for x in sorted]
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
        if third_highest == 0 or second_highest / third_highest > 10:
            result = 0
        else:
            max_val = max(sorted)
            min_val = min(sorted)
            if min_val == 0 or max_val / min_val > 3:
                result = sorted[1]
            else:
                result = median(sorted)

    return result

def get_status(fcff_delta, div_delta, liquidation_delta, country, region, company_size, company_type, dilution, complexity,
               revenue, receivables=None, inventory=None, debug=False):

    STATUS_OK = "OK"
    STATUS_NI = "NI"
    STATUS_KO = "KO"

    max_base = 0.2
    if liquidation_delta < -0.5:
        t = -max_base
    elif liquidation_delta < 0:
        t = liquidation_delta / 0.5 * max_base
    elif liquidation_delta >= 10:
        t = max_base
    else:
        t = np.log10(liquidation_delta + 1) * max_base

    if debug:
        print("base threshold", t, "(liquidation delta:", liquidation_delta, ")")

    if country == "United States":
        t += 0
    elif region in ["US","EU","Japan","Rest"]:
        t += 0.05
    else:
        t += 0.1

    if debug:
        print("country/region threshold", t, "(country:", country, ", region:", region, ")")

    if company_size == "Mega":
        t += 0
    elif company_size == "Large":
        t += 0.05
    elif company_size == "Medium":
        t += 0.12
    elif company_size == "Small":
        t += 0.18
    elif company_size == "Micro":
        t += 0.25
    else:
        t += 0.3

    if debug:
        print("company size threshold", t, "(company size:", company_size, ")")

    elif complexity == 2:
        t += 0.03
    elif complexity == 3:
        t += 0.06
    elif complexity == 4:
        t += 0.1
    elif complexity == 5:
        t += 0.2

    if debug:
        print("company complexity threshold", t, "(complexity:", complexity, ")")

    if dilution > 0.1:
        t += 0.05
    elif dilution > 0.02:
        t += 0.02

    if debug:
        print("dilution threshold", t, "(dilution:", dilution, ")")

    first_idx = 0
    for i, r in enumerate(revenue):
        if r > 0:
            first_idx = i
            break

    ratio, score = calculate_divergence(revenue[first_idx], inventory[first_idx], revenue[-1], inventory[-1])
    if score is None:
        if ratio < 10:
            t += 0.02
        elif ratio < 5:
            t += 0.05
    else:
        if score > 0.8 or score < -0.4:
            t += 0.05
        elif score > 0.4 or score < -0.2:
            t += 0.02

    if debug:
        print("inventory divergence threshold", t, "(inventory:", inventory, ", revenue:", revenue, ")")

    ratio, score = calculate_divergence(revenue[first_idx], receivables[first_idx], revenue[-1], receivables[-1])
    if score is None:
        if ratio < 10:
            t += 0.02
        elif ratio < 5:
            t += 0.05
    else:
        if score > 0.8 or score < -0.4:
            t += 0.05
        elif score > 0.4 or score < -0.2:
            t += 0.02

    if debug:
        print("receivables divergence threshold", t, "(receivables:", receivables, ", revenue:", revenue, ")")

    if company_type["declining"]:
        t += 0.05
    if company_type["turn_around"]:
        t += 0.05
    if company_type["cyclical"]:
        t += 0.05

    if debug:
        print("company type threshold", t, "(company_type:", company_type, ")")

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
        print("status", status)
        print()

    return status


import math


def calculate_divergence(initial_a, initial_b, final_a, final_b):

    try:
        ratio = max(initial_a, final_a) / max(initial_b, final_b)
    except:
        return 0, 0

    try:
        growth_a = final_a / initial_a
        growth_b = final_b / initial_b
        growth_ratio = growth_b / growth_a - 1
    except:
        return ratio, None

    # print(initial_a, "=>", final_a, "|", initial_b, "=>", final_b, "ratio:", ratio, "growth_ratio:", growth_ratio, "result:", growth_ratio/ratio)

    return ratio, growth_ratio / ratio

    # A growing
    # < -0.5 = risky
    # > 0.4 = risky
    # > 2 = bad

    # A declining
    # < -2 = bad
    # < 0.4 risky


if __name__ == '__main__':

    initial_a = 100

    final_a = [0]
    initial_b = [100]
    final_b = [
        [50, 0]
    ]

    for f_a in final_a:
        for idx, i_b in enumerate(initial_b):
            for f_b in final_b[idx]:
                divergence = calculate_divergence(initial_a, i_b, f_a, f_b)
            print()
        print("=====")
