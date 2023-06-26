import math
from configparser import ConfigParser
import os
from datetime import datetime

import pandas as pd

import psycopg2
from dateutil.relativedelta import relativedelta

from investing_com import get_10y_bond_yield
import numpy as np

country_to_region = {
    "CzechRepublic": "emerg",
    "Cyprus": "Europe",
    "Macau": "China",
    "IsleofMan": "Global",
    "BritishVirginIslands": "Global",
    "Greece": "Europe",
    "Cambodia": "emerg",
    "Malaysia": "emerg",
    "Bermuda": "emerg",
    "Canada": "Rest",
    "UnitedStates": "US",
    "UnitedArabEmirates": "US",
    "Japan": "Japan",
    "Australia": "Rest",
    "NewZealand": "Rest",
    "Austria": "Europe",
    "Belgium": "Europe",
    "Denmark": "Europe",
    "Finland": "Europe",
    "France": "Europe",
    "Germany": "Europe",
    "Ireland": "Europe",
    "Italy": "Europe",
    "Luxembourg": "Europe",
    "Netherlands": "Europe",
    "Portugal": "Europe",
    "Spain": "Europe",
    "Sweden": "Europe",
    "Switzerland": "Europe",
    "UnitedKingdom": "Europe",
    "China": "China",
    "HongKong": "China",
    "Taiwan": "China",
    "India": "India",
    "Argentina": "emerg",
    "Brazil": "emerg",
    "Chile": "emerg",
    "Colombia": "emerg",
    "Ecuador": "emerg",
    "Indonesia": "emerg",
    "Israel": "emerg",
    "Mexico": "emerg",
    "Peru": "emerg",
    "Philippines": "emerg",
    "Poland": "emerg",
    "Estonia": "emerg",
    "Romania": "emerg",
    "Russia": "emerg",
    "Latvia": "emerg",
    "Lithuania": "emerg",
    "Singapore": "China",
    "Thailand": "emerg",
    "Turkey": "emerg",
    "SouthKorea": "Japan",
    "SouthAfrica": "emerg",
    "Iceland": "Europe",
    "Liechtenstein": "Europe",
    "Monaco": "Europe",
    "Norway": "Europe",
    "SaudiArabia": "emerg",


    "Africa":"emerg",
    "Asia":"emerg",
    "Australia & New Zealand":"Rest",
    "Caribbean":"emerg",
    "Central and South America":"emerg",
    "Eastern Europe & Russia":"emerg",
    "Middle East":"emerg",
    "North America":"US",
    "Western Europe":"Europe",
    "Global":"Global",

}
industry_translation = {
    'Advertising Agencies': 'Advertising',
    'Aerospace & Defense': 'Aerospace/Defense',
    'Airlines': 'Air Transport',
    'Airports & Air Services': 'Air Transport',
    'Apparel Manufacturing': 'Apparel',
    'Apparel Retail': 'Apparel',
    'Textile Manufacturing': 'Apparel',
    'Auto Manufacturers': 'Auto & Truck',
    'Auto Manufacturers - Major': 'Auto & Truck',
    'Auto Parts': 'Auto Parts',
    'Banks—Diversified': 'Bank (Money Center)',
    'Banks—Regional': 'Banks (Regional)',
    'Beverages—Brewers': 'Beverage (Alcoholic)',
    'Beverages—Wineries & Distilleries': 'Beverage (Alcoholic)',
    'Beverages—Non-Alcoholic': 'Beverage (Soft)',
    'Broadcasting': 'Broadcasting',
    'Asset Management': 'Brokerage & Investment Banking',
    'Capital Markets': 'Brokerage & Investment Banking',
    'Closed-End Fund - Equity': 'Brokerage & Investment Banking',
    'Building Materials': 'Building Materials',
    'Business Equipment & Supplies': 'Business & Consumer Services',
    'Chemicals': 'Chemical (Basic)',
    'Chemicals - Major Diversified': 'Chemical (Diversified)',
    'Specialty Chemicals': 'Chemical (Specialty)',
    'Coking Coal': 'Coal & Related Energy',
    'Thermal Coal': 'Coal & Related Energy',
    'Information Technology Services': 'Computer Services',
    'Computer Hardware': 'Computers/Peripherals',
    'Building Products & Equipment': 'Construction Supplies',
    'Conglomerates': 'Diversified',
    'Biotechnology': 'Drugs (Biotechnology)',
    'Drug Manufacturers—General': 'Drugs (Pharmaceutical)',
    'Drug Manufacturers—Specialty & Generic': 'Drugs (Pharmaceutical)',
    'Pharmaceutical Retailers': 'Drugs (Pharmaceutical)',
    'Education & Training Services': 'Education',
    'Electrical Equipment & Parts': 'Electrical Equipment',
    'Electronic Components': 'Electrical Equipment',
    'Consumer Electronics': 'Electronics (Consumer & Office)',
    'Electronic Gaming & Multimedia': 'Software (Entertainment)',
    'Electronics & Computer Distribution': 'Electronics (General)',
    'Engineering & Construction': 'Engineering/Construction',
    'Entertainment': 'Entertainment',
    'Leisure': 'Entertainment',
    'Pollution & Treatment Controls': 'Environmental & Waste Services',
    'Waste Management': 'Environmental & Waste Services',
    'Agricultural Inputs': 'Farming/Agriculture',
    'Farm & Heavy Construction Machinery': 'Machinery',
    'Farm Products': 'Farming/Agriculture',
    'Credit Services': 'Financial Svcs. (Non-bank & Insurance)',
    'Confectioners': 'Food Processing',
    'Packaged Foods': 'Food Processing',
    'Food Distribution': 'Food Wholesalers',
    'Furnishings, Fixtures & Appliances': 'Furn/Home Furnishings',
    'Solar': 'Green & Renewable Energy',
    'Medical Instruments & Supplies': 'Healthcare Products',
    'Medical Distribution': 'Healthcare Support Services',
    'Scientific & Technical Instruments': 'Heathcare Information and Technology',
    'Diagnostics & Research': 'Heathcare Information and Technology',
    'Residential Construction': 'Homebuilding',
    'Medical Care Facilities': 'Hospitals/Healthcare Facilities',
    'Resorts & Casinos': 'Hotel/Gaming',
    'Gambling': 'Hotel/Gaming',
    'Lodging': 'Hotel/Gaming',
    'Household & Personal Products': 'Household Products',
    'Internet Content & Information': 'Information Services',
    'Insurance—Diversified': 'Insurance (General)',
    'Insurance—Life': 'Insurance (Life)',
    'Insurance—Property & Casualty': 'Insurance (Prop/Cas.)',
    'Tools & Accessories': 'Machinery',
    'Specialty Industrial Machinery': 'Machinery',
    'Other Industrial Metals & Mining': 'Metals & Mining',
    'Aluminum': 'Metals & Mining',
    'Copper': 'Metals & Mining',
    'Metal Fabrication': 'Metals & Mining',
    'Consulting Services': 'Office Equipment & Services',
    'Specialty Business Services': 'Office Equipment & Services',
    'Oil & Gas Integrated': 'Oil/Gas (Integrated)',
    'Oil & Gas E&P': 'Oil/Gas (Production and Exploration)',
    'Oil & Gas Refining & Marketing': 'Oil/Gas Distribution',
    'Oil & Gas Equipment & Services': 'Oilfield Svcs/Equip.',
    'Packaging & Containers': 'Packaging & Container',
    'Paper & Paper Products': 'Paper/Forest Products',
    'Lumber & Wood Production': 'Paper/Forest Products',
    'Independent Oil & Gas': 'Power',
    'Oil & Gas Drilling': 'Power',
    'Oil & Gas Midstream': 'Power',
    'Uranium': 'Power',
    'Other Precious Metals & Mining': 'Precious Metals',
    'Gold': 'Precious Metals',
    'Silver': 'Precious Metals',
    'Publishing': 'Publishing & Newspapers',
    'REIT—Diversified': 'R.E.I.T.',
    'REIT—Healthcare Facilities': 'R.E.I.T.',
    'REIT—Hotel & Motel': 'R.E.I.T.',
    'REIT—Industrial': 'R.E.I.T.',
    'REIT—Mortgage': 'R.E.I.T.',
    'REIT—Office': 'R.E.I.T.',
    'REIT—Residential': 'R.E.I.T.',
    'REIT—Retail': 'R.E.I.T.', 'REIT—Specialty': 'R.E.I.T.',
    'Real Estate—Development': 'Real Estate (Development)',
    'Real Estate—Diversified': 'Real Estate (General/Diversified)',
    'Property Management': 'Real Estate (Operations & Services)',
    'Real Estate Services': 'Real Estate (Operations & Services)',
    'Rental & Leasing Services': 'Real Estate (Operations & Services)',
    'Recreational Vehicles': 'Recreation',
    'Insurance—Reinsurance': 'Reinsurance',
    'Restaurants': 'Restaurant/Dining',
    'Auto & Truck Dealerships': 'Retail (Automotive)',
    'Home Improvement Retail': 'Retail (Building Supply)',
    'Department Stores': 'Retail (Distributors)',
    'Discount Stores': 'Retail (Distributors)',
    'Luxury Goods': 'Retail (General)',
    'Grocery Stores': 'Retail (Grocery and Food)',
    'Internet Retail': 'Retail (Online)',
    'Specialty Retail': 'Retail (Special Lines)',
    'Semiconductors': 'Semiconductor',
    'Semiconductor Equipment & Materials': 'Semiconductor Equip',
    'Marine Shipping': 'Shipbuilding & Marine',
    'Footwear & Accessories': 'Shoe',
    'Software—Infrastructure': 'Software (Internet)',
    'Software—Application': 'Software (System & Application)',
    'Technical & System Software': 'Software (System & Application)',
    'Steel': 'Steel',
    'Communication Equipment': 'Telecom. Equipment',
    'Telecom Services': 'Telecom. Services',
    'Tobacco': 'Tobacco',
    'Travel Services': 'Transportation',
    'Integrated Freight & Logistics': 'Transportation',
    'Railroads': 'Transportation (Railroads)', 'Trucking': 'Trucking',
    'Utilities—Diversified': 'Utility (General)',
    'Utilities—Independent Power Producers': 'Utility (General)',
    'Utilities—Regulated Electric': 'Utility (General)',
    'Utilities—Regulated Gas': 'Utility (General)',
    'Utilities—Renewable': 'Utility (General)',
    'Gas Utilities': 'Utility (General)',
    'Utilities—Regulated Water': 'Utility (Water)',
    'Financial Conglomerates': 'Bank (Money Center)',
    'Financial Data & Stock Exchanges': 'Brokerage & Investment Banking',
    'Health Information Services': 'Heathcare Information and Technology',
    'Healthcare Plans': 'Healthcare Support Services',
    'Industrial Distribution': 'Trucking',
    'Infrastructure Operations': 'Machinery',
    'Insurance Brokers': 'Insurance (General)',
    'Insurance—Specialty': 'Insurance (General)',
    'Medical Appliances & Equipment': 'Healthcare Products',
    'Medical Devices': 'Healthcare Products',
    'Mortgage Finance': 'Banks (Regional)',
    'Personal Services': 'Business & Consumer Services',
    'Security & Protection Services': 'Business & Consumer Services',
    'Shell Companies': 'Financial Svcs. (Non-bank & Insurance)',
    'Staffing & Employment Services': 'Business & Consumer Services',
    'Staffing & Outsourcing Services': 'Business & Consumer Services',
    'Entertainment - Diversified': 'Entertainment'}

def get_connection():

    parser = ConfigParser()
    _ = parser.read(os.path.join("credentials.cfg"))
    database = parser.get("postgresql", "DB_NAME")
    user = parser.get("postgresql", "DB_USER")
    password = parser.get("postgresql", "DB_PASS")
    host = parser.get("postgresql", "DB_HOST")

    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connection parameters
        params = {
            'database': database,
            'user': user,
            'password': password,
            'host': host
        }

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print("CONNECTION ERROR: ", error)
        if conn is not None:
            conn.close()

def get_df_from_table(tablename, where=";", most_recent=False):
    if most_recent:
        if where == ";":
            where = f" WHERE created_at = (SELECT MAX(created_at) FROM {tablename})"
        else:
            where += f" AND created_at = (SELECT MAX(created_at) FROM {tablename})"

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f'''SELECT * FROM {tablename} {where}''')
    data = cur.fetchall()
    cols = []
    for elt in cur.description:
        cols.append(elt[0])
    df = pd.DataFrame(data=data, columns=cols)
    cur.close()
    return df

def get_generic_info(ticker):

    ticker_info = get_df_from_table("yahoo_equity_tickers", f"where symbol = '{ticker}'", most_recent=True).iloc[0]
    ticker_additional_info = get_df_from_table("tickers_additional_info", f"where symbol = '{ticker}'").iloc[0]
    company_name = ticker_info["long_name"]
    country = ticker_additional_info["country"]
    industry = ticker_additional_info["industry"]
    region = country_to_region[country.replace(" ","")]

    try:
        industry = industry_translation[industry]
    except:
        print(f"\n#######\nCould not find industry: {industry} mapping. "
              f"Check industry_translation dictionary.\n#######\n")
        industry = "Total Market"

    return company_name, country, industry, region

def currency_bond_yield(currency, alpha_3_code, country_stats):

    currency_10y_bond, mother_country = get_10y_bond_yield(currency)

    if currency_10y_bond is not None:

        filter_df = country_stats[country_stats["country"] == mother_country.replace(" ", "")].iloc[0]
        country_default_spread = float(filter_df["adjusted_default_spread"])

        #10y yield currency - default risk mother currency
        riskfree = currency_10y_bond - country_default_spread

    else:
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

    sales_capital_5y = sum(revenue_delta) / sum(reinvestment)
    if sales_capital_5y <= 0:
        sales_capital_5y = industry_sales_capital

    for i in range(len(revenue)):
        if revenue[i] > 0:
            operating_margin.append(ebit_adj[i] / revenue[i])
        else:
            operating_margin.append(0)

        if (equity_bv_adj[i] * (equity_mkt/mr_equity_adj)) > 0:
            debt_equity.append((debt_bv_adj[i] * (debt_mkt/mr_debt_adj)) / (equity_bv_adj[i] * (equity_mkt/mr_equity_adj)))
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

    om_industry_weight = max(0, min(1, ((std_om_company / om_company) - min_std) / (max_std - min_std)))
    de_industry_weight = max(0, min(1, ((std_de_company / de_company) - min_std) / (max_std - min_std)))
    sc_industry_weight = max(0, min(1, ((std_sc_company / sc_company) - min_std) / (max_std - min_std)))

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
