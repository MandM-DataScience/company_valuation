from configparser import ConfigParser
import os
from datetime import datetime

import pandas as pd

import psycopg2
from dateutil.relativedelta import relativedelta

from investing_com import get_10y_bond_yield


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

    country_to_region = {
        "Cyprus": "EU",
        "Macau": "China",
        "IsleofMan": "global",
        "BritishVirginIslands": "global",
        "Greece": "EU",
        "Cambodia": "emerg",
        "Malaysia": "emerg",
        "Bermuda": "emerg",
        "Canada": "Rest",
        "UnitedStates": "US",
        "UnitedArabEmirates": "US",
        "Japan": "Japan",
        "Australia": "Rest",
        "NewZealand": "Rest",
        "Austria": "EU",
        "Belgium": "EU",
        "Denmark": "EU",
        "Finland": "EU",
        "France": "EU",
        "Germany": "EU",
        "Ireland": "EU",
        "Italy": "EU",
        "Luxembourg": "EU",
        "Netherlands": "EU",
        "Portugal": "EU",
        "Spain": "EU",
        "Sweden": "EU",
        "Switzerland": "EU",
        "UnitedKingdom": "EU",
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
        "Iceland": "EU",
        "Liechtenstein": "EU",
        "Monaco": "EU",
        "Norway": "EU",
        "SaudiArabia": "emerg"
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

def currency_bond_yield(country, alpha_3_code, country_default_spread):

    currency_10y_bond = get_10y_bond_yield(country)

    if currency_10y_bond is None:
        us_10y_bond = get_10y_bond_yield("United States")

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

        currency_10y_bond = us_10y_bond * float(inflation_country) / float(inflation_us)
        currency_10y_bond += country_default_spread
        print("10y bond yield not found - inflation_country", inflation_country, "inflation_us", inflation_us)

    return currency_10y_bond

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

    if value is None:
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

def get_industry_data(industry, region, debug=True):

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

    # print(df)

    target_sales_capital = get_industry_parameter(df, industry, region, "sales_capital", debug=debug)
    industry_payout = get_industry_parameter(df, industry, region, "cash_return", debug=debug)

    if industry_payout > 1:
        industry_payout = 1

    pbv = get_industry_parameter(df, industry, region, "pbv", debug=debug)
    unlevered_beta = get_industry_parameter(df, industry, region, "unlevered_beta", debug=debug)
    target_operating_margin = get_industry_parameter(df, industry, region, "opmargin_adjusted", debug=debug)
    target_debt_equity = get_industry_parameter(df, industry, region, "debt_equity", debug=debug)

    return target_sales_capital, industry_payout, pbv, unlevered_beta, target_operating_margin, target_debt_equity