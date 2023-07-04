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
    "Guernsey": "Global",
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
    cur.execute(f"""SELECT * FROM {tablename} {where}""")
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

    try:
        region = country_to_region[country.replace(" ","")]
    except:
        print("country not found in country_to_region dict:", country)
        region = "Global"

    try:
        industry = industry_translation[industry]
    except:
        print(f"\n#######\nCould not find industry: {industry} mapping. "
              f"Check industry_translation dictionary.\n#######\n")
        industry = "Total Market"

    return company_name, country, industry, region


