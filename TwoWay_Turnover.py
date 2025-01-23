from xml.dom import IndexSizeErr
from requests import get
import pandas as pd
import datetime as dt
import requests
from urllib3 import exceptions, disable_warnings
import datetime
import os
from operator import index
from datetime import date, timedelta
from pandas.tseries.offsets import BDay
from io import StringIO
from requests.auth import HTTPBasicAuth
import calendar
from dateutil.relativedelta import relativedelta
import sys
directory = os.path.dirname(os.path.abspath(__file__))
sys.path.append(directory + r'\STOXX')
import stoxx
 
disable_warnings(exceptions.InsecureRequestWarning)
 
# User variables
Review_Date = pd.read_csv(directory + "\Input\Dates_Frame_cutoff_19-24.csv", index_col=0, parse_dates=["Review"])
Review_Date["Review_T-1"] = Review_Date["Review"] - pd.tseries.offsets.BDay()

 
header = {
    "Content-Type": "application/json",
    "iStudio-User": "ysun@qontigo.com"
}
 
server = {
    "PROD": {
        "url": "https://vmindexstudioprd01:8002/",
        "ssl": False
    }
}
 
returns = {
    "NR": 2,
    "PR": 1,
    "GR": 3,
}
 
def fetch_batch_composition_istudio(batch_ids, Review_Date, Date_Download, server_type="PROD", batch_name="Output_file"):
    compositions = []
    for batch_id in batch_ids:
        composition = pd.DataFrame()
        for current_date in Review_Date[Date_Download]:
            url = "{}/api/2.0/analytics/batch/{}/composition/export/{}/".format(
                server[server_type]["url"], batch_id, current_date.strftime("%Y-%m-%d"))
            json_result = get(url=url, headers=header, verify=server[server_type]["ssl"]).json()
 
            if "data" in json_result.keys():
                print("Components found for batch id: {}, keyword: {} at date - {}".format(
                    batch_id, batch_id, current_date))
                composition = composition.append(pd.json_normalize(json_result["data"]["composition_export"]))
 
        composition.reset_index(drop=True, inplace=True)
 
        composition = composition[composition.index_type.isin(['Price'])]
        composition = composition[composition.index_currency.isin(['EUR'])]
    return composition 


def get_prod_comp(symbol, date_i, oc):
    """" Reads index composition from STOXX composition folder/ SID/ STOXX website (priority in that order) and returns dataframe with composition
    This code should be valid even if the STOXX composition folder is no longer updated/ deleted. It will look for SID comps in that case.

    Parameters
    ----------
    - symbol : str, desired index symbol
    - date : datetime, desired date in datetime format
    - oc: str, desired type, 'open' or 'close'
    """

    proxies = {'http': 'http://webproxy.deutsche-boerse.de:8080',
               'https': 'http://webproxy.deutsche-boerse.de:8080', }

    symbol = symbol.lower()

    oc = oc.lower()
    if oc.lower() == 'open':
        Parse_Date = ['Next_Trading_Day']
    elif oc.lower() == 'close':
        Parse_Date = ['Date']

    try:
        # Prod composition folder: 'S:\Stoxx\Stoxx_Reports\stoxx_composition_files\STOXX\'
        comp = pd.read_csv('S:\Stoxx\Stoxx_Reports\stoxx_composition_files\STOXX\\' + symbol + '\\' + oc + '_' + symbol + '_' + date_i.strftime('%Y%m%d') + '.csv',
                           sep=";", encoding="ISO-8859-1", parse_dates=Parse_Date, dayfirst=True,
                           dtype={'SEDOL': str, 'Internal_Number': str, 'ISIN': str, 'RIC': str, 'ICB': str, 'Weight': float,
                                  'Shares': float, 'Free_Float': float, 'Close_adjusted_local': float,
                                  'FX_local_to_Index_Currency': float, 'Mcap_Units_Index_Currency': float}, index_col=False,
                            keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
        print ('%s %s source: STOXX composition folder.' %(symbol.upper(), date_i.strftime("%d-%m-%Y")))
    except:
        try:
            # Prod SID (MADDOX02): 'http://maddox2.prod.ci.dom/sidwebapi/Help/Api/GET-api-Index-GetOpenCloseComposition_indexSymbol_date_type
            compsidpath = 'http://maddox2.prod.ci.dom/sidwebapi/api/Index/GetOpenCloseComposition?indexSymbol=' + symbol + '&date=' + date_i.strftime('%Y-%m-%d') + '&type=' + oc
            comp =pd.read_csv(compsidpath, sep=";", encoding="ISO-8859-1", parse_dates=Parse_Date, dayfirst=True,
                              dtype={'SEDOL': str, 'Internal_Number': str, 'ISIN': str, 'RIC': str, 'ICB': str, 'Weight': float,
                                     'Shares': float, 'Free_Float': float, 'Close_adjusted_local': float,
                                     'FX_local_to_Index_Currency': float, 'Mcap_Units_Index_Currency': float}, index_col=False,
                              keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
            comp.rename(columns={'CapFactor':'Capfactor'}, inplace=True)
            if comp.empty:
                proxies = {'http': 'http://webproxy.deutsche-boerse.de:8080',
                           'https': 'http://webproxy.deutsche-boerse.de:8080', }
                # url = 'http://www.stoxx.com/download/data/composition_files/' + symbol + '/' + oc + '_' + symbol + '_' + date_i.strftime('%Y%m%d') + '.csv' - OLD link. Leads to old format, this is being phased out
                # New composition files in the website have missing fields:
                # 'CUSIP', 'Cash_Dividend_Amount', 'Cash_Dividend_Currency', 'Ci-factor', 'Corporate_Action_Description',
                #  'Country', 'Exchange', 'ICB', 'Mcap_Units_local', 'RIC', 'SEDOL', 'Special_Cash_Dividend_Amount', 'Special_Cash_Dividend_Currency'
                if oc.lower() == 'open':
                    correct_date_i = date_i - BDay(1)
                elif oc.lower() == 'close':
                    correct_date_i = date_i
                url = 'https://www.stoxx.com/document/Indices/Current/Composition_Files/' + oc + 'composition_' + symbol + '_' + correct_date_i.strftime('%Y%m%d') + '.csv'
                rr = requests.get(url.format(), stream=True, auth=HTTPBasicAuth('stoxxindex@stoxx.com', 'Welcome11'), proxies=proxies)
                dataTS = rr.text
                b = StringIO(dataTS)
                comp = pd.read_csv(b, sep=";", encoding="ISO-8859-1", parse_dates=Parse_Date, dayfirst=True,
                                   dtype={'SEDOL': str, 'Internal_Key': str, 'ISIN': str, 'RIC': str, 'ICB': str, 'Weight': float,
                                          'Shares': float, 'Free_Float': float, 'Close_adjusted_local': float,
                                          'FX_local_to_Index_Currency': float, 'Mcap_Units_Index_Currency': float}, index_col=False,
                                   keep_default_na=False, na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
                comp.rename(columns={'Internal_Key': 'Internal_Number'}, inplace=True)
                # Some of the missing fields will be fetched from SID.
                othersid = 'http://maddox2.prod.ci.dom/sidwebapi/api/Security/getSecurityTimeSeriesByIndexSymbolCSV?calendarName=STOXXCAL&indexSymbol=' + symbol + '&targetCcy=EUR&startDate=' + date_i.strftime("%Y-%m-%d") + '&endDate=' + date_i.strftime("%Y-%m-%d")
                other_i = pd.read_csv(othersid, parse_dates=['vd', 'prevVd'], dayfirst=True, sep=";",
                                      dtype={'stoxx_id': str, 'isin': str, 'sedol': str, 'ric': str, 'icb_subsector': str, 'icb2_subsector': str, 'stoxxId_primary_company': str, 'stoxxId_primary_company': str},
                                      index_col=False, encoding="ISO-8859-1", keep_default_na=False,
                                      na_values=['', '#N/A', '#N/A N/A', '#NA', '-NaN', '-nan', 'N/A', 'NULL', 'NaN', 'n/a', 'nan', 'null'])
                other_i.rename(columns={'stoxx_id': 'Internal_Number', 'sedol': 'SEDOL', 'ric': 'RIC', 'icb5_subsector': 'ICB', 'iso_country_dom': 'Country', 'exchangeName':'Exchange'}, inplace=True)
                comp = pd.merge(comp, other_i[['Internal_Number','SEDOL','RIC', 'ICB',  'Country','Exchange']], on=['Internal_Number'], how='left')
                print('%s %s source: STOXX website.' % (symbol.upper(), date_i.strftime("%d-%m-%Y")))
            else:
                print('%s %s source: SID (MADDOX02).' % (symbol.upper(), date_i.strftime("%d-%m-%Y")))
        except:
            print ('Index composition was not found for %s for %s. Please investigate.' %(symbol.upper(), date_i.strftime("%d-%m-%Y")))
    try:
        cols = comp.columns.tolist()
        cols = [x for x in cols if 'Unnamed' not in x]
        comp = comp[cols]
    except:
        pass
    return comp


def fetch_batch_composition_sid(Review_Date, Date_Download):
    Output = pd.DataFrame()
    for idx in idxs:
        for date in Review_Date[Date_Download]:
            date = pd.to_datetime(date)
            cons = get_prod_comp(idx, dt.date(date.year, date.month, date.day), oc = opclo)
            # date_review = Review_Date[Review_Date["Cutoff"] == date]["Date"].values[0]
            cons["Date"] = date
            Output = pd.concat((Output, cons))
    return Output 



# Download from SID 

idxs = ["SDGP"]
# idx = ['']
opclo = "close"
Date_Download = "Review"

Date_Download = "Review"
Composition_T_SID = fetch_batch_composition_sid(Review_Date, Date_Download)

Date_Download = "Review_T-1"
Composition_T_Minus_1_SID = fetch_batch_composition_sid(Review_Date, Date_Download)

 
# Rename columns
Composition_T_Minus_1_SID["next_trading_day"] = Composition_T_Minus_1_SID["Date"] + pd.tseries.offsets.BDay()
new_column_names = [f"{col}_Prev_Business_Day" for col in Composition_T_Minus_1_SID.columns]
Composition_T_Minus_1_SID.rename(columns=dict(zip(Composition_T_Minus_1_SID.columns, new_column_names)), inplace=True)

 
# Merge the two DataFrames
Composition_T_SID = Composition_T_SID.merge(Composition_T_Minus_1_SID, left_on = ["Date", "Internal_Number"], right_on = ["next_trading_day_Prev_Business_Day", "Internal_Number_Prev_Business_Day"],
                                    how = "outer")
 
# Calculate ABS Weights difference
Composition_T_SID["Weight"] = Composition_T_SID["Weight"]/100
Composition_T_SID["Weight_Prev_Business_Day"] = Composition_T_SID["Weight_Prev_Business_Day"]/100
Composition_T_SID["Weight_Prev_Business_Day"] = Composition_T_SID["Weight_Prev_Business_Day"].fillna(0)
Composition_T_SID["Weight"] = Composition_T_SID["Weight"].fillna(0)
Composition_T_SID["ABS_Weight_Difference"] = abs(Composition_T_SID["Weight"] - Composition_T_SID["Weight_Prev_Business_Day"])
 
Composition_T_SID["next_trading_day_Prev_Business_Day"] = Composition_T_SID["next_trading_day_Prev_Business_Day"].fillna(Composition_T_SID["Date"])
TwoWayTurnover = Composition_T_SID.groupby("next_trading_day_Prev_Business_Day")["ABS_Weight_Difference"].sum()
TwoWayTurnover = TwoWayTurnover.reset_index()
TwoWayTurnover = TwoWayTurnover.rename(columns={"next_trading_day_Prev_Business_Day": "Review_Date", "ABS_Weight_Difference": "Two_Ways_Turnover"})
TwoWayTurnover.to_clipboard()


#Download from iStudio 

batch_ids = [16041]
 
# Get composition by Review Date
Date_Download = "Review"
Composition_T_iStudio = fetch_batch_composition_istudio(batch_ids, Review_Date, Date_Download)

# Get previous business day Review Date
Date_Download = "Review_T-1"
Composition_T_Minus_1_iStudio = fetch_batch_composition_istudio(batch_ids, Review_Date, Date_Download)
# Composition_T_Minus_1_iStudio = Composition_T_Minus_1_iStudio[Composition_T_Minus_1_iStudio["weight"]!=0]
 
# Add "Prev_Business_Day" to each column name
new_column_names = [f"{col}_Prev_Business_Day" for col in Composition_T_Minus_1_iStudio.columns]
 
# Rename columns
Composition_T_Minus_1_iStudio.rename(columns=dict(zip(Composition_T_Minus_1_iStudio.columns, new_column_names)), inplace=True)
 
# Merge the two DataFrames
Composition_T_iStudio = Composition_T_iStudio.merge(Composition_T_Minus_1_iStudio, left_on = ["close_day", "internal_number"], right_on = ["next_trading_day_Prev_Business_Day", "internal_number_Prev_Business_Day"],
                                    how = "outer")
 
# Calculate ABS Weights difference
Composition_T_iStudio["weight"] = Composition_T_iStudio["weight"].fillna(0)
Composition_T_iStudio["ABS_Weight_Difference"] = abs(Composition_T_iStudio["weight"] - Composition_T_iStudio["weight_Prev_Business_Day"])
 
TwoWayTurnover = Composition_T_iStudio.groupby("next_trading_day_Prev_Business_Day")["ABS_Weight_Difference"].sum()
TwoWayTurnover = TwoWayTurnover.reset_index()
TwoWayTurnover = TwoWayTurnover.rename(columns={"next_trading_day_Prev_Business_Day": "Review_Date", "ABS_Weight_Difference": "Two_Ways_Turnover"})
TwoWayTurnover.to_clipboard()