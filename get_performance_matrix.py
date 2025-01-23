import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time 
import os
from io import StringIO
from datetime import datetime, timedelta

directory = os.path.dirname(os.path.abspath(__file__))


def get_from_website(soup,index_name):
    sec_allocation = soup.find('table',{'id':'supersector-weighting-data'})
    allocation_data = []

    for row in sec_allocation.find_all('tr'):
        columns = row.find_all(['th', 'td'])
        allocation_data.append([col.get_text(strip=True) for col in columns])

    supersector = pd.DataFrame(allocation_data[1:], columns= ['Supersector', index_name])
    supersector.iloc[:,1:2] = supersector.iloc[:,1:2].astype(float)
    industry = pd.merge(supersector, icb_mapping, left_on='Supersector', right_on='ICB Supersector', how='left')
    industry = industry.groupby('ICB Industry')[industry.columns[1]].sum().reset_index()

    # Fundamentals
    Tables = soup.findAll('table',{'class':'listing-table'})
    Descriptive = Tables[0]
    Fundamentals = Tables[2]
    Fundamentals = pd.read_html(str(Fundamentals), header=[0, 1], index_col=0)[0]
    Descriptive = pd.read_html(str(Descriptive), header=[0, 1], index_col=0)[0]

    return industry, Descriptive, Fundamentals

def get_dates_yearly(df):
    Dates_yearly = pd.DataFrame()
    end_date = df.index[-1]
    first_date = df.index[0]
    month = end_date.month 
    year = end_date.year
    years = end_date.year - first_date.year
    latest_date = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
    for i in range(years):
        start_date = latest_date - timedelta(days = ((i+1)*365+1))
        while start_date.weekday() >= 5:  # 5 and 6 represent Saturday and Sunday
            start_date -= timedelta(days=1)
        # if start_date.weekday() == 5: 
        #     start_date -= timedelta(days=1)
        if start_date.month!=end_date.month:
            start_date -= timedelta(days=1)
        while start_date.weekday() >= 5:  # 5 and 6 represent Saturday and Sunday
            start_date -= timedelta(days=1)
        Dates_yearly.loc[i,'Start'] = start_date
        Dates_yearly.loc[i,'End'] = end_date
        end_date = start_date
    return Dates_yearly


def get_dates_yearly_EOY(df):
    Dates_yearly_EOY = pd.DataFrame()  
    latest_date = df.index[-1]  
    first_date = df.index[0]  
    current_date = latest_date 
    last_year = latest_date.year 
    
    while (current_date.year >= first_date.year +1) & (current_date <= latest_date):
        if current_date.year == last_year:
            end_date = current_date
        else:
            end_date = pd.Timestamp(year=current_date.year, month=12, day=31)
        
        if end_date.weekday() >= 5:  # If Saturday (5) or Sunday (6)
            end_date -= timedelta(days=end_date.weekday() - 4)
        
        start_date = pd.Timestamp(year=current_date.year-1, month=12, day=31)
        if start_date .weekday() >= 5:  # If Saturday (5) or Sunday (6)
            start_date -= timedelta(days=start_date .weekday() - 4)
        Dates_yearly_EOY.loc[current_date.year, 'Start'] = start_date 
        Dates_yearly_EOY.loc[current_date.year, 'End'] = end_date
        
        # Update current_date to the start of the previous year
        current_date = start_date 
    
    return Dates_yearly_EOY


# def get_dates_EOM(df):
#     Dates_yearly = pd.DataFrame()
#     latest_date = df.index[-1]
#     first_date = df.index[0]
#     month = latest_date.month
#     years = latest_date.year - first_date.year
#     date_range = pd.date_range(start=first_date, end=latest_date, freq='BM')  # Business month end dates
#     last_month_days = [date_range[i] for i in range(len(date_range)) if date_range[i].month == month and (i == len(date_range) - 1 or date_range[i + 1].month != month)]
    

def calculate_yearly_perf(df,EOY):
    actual_returns = pd.DataFrame()
    actual_returns['Return Overall'] = (df.iloc[-1]/df.iloc[0])-1
    if EOY == True:
        Date_Frame = get_dates_yearly_EOY(df)
    else:
        Date_Frame = get_dates_yearly(df)
    for i in range(Date_Frame.shape[0]):
        start_date_temp = Date_Frame.iloc[i,0]
        end_date_temp = Date_Frame.iloc[i,1]
        period = str(start_date_temp.strftime('%Y %b')) + "-" + str(end_date_temp.strftime('%Y %b'))
        for index in df.columns:
            df_temp = df[index]
            actual_returns.loc[index,f'{period}'] = (df_temp.loc[end_date_temp]/df_temp.loc[start_date_temp])-1 
    actual_returns = actual_returns.T
    actual_returns = actual_returns.sort_index()
    return actual_returns


def calculate_div(df):
    div_yield = pd.DataFrame()
    columns = df.shape[1]
    group_size = 3
    column_groups = [(start, min(start + group_size - 1, columns - 1)) for start in range(0, columns, group_size)]
    # column_groups = [(0, 2), (3, 5), (6, 8)] #example
    for start, end in column_groups:
        df_temp = df[df.columns[start:end+1]]
        for index in df_temp.columns:
            div_yield[index] = df_temp[index] - df_temp.iloc[:,0]
    return div_yield 



# Function to calculate the annualized returns and fluctuations
def calculate_annualized_perf(df,periods=[1, 3, 5]):
    """
    Calculate annualized returns for specified periods.

    Parameters:
    - df: DataFrame containing historical daily returns for three securities.
    - periods: List of periods for which to calculate annualized returns (default: [1, 3, 5]).

    Returns:
    - DataFrame with annualized returns for each specified period.
    """

    # Calculate daily returns
    daily_returns = df.pct_change()

    # Calculate annualized returns for specified periods
    annualized_returns = pd.DataFrame()
    annualized_vola =  pd.DataFrame()
    annualized_returns['Return Overall(ann.)'] = (df.iloc[-1]/df.iloc[0])**(260/df.shape[0])-1
    annualized_vola['Volatility overall(ann.)'] = daily_returns.std()*(260**0.5)
    latest_date = df.index[-1]  
    year = latest_date.year
    month = latest_date.month
    end_date = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
    for period in periods:
        calc_date = end_date - timedelta(days = (period*365+1))
        if calc_date.month!=latest_date.month:
            calc_date -= timedelta(days=1)
        while calc_date.weekday() >= 5:  # 5 and 6 represent Saturday and Sunday
            calc_date -= timedelta(days=1)
        # if calc_date.weekday() == 5: 
        #     calc_date -= timedelta(days=1)
        if calc_date.month!=latest_date.month:
            calc_date -= timedelta(days=1)
        for index in df.columns:
            df_temp = df[index]
            # Calculate daily returns
            daily_returns_temp = df_temp.dropna().pct_change()
            shape =df_temp.loc[(df_temp.index >= calc_date ) & (df_temp.index <= latest_date)].dropna().shape[0]
            try: 
                annualized_returns.loc[index,f'Return {period}Y(ann.)'] = (df_temp.loc[latest_date]/df_temp.loc[calc_date])**(260/shape)-1  #annualized returns 
            except:
                calc_date -= timedelta(days=1)
                shape =df_temp.loc[(df_temp.index >= calc_date ) & (df_temp.index <= latest_date)].dropna().shape[0]-1
                annualized_returns.loc[index,f'Return {period}Y(ann.)'] = (df_temp.loc[latest_date]/df_temp.loc[calc_date])**(260/shape)  #annualized returns 
            # annualized_returns[f'Return {period}Y'] = (df.loc[latest_date]/df.loc[calc_date])-1   #total returns 
            annualized_vola.loc[index,f'Volatility {period}Y(ann.)'] = daily_returns_temp.loc[calc_date:latest_date].std(ddof=2)*(260**0.5)
        annualized_perf = pd.concat([annualized_returns,annualized_vola],axis=1).T
    return annualized_perf

def calculate_corr(df):
    daily_returns = df.dropna().pct_change()
    correlation_matrix = daily_returns.corr()
    return correlation_matrix

def get_matrix_main(web): 

    '''
    Calculated Historical Performance, Correlations
    Get Sector Allocation, PE, P/B Ratio from STOXX Website

    Export report in Excel format

    '''
    # Historical Performance & Corrlation table
    perf_df_all = historical_performance
    perf_df_all.iloc[:,0] = pd.to_datetime(perf_df_all.iloc[:,0])
    perf_df_all = perf_df_all.set_index(perf_df_all.iloc[:,0])
    perf_df_all = perf_df_all.drop(perf_df_all.columns[0], axis=1)
    # perf_df_all = perf_df_all.loc[:'2024-01-31',:] # FOR TEST
    # perf_df = perf_df_all[perf_df_all.columns[perf_df_all.columns.str.contains('Return',case = False)]]
    # perf_df = perf_df.replace('%', '', regex=True).apply(pd.to_numeric, errors='coerce')
    # price_df = perf_df_all.loc[:,~perf_df_all.columns.str.contains('Return',case = False)]
    price_df = perf_df_all
    # price_df.columns = price_df.columns.str.replace(' Value', '')
    annualized_perf = calculate_annualized_perf(price_df, periods=[1,3,5])
    actual_perf = calculate_yearly_perf(price_df,EOY=True)
    div_yield = calculate_div(actual_perf)
    correlation_matrix = calculate_corr(price_df)

    if web == True:
    # Sector Allocation & Fundamentals
        sec_allocation = pd.DataFrame()
        descriptive = pd.DataFrame()
        fundamentals = pd.DataFrame()
        for i in range(Input.shape[0]):
            time.sleep(3)
            index_name = Input.iloc[i,0] 
            url = Input.iloc[i,2]
            req = requests.get(url)
            soup = BeautifulSoup(req.text, 'html')
            sec_allocation_temp, descriptive_temp, fundamentals_temp = get_from_website(soup,index_name)
            sec_allocation_temp = sec_allocation_temp.set_index(sec_allocation_temp.columns[0])
            sec_allocation = pd.concat([sec_allocation,sec_allocation_temp],axis=1)
            sec_allocation = sec_allocation.fillna(0)
            descriptive = pd.concat([descriptive,descriptive_temp],axis = 0)
            descriptive = descriptive.drop_duplicates()
            fundamentals = pd.concat([fundamentals,fundamentals_temp],axis=0)
            fundamentals = fundamentals.drop_duplicates()

        sec_allocation = sec_allocation.apply(pd.to_numeric, errors='coerce')
        sec_allocation = sec_allocation.applymap(lambda x: f"{x:.1f}%" if x != 0 else "0%")

    # Export
    with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
        # Save each DataFrame to a different sheet
        price_df.to_excel(writer, sheet_name='Index Price', index=True)
        annualized_perf.to_excel(writer, sheet_name='Annualized Performance', index=True)
        # actual_perf.to_excel(writer,sheet_name='Yearly_Returns', index=True)
        div_yield.to_excel(writer, sheet_name='Dividend Yield', index=True)
        correlation_matrix.to_excel(writer, sheet_name='correlation_matrix', index=True)
        if web == True:
            descriptive.to_excel(writer, sheet_name='Descriptive Statistics', index=True)
            sec_allocation.to_excel(writer, sheet_name='Sector Allocation', index=True)
            fundamentals.to_excel(writer, sheet_name='Fundamentals', index=True)


################################################################################################################ 
                          # Change Input/Output Locations and Parameters here #
################################################################################################################


if __name__ == "__main__":
    ## Define the parameters
    # Input input Format: columns: Name, Symbol, Index Factsheet url 
    # Input = pd.read_excel(directory+'\Flagship regional indices.xlsx')
    # icb_mapping = pd.read_excel(directory+'\icb-codes-description.xlsx')
    historical_performance = pd.read_csv(directory+'\BT_Archive\BT_v2.csv') # If already have price history 
    benchmarkSymbol = 'SWUSGV' #if needed
    # startDate = '2014-01-31' #analyse window 
    # endDate = '2024-01-31'
    excel_file_path = directory+'\Output\Performance_metrix_v4.xlsx'

    # Output for getting all the information / Annu for getting annualized performance 
    Output = get_matrix_main(web = False)
    print('Analyse Finished!')

