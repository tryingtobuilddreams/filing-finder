import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta 
import pandas_datareader.data as web

API_KEY = '4WX6M2G38PV0YISE'
# TODO make the start and end date functionality
# TODO implement the time horizon functionality

def get_filings(ticker, start_date, end_date, API_KEY):
    # Finds the CIK of the company, CIK is required for more specific page requests
    cik_search = requests.get('https://www.sec.gov/cgi-bin/browse-edgar?CIK=' + ticker + '&owner=exclude&action=getcompany')
    cik_search_content = cik_search.content
    cik_soup = BeautifulSoup(cik_search_content, 'html.parser')
    CIK_text = ''
    CIK_list = []
    CIK = ''
    for link in cik_soup.find_all(attrs={'class':'companyName'}):
        CIK_text = link.text
        CIK_list = CIK_text.split()
        CIK = CIK_list[3]
    # Gets the quarterly filings dataframe
    df_q_filings_page = pd.read_html('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=' + CIK + '&type=10-Q&dateb=&owner=exclude&count=100')
    # Gets the annual filings dataframe
    df_a_filings_page = pd.read_html('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=' + CIK + '&type=10-K&dateb=&owner=exclude&count=100')

    # Gets the quarterly filing dates
    df_q_filings = df_q_filings_page[2]
    df_q_filing_dates = pd.DataFrame(df_q_filings['Filing Date'])
    # Gets the annual filing dates
    df_a_filings = df_a_filings_page[2]
    df_a_filing_dates = pd.DataFrame(df_a_filings['Filing Date'])

    # Merges the quarterly and annual filing dates dataframes
    frames = [df_q_filing_dates, df_a_filing_dates]
    df_combined = pd.concat(frames)
    # Sorts the dataframe
    df_sorted = df_combined.sort_values(by=['Filing Date'])
    # Subsets the data to only include those between the start and end dates, creates dataframe with dates as datetime objects
    mask = (df_sorted['Filing Date'] > start_date) & (df_sorted['Filing Date'] <= end_date)
    df_between_dates = df_sorted.loc[mask]
    lst_filing_dates = [] 
    for row in df_between_dates['Filing Date']:
        new_val = datetime.strptime(row, '%Y-%m-%d')
        lst_filing_dates.append(new_val)
    df_between_dates['Dates'] = lst_filing_dates

    # Retrieves the openining prices between the start and end date, creates dataframe with dates as datetime objects
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    end_date_45 = end_date_dt + timedelta(days=120) # loads extra price data to account for the time horizon
    query_sec_price = web.DataReader(ticker, 'av-daily', start=start_date, end=end_date_45, api_key=API_KEY)
    sec_price = pd.DataFrame(data=query_sec_price['open'], index=query_sec_price.index)
    lst_sec_price_dates = []
    for row in sec_price.index:
        new_val = datetime.strptime(row, '%Y-%m-%d')
        lst_sec_price_dates.append(new_val)
    sec_price['Dates'] = lst_sec_price_dates

    # Finds the opening price the day after the SEC filing date
    filings = []
    price_days = []
    p_indexes = []
    for row in df_between_dates['Dates']:
        filings.append(row)
    for row in sec_price['Dates']:
        price_days.append(row)

    dates_of_interest = []
    for f in filings:
        f_idx = filings.index(f)
        for p in price_days:
            p_idx = price_days.index(p)
            if p > f:
                #too_far = p_idx - 1
                doi = price_days[p_idx]
                p_indexes.append(p_idx)
                dates_of_interest.append(doi)
                break
            
    # Extracts prices for SEC+1 (SEC1)
    df_master = sec_price[sec_price['Dates'].isin(dates_of_interest)]

    # Extracts prices for SEC+45 (SEC45) - 45 trading days after the post sec filing date
    sec45_indexes = [45 + val for val in p_indexes] # list of indexes for the target dates

    # Finds the price for each date of interest indexes (given by p_indexes)
    sec_45_prices = [] 

    # sec_price is a dataframe with columns [open, dates], indexed by dates
    lst_sec_45 = [] # all opening prices
    for row in sec_price['open']:
        lst_sec_45.append(row)

    # call list items by indexes given in sec_indexes
    for val in sec45_indexes:
        sec_45_prices.append(lst_sec_45[val])
    # convert that to a dataframe 
    df_master['TH_Prices'] = sec_45_prices

    # Computes returns between SEC45 and SEC1, adding them to a master dataframe. 
    df_master['TH_Return'] = (df_master['TH_Prices'] - df_master['open']) / df_master['open']
    df_master.to_csv(path_or_buf='C:/Users/User/Documents/'+ticker+'.csv')

    return df_master


get_filings('TSLA', '2011-01-01', '2019-10-01', API_KEY)