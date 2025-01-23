import requests
from requests.auth import HTTPBasicAuth
from io import StringIO
import sys
import xlrd
import getpass
import pathlib
import numpy as np
import pandas as pd
import datetime as dt
from datetime import date
from pandas.tseries.offsets import BMonthEnd
from dateutil.relativedelta import relativedelta
import requests
from urllib3 import exceptions, disable_warnings
from requests.auth import HTTPBasicAuth
import os
import pathlib 

disable_warnings(exceptions.InsecureRequestWarning)
directory = os.path.dirname(os.path.abspath(__file__))

def risk_ret(df,rf,ccy,const):
    print(df)
    df.index = pd.to_datetime(df.index)
    #Dates for Returns
    FirstDay=df.index[0]
    LastDay=df.index[-1]
    ytd=LastDay.year-1
    YTD=df.index[df.index.year == ytd][-1]
    offset = BMonthEnd()
    
    if LastDay==offset.rollforward(LastDay):
        y=LastDay-relativedelta(months=12)
        one_y=df[0][y.strftime('%Y'+'-'+'%m')].index.values[-1]
        y3=LastDay-relativedelta(years=3)
        three_y=df[0][y3.strftime('%Y'+'-'+'%m')].index.values[-1]
        y5=LastDay-relativedelta(years=5)
        five_y=df[0][y5.strftime('%Y'+'-'+'%m')].index.values[-1]          
    else:
        y=LastDay-relativedelta(months=12)
        one_y=df[:y].index[-1]
        y3=LastDay-relativedelta(years=3)
        three_y=df[:y3].index[-1]
        y5=LastDay-relativedelta(years=5)
        five_y=df[:y5].index[-1]
    
    dtlst=[FirstDay,five_y,three_y,one_y,LastDay]
    #[LastDay,one_y,three_y,five_y,FirstDay]
    dtlst=dtlst.copy()

    #Returns (YTD, 1Y, 3Y, 5Y)
    ret_ytd=df.loc[LastDay]/df.loc[YTD] -1
    ret_1y=df.loc[LastDay]/df.loc[one_y] -1
    ret_3y=df.loc[LastDay]/df.loc[three_y] -1
    ret_5y=df.loc[LastDay]/df.loc[five_y] -1
    ret_overall=df.loc[LastDay]/df.loc[FirstDay] -1
    
    ct_1y=len(df[one_y:LastDay])-1
    ct_3y=len(df[three_y:LastDay])-1
    ct_5y=len(df[five_y:LastDay])-1
    ct_all=len(df[:LastDay])-1
    
    ret_1y_ann=(1+ret_1y)**(260/ct_1y)-1
    ret_3y_ann=(1+ret_3y)**(260/ct_3y)-1
    ret_5y_ann=(1+ret_5y)**(260/ct_5y)-1
    ret_all_ann=(1+ret_overall)**(260/ct_all)-1

    Returns=pd.concat([ret_ytd,ret_1y,ret_3y,ret_5y,ret_overall,ret_1y_ann,ret_3y_ann,ret_5y_ann,ret_all_ann], axis = 1)
    Returns.columns = ['YTD Return (actual)','1Y Return (actual)','3Y Return (actual)','5Y Return (actual)',
                                'Overall Return (actual)','1Y Return (annualized)','3Y Return (annualized)','5Y Return (annualized)','Overall Return (annualized)']
    Returns = Returns.T

    #Dates for Volatilities
    #Please note that these dates are different from the ones used for returns.
    one_y=df[one_y:].index[1]
    three_y=df[three_y:].index[1]
    five_y=df[five_y:].index[1]
    #dates=[LastDay,one_y,three_y,five_y,FirstDay]
    dates=[FirstDay,five_y,three_y,one_y,LastDay]
    
    #Volatilities (1Y,3Y,5Y,Overall) #Annualised
    vol_ytd=df.pct_change().dropna()[YTD:LastDay].std()*np.sqrt(260)
    vol_1y=df.pct_change().dropna()[one_y:LastDay].std()*np.sqrt(260)
    vol_3y=df.pct_change().dropna()[three_y:LastDay].std()*np.sqrt(260)
    vol_5y=df.pct_change().dropna()[five_y:LastDay].std()*np.sqrt(260)
    vol_all=df.pct_change().dropna()[:LastDay].std()*np.sqrt(260)

    Volatilities=pd.concat([vol_ytd,vol_1y,vol_3y,vol_5y,vol_all], axis = 1)
    Volatilities.columns = ['YTD Volatility (annualized)','1Y Volatility (annualized)','3Y Volatility (annualized)',
                                   '5Y Volatility (annualized)','Overall Volatility (annualized)']
    Volatilities = Volatilities.T

    Div_yield=Returns.iloc[1:5].copy()
    for i in range(Div_yield.shape[1]):
        if i%3==0:
            pass
        else:
            j=int(i/3)*3
            Div_yield.iloc[:,i]=Div_yield.iloc[:,i]-Div_yield.iloc[:,j]
    for i in range(Div_yield.shape[1]):
        if i%3==0:
            Div_yield.iloc[:,i]=np.nan
        else:
            pass
    Div_yield.index=['1Y Dividend yield','3Y Dividend yield','5Y Dividend yield','Overall Dividend yield']

    div_1y_ann=(1+Div_yield.loc['1Y Dividend yield'])**(260/ct_1y)-1
    div_3y_ann=(1+Div_yield.loc['3Y Dividend yield'])**(260/ct_3y)-1
    div_5y_ann=(1+Div_yield.loc['5Y Dividend yield'])**(260/ct_5y)-1
    div_all_ann=(1+Div_yield.loc['Overall Dividend yield'])**(260/ct_all)-1

    Ann_Div_yield=pd.concat([div_1y_ann,div_3y_ann,div_5y_ann,div_all_ann],axis = 1)
    Ann_Div_yield.columns = ['1Y Dividend yield (annualized)','3Y Dividend yield (annualized)',
                               '5Y Dividend yield (annualized)','Overall Dividend yield (annualized)']
    Ann_Div_yield = Ann_Div_yield.T

    daily_ret=df.pct_change().dropna()
    diff=daily_ret.copy()
    for i in range(df.shape[1]):
        j=i%3+(int(df.shape[1]/3)-1)*3
        diff.iloc[:,i]=diff.iloc[:,i]-diff.iloc[:,j]
    for i in range(diff.shape[1]):
        if i>(df.shape[1]/3)*3-4:
            diff.iloc[:,i]=np.nan
        else:
            pass
    
    #Sharpe Ratio Calculations
    if ccy.upper()=='USD':
        rf=rf_rates['SOFR']
    elif ccy.upper()=='EUR':
        rf=rf_rates['EONIA']
    elif ccy.upper()=='CNY':
        rf=rf_rates['SHIBOR']
    else:
        rf=rf_rates[['Zero']]
    rf=rf.align(df, join='right', axis=0, method='ffill')[0]
    rf=rf.dropna() 
    
    SR=pd.DataFrame()

    for i,date_i in enumerate(dtlst[:-1]):
        ret=daily_ret[dates[i]:]
        rfrt=rf[date_i:LastDay].copy()
        timedelta=pd.DataFrame([(rfrt.iloc[[i+1]].index - rfrt.iloc[[i]].index).days for i in range(len(rfrt)-1)])
        rfrt=rfrt[:LastDay-pd.Timedelta(1,'D')]
        drate= pd.DataFrame(rfrt.values * timedelta.values /365)
        drate=drate[:-1]
        drate=drate[:len(ret)]
        excess_ret= pd.DataFrame(ret.values - drate.values, columns=ret.columns)
        sr= pd.DataFrame(excess_ret.mean(axis = 0)/ np.std(excess_ret))*np.sqrt(260)
        SR=pd.concat([sr.T,SR],axis=0)
    # for i,date_i in enumerate(dtlst[:-1]):
    #     ret=daily_ret[dates[i]:]
    #     rfrt=rf[date_i:LastDay].copy()
    #     timedelta=[(rfrt.index[i+1] - rfrt.index[i]).days for i in range(len(rfrt)-1)]
    #     timedelta.append(pd.NaT)
    #     rfrt["timedelta"] = timedelta
    #     rfrt=rfrt[:LastDay-pd.Timedelta(1,'D')]
    #     rfrt = rfrt[:-1]
    #     drate= pd.DataFrame(rfrt.iloc[:,1] * (rfrt["timedelta"]/365))
    #     # drate=drate[:-1]
    #     excess_ret= pd.DataFrame(ret.values - drate.values, columns=ret.columns)
    #     sr= pd.DataFrame(excess_ret.mean(axis = 0)/ np.std(excess_ret))*np.sqrt(260)
    #     SR=pd.concat([sr.T,SR],axis=0)
        
    
    SR=pd.DataFrame(SR)
    SR=SR.reindex(index=SR.index[::-1])
    SR.index=['1Y Sharpe ratio','3Y Sharpe ratio','5Y Sharpe ratio','Overall Sharpe ratio']
    
    te_1y=diff[one_y:LastDay].std()*np.sqrt(260)
    te_3y=diff[three_y:LastDay].std()*np.sqrt(260)
    te_5y=diff[five_y:LastDay].std()*np.sqrt(260)
    te_all=diff[:LastDay].std()*np.sqrt(260)         
    Tracking_Err=pd.concat([te_1y,te_3y,te_5y,te_all], axis =1)
    Tracking_Err.columns = ['1Y Tracking Error (annualized)','3Y Tracking Error (annualized)',
                                   '5Y Tracking Error (annualized)','Overall Tracking Error (annualized)']
    Tracking_Err = Tracking_Err.T

    for i in range(Tracking_Err.shape[1]):
        if i>(Tracking_Err.shape[1]/3)*3-4:
            Tracking_Err.iloc[:,i]=np.nan
        else:
            pass
       
    #Correlation
    temp=daily_ret[one_y:LastDay].corr(method ='pearson').iloc[df.shape[1]-3:,]
    temp1=pd.DataFrame()
    for i in range (df.shape[1]):
        if i % 3 == 0:
            temp1 = pd.concat((temp1,temp.iloc[:,i:i+3]),axis=0, ignore_index=True,sort=False)
        else:
            pass
    corr_1y=pd.DataFrame(np.diag(temp1), index=temp1.columns).T.iloc[0]
    temp=daily_ret[three_y:LastDay].corr(method ='pearson').iloc[df.shape[1]-3:,]
    temp1=pd.DataFrame()
    for i in range (df.shape[1]):
        if i % 3 == 0:
            temp1=pd.concat([temp1,temp.iloc[:,i:i+3].reset_index(drop=True)],axis=0, ignore_index=True,sort=False)
        else:
            pass
    corr_3y=pd.DataFrame(np.diag(temp1), index=temp1.columns).T.iloc[0]
    temp=daily_ret[five_y:LastDay].corr(method ='pearson').iloc[df.shape[1]-3:,]
    temp1=pd.DataFrame()
    for i in range (df.shape[1]):
        if i % 3 == 0:
            temp1=pd.concat((temp1,temp.iloc[:,i:i+3]),axis=0, ignore_index=True,sort=False)
        else:
            pass
    corr_5y=pd.DataFrame(np.diag(temp1), index=temp1.columns).T.iloc[0]
    temp=daily_ret[:LastDay].corr(method ='pearson').iloc[df.shape[1]-3:,]
    temp1=pd.DataFrame()
    for i in range (df.shape[1]):
        if i % 3 == 0:
            temp1=pd.concat((temp1,temp.iloc[:,i:i+3]),axis=0, ignore_index=True,sort=False)
        else:
            pass
    corr_all=pd.DataFrame(np.diag(temp1), index=temp1.columns).T.iloc[0]    
    
    Correlation=pd.concat([corr_1y,corr_3y,corr_5y,corr_all],axis = 1)
    Correlation.columns = ['1Y Correlation','3Y Correlation','5Y Correlation','Overall Correlation']
    Correlation = Correlation.T

    for i in range(Correlation.shape[1]):
        if i>(Correlation.shape[1]/3)*3-4:
            Correlation.iloc[:,i]=np.nan
        else:
            pass

    beta=[]
    for i in range(df.shape[1]):
        j=i%3+(int(df.shape[1]/3)-1)*3
        temp=daily_ret[one_y:LastDay].copy()
        x=np.cov(temp.iloc[:,i],temp.iloc[:,j])/np.cov(temp.iloc[:,j],temp.iloc[:,j])
        x=x[0][1]
        beta.append(x)
    beta_1y=pd.DataFrame(beta, index=df.columns).T.iloc[0]
    beta=[]
    for i in range(df.shape[1]):
        j=i%3+(int(df.shape[1]/3)-1)*3
        temp=daily_ret[three_y:LastDay].copy()
        x=np.cov(temp.iloc[:,i],temp.iloc[:,j])/np.cov(temp.iloc[:,j],temp.iloc[:,j])
        x=x[0][1]
        beta.append(x)
    beta_3y=pd.DataFrame(beta, index=df.columns).T.iloc[0]
    beta=[]
    for i in range(df.shape[1]):
        j=i%3+(int(df.shape[1]/3)-1)*3
        temp=daily_ret[five_y:LastDay].copy()
        x=np.cov(temp.iloc[:,i],temp.iloc[:,j])/np.cov(temp.iloc[:,j],temp.iloc[:,j])
        x=x[0][1]
        beta.append(x)
    beta_5y=pd.DataFrame(beta, index=df.columns).T.iloc[0]
    beta=[]
    for i in range(df.shape[1]):
        j=i%3+(int(df.shape[1]/3)-1)*3
        temp=daily_ret[:LastDay].copy()
        x=np.cov(temp.iloc[:,i],temp.iloc[:,j])/np.cov(temp.iloc[:,j],temp.iloc[:,j])
        x=x[0][1]
        beta.append(x)
    beta_all=pd.DataFrame(beta, index=df.columns).T.iloc[0]
    Beta=pd.DataFrame([beta_1y,beta_3y,beta_5y,beta_all],\
                             index=['1Y Beta','3Y Beta','5Y Beta','Overall Beta'])
    for i in range(Beta.shape[1]):
        if i>(Beta.shape[1]/3)*3-4:
            Beta.iloc[:,i]=np.nan
        else:
            pass      
    
    diff=daily_ret.copy()
    for i in range(df.shape[1]):
        j=i%3+(int(df.shape[1]/3)-1)*3
        diff.iloc[:,i]=diff.iloc[:,i]-diff.iloc[:,j]
    for i in range(diff.shape[1]):
        if i>(df.shape[1]/3)*3-4:
            diff.iloc[:,i]=np.nan
        else:
            pass    
  
    Div_yield=Returns.iloc[1:5].copy()
    for i in range(Div_yield.shape[1]):
        if i%3==0:
            pass
        else:
            j=int(i/3)*3
            Div_yield.iloc[:,i]=Div_yield.iloc[:,i]-Div_yield.iloc[:,j]
    for i in range(Div_yield.shape[1]):
        if i%3==0:
            Div_yield.iloc[:,i]=np.nan
        else:
            pass
    Div_yield.index=['1Y Dividend yield','3Y Dividend yield','5Y Dividend yield','Overall Dividend yield']
    
    Max_dd = []
    for d in range(len(dtlst[1:])):
        dft = df[(df.index <= dtlst[4]) & (df.index > dtlst[d])]
        pk = np.zeros((len(dft) + 1,df.shape[1]))
        dd = np.zeros((len(dft) + 1,df.shape[1]))
        h = np.array(dft.iloc[:])
        pk[0] = h[0]
        for i in range(len(h)):
            for j in range(df.shape[1]):
                pk[i + 1, j] = h[i, j] if h[i, j] > pk[i, j] else pk[i, j]
                dd[i + 1, j] = h[i, j] / pk[i + 1, j] - 1 if h[i, j] < pk[i + 1, j] else 0
        dd = dd[1:]
        
        Max_dd.append((abs(dd.min(axis=0))))
    Max_dd=pd.DataFrame(Max_dd)
    Max_dd.index=['Overall Maximum drawdown','5Y Maximum drawdown','3Y Maximum drawdown','1Y Maximum drawdown']
    Max_dd.columns=df.columns
    Max_dd=Max_dd.reindex(index=Max_dd.index[::-1])
    
    ppt=pd.concat([Returns, Volatilities, Div_yield, Ann_Div_yield, SR, Tracking_Err, Correlation, Beta, Max_dd])
    ppt.loc['Number of constituents']=np.nan
    for i,col in enumerate (list(df.columns)):
        try:
            ppt.loc['Number of constituents'][col]=const[int(i/3)]
        except:
            pass
        
    base_date=first_day(df,FirstDay).strftime("%Y-%#m-%#d")
    rebased=rebase(df.copy(), base_date, base_value)
    #rebased=zip(rebased[:len(sheet_names)],rebased[len(sheet_names):2*len(sheet_names)], rebased[2*len(sheet_names):3*len(sheet_names)])
    
    return (rebased, ppt, dates)