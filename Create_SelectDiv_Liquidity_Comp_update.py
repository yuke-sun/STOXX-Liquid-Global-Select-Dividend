import pandas as pd
import numpy as np
import os

directory = os.path.dirname(os.path.abspath(__file__))
Input_loc = directory+"\\Input\\"
Output_loc = directory+"\\Output\\"

start_date = '2015-02-27'
end_date = '2025-03-24'
Dates_Frame = pd.read_csv(Input_loc +"Dates_Frame_Annual.csv", index_col=0)
Dates_Frame["Review"] = pd.to_datetime(Dates_Frame["Review"])
Dates_Frame["Cutoff"] = pd.to_datetime(Dates_Frame["Cutoff"])
Dates_Frame = Dates_Frame[(Dates_Frame["Review"]>= start_date)&(Dates_Frame["Review"]<= end_date)]

# Get SDGP Composition
Comp_raw = pd.read_csv(Input_loc +"SDGP_Comp_10y.csv", sep =",", parse_dates=["Date"], keep_default_na=False, na_values=["", "null", "N/A"])
Comp_raw = Comp_raw[Comp_raw['Date'].isin(Dates_Frame['Review'])]
Comp_raw = Comp_raw.merge(Dates_Frame, left_on="Date", right_on="Review", how="left").drop(columns={"Review"})
Comp_raw = Comp_raw.dropna(subset=['Cutoff'])

Reviewed_Comp = pd.read_csv(Input_loc + '\\qr_P000_sdgp_20250314.csv', sep = ';',parse_dates = ["Creation_Date"], keep_default_na=False, na_values=["", "null", "N/A"])
Reviewed_Comp = Reviewed_Comp[['Creation_Date','Internal_Key','ISIN','SEDOL','Weights','Close_Price']]
Reviewed_Comp['FX_local_to_Index_Currency'] = 1
Reviewed_Comp['Cutoff'] = pd.to_datetime('2025-02-28')
Reviewed_Comp = Reviewed_Comp.dropna()
Reviewed_Comp.columns = ['Date','Internal_Number','ISIN','SEDOL','Weight','Close_unadjusted_local','FX_local_to_Index_Currency','Cutoff']
Comp_raw = pd.concat([Comp_raw, Reviewed_Comp], axis= 0)


#Option1: Data from SID
# ADTV = pd.read_csv(Input_loc+"Output_ADTV_update.csv", sep =",", parse_dates=["date"])
# ADTV['value'] = ADTV["value"].round(2)
# Comp_raw = pd.read_csv(Input_loc +"SDGP_Comp_10y.csv", sep =",", parse_dates=["Date"])
# Comp_raw = Comp_raw[Comp_raw['Date'].isin(Dates_Frame['Review'])]
# Comp = Comp_raw
# Comp = Comp_raw.merge(Dates_Frame, left_on="Date", right_on="Review", how="left").drop(columns={"Review"})
# Comp = Comp.dropna(subset=['Cutoff'])

# Comp = Comp.merge(ADTV[["value", "stoxx_id", "date"]], left_on=["Cutoff", "Internal_Number"], 
#                         right_on=["date", "stoxx_id"], how="left").drop(columns={"stoxx_id", "date","Unnamed: 0"}).rename(columns={"value": "ADTV_3M_EUR"})
# Comp["Weight"] = Comp["Weight"]/100
# Dates = Comp["Date"].unique()


#Option2: Data from Toolkit
ADTV = pd.read_csv(Input_loc+"Comp&ADTV_TW1P_all.csv", sep =",", parse_dates=["composition_date"],keep_default_na=False, na_values=["", "null", "N/A"])
ADTV['ADTV_3m_EUR'] = ADTV["ADTV_3m_EUR"].round(2)
Comp = Comp_raw.merge(ADTV[["ADTV_3m_EUR", "stoxxid", "composition_date"]], left_on=["Cutoff", "Internal_Number"], 
                        right_on=["composition_date", "stoxxid"], how="left").drop(columns={"stoxxid", "composition_date","Unnamed: 0"}).rename(columns={"ADTV_3m_EUR": "ADTV_3M_EUR"})
Comp["Weight"] = Comp["Weight"]/100


#Liquidity Screens Selection
ADTV_Cap = True

####################################################
######## Method: Weight Cap by ADTV Screen #########
####################################################

# Assign the parameters
nominal_amount = 1000000000
ADTV_Multiple = 2.5


# Do the pre-check
def ADTV_Test(weight):
    Comp["ADTV_Cap_Screen"] = round(ADTV_Multiple * Comp['ADTV_3M_EUR'],3)
    threshold_condition = Comp[weight] * nominal_amount <= Comp["ADTV_Cap_Screen"]
    Comp['Liquidity_Test'] = "No"
    Comp.loc[threshold_condition, 'Liquidity_Test'] = 'PASS'
    if "No" in Comp["Liquidity_Test"].unique():
        print("Not Pass")
    else:
        print("Pass the Liquidity Screen")
    return Comp

Comp = ADTV_Test("Weight")

# Add ADTV Liquidity Screen 
while "No" in Comp["Liquidity_Test"].unique():
    #assign new weights
    Comp["New_Weight"] = Comp["Weight"]
    Comp.loc[Comp["Liquidity_Test"]=="No", 'New_Weight'] = Comp["ADTV_Cap_Screen"]/nominal_amount
    Comp["New_Weight"] = Comp["New_Weight"].astype("float64")
    Comp['New_Weight_Sum'] = Comp.groupby('Date')['New_Weight'].transform('sum')
    Comp["Weight_Sum_for_update"] = Comp[Comp['Liquidity_Test'] == 'PASS'].groupby('Date')['New_Weight'].transform('sum')
    Comp.loc[Comp["Liquidity_Test"]=="PASS", 'New_Weight'] = Comp["Weight"] + (Comp["Weight"]/Comp['Weight_Sum_for_update']) * (1-Comp['New_Weight_Sum'])
    Comp['New_Weight_Sum'] = Comp.groupby('Date')['New_Weight'].transform('sum')
    Comp = Comp.drop("Weight_Sum_for_update",axis =1)
    # Comp["New_Mcap"] = Comp["Index_Mcap_Units"]*Comp["New_Weight"]
    # Comp["New_Mcap"] = Comp["New_Mcap"].astype("float64")
    Comp["New_Weightfactor"] = nominal_amount * Comp["New_Weight"]/(Comp["Close_unadjusted_local"]*Comp["FX_local_to_Index_Currency"])
    Comp["New_Weightfactor"]  = round(Comp["New_Weightfactor"],0).astype(int)
    Comp = ADTV_Test("New_Weight")

# After the loop completes or condition is no longer met
print("Alles gut!")

# Comp.to_csv(Output_loc + "Liquidity_Comp_10y_toolkit_2025.csv")
Comp.to_excel( Output_loc + "\\Liquidity_Comp_10y_toolkit_2025.xlsx")
