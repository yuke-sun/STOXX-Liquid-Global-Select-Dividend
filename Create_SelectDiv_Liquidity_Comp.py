import pandas as pd
import numpy as np
import os

directory = os.path.dirname(os.path.abspath(__file__))
Input_loc = directory+"\\Input\\"
Output_loc = directory+"\\Output\\"

start_date = '2019-03-18'
end_date = '2024-03-18'

#Input Files
ADTV = pd.read_csv(Input_loc+"ADTV_EUR.csv", sep =",", parse_dates=["date"])
Comp_raw = pd.read_csv(Input_loc +"SDGP_SID_Comp_6y.csv", sep =",", parse_dates=["Date"])
Comp_raw = Comp_raw[Comp_raw['Date']>= '2019-02-28']

Dates_Frame = pd.read_csv(Input_loc +"Dates_Frame_Annual.csv", index_col=0)
Dates_Frame["Review"] = pd.to_datetime(Dates_Frame["Review"])
Dates_Frame["Cutoff"] = pd.to_datetime(Dates_Frame["Cutoff"])
Dates_Frame = Dates_Frame[(Dates_Frame["Review"]>= start_date)&(Dates_Frame["Review"]<= end_date)]
Comp = Comp_raw.merge(Dates_Frame, left_on="Date", right_on="Review", how="left").drop(columns={"Review"})
Comp = Comp.dropna(subset=['Cutoff'])

Comp = Comp.merge(ADTV[["value", "stoxx_id", "date"]], left_on=["Cutoff", "Internal_Number"], 
                        right_on=["date", "stoxx_id"], how="left").drop(columns={"stoxx_id", "date","Unnamed: 0"}).rename(columns={"value": "ADTV_3M_EUR"})
Comp["Weight"] = Comp["Weight"]/100
Dates = Comp["Date"].unique()


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
    Comp["New_Mcap"] = Comp["Index_Mcap_Units"]*Comp["New_Weight"]
    Comp["New_Mcap"] = Comp["New_Mcap"].astype("float64")
    Comp["New_Weightfactor"] = nominal_amount * Comp["New_Weight"]/(Comp["Close_unadjusted_local"]*Comp["FX_local_to_Index_Currency"])
    Comp["New_Weightfactor"]  = round(Comp["New_Weightfactor"],0).astype(int)
    Comp = ADTV_Test("New_Weight")

# After the loop completes or condition is no longer met
print("Alles gut!")

Comp.to_csv(Output_loc + "Liquidity_Comp.csv")

