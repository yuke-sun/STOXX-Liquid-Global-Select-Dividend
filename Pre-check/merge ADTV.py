import pandas as pd
import os

directory = os.path.dirname(os.path.abspath(__file__))

ADTV = pd.read_csv(directory+"\ADTV_EUR.csv", sep =",", parse_dates=["date"])
Comp = pd.read_csv(directory+"\SDGP_SID_Comp.csv", sep =",", parse_dates=["Date"])
Dates_Frame = pd.read_csv(directory + "\\Dates_Frame_cutoff_19-24.csv",index_col=0, parse_dates=["Review", "Cutoff"])

Comp_new = Comp.merge(Dates_Frame, left_on="Date", right_on="Review", how="left").drop(columns={"Review"})

Comp_new = Comp_new.merge(ADTV[["value", "stoxx_id", "date"]], left_on=["Cutoff", "Internal_Number"], 
                        right_on=["date", "stoxx_id"], how="left").drop(columns={"stoxx_id", "date"}).rename(columns={"value": "ADTV_3M_EUR"})
Comp_new.to_csv(directory+'\\SDGP_Components.csv')