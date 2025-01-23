import os
import pandas as pd

# Specify the directory containing the CSV files
directory = os.path.dirname(os.path.abspath(__file__))

file = "//Output//Liquidity_Comp.csv"
filepath = directory + file

# Read the CSV file into a DataFrame
Input = pd.read_csv(filepath, index_col=0)

# Pick only the columns for iStudio Input
Input = Input[["Internal_Number", "SEDOL", "ISIN", "Date", "New_Weightfactor", "Capfactor"]]

Input["Capfactor"] = 1
# Format date
Input["Date"] = pd.to_datetime(Input["Date"]).dt.strftime('%Y-%m-%d')
Input["Date"] = Input["Date"].str.replace("-", "")
Input['Internal_Number'] = ""
Input['SEDOL'] = ""

# Rename Columns
Input = Input.rename(columns={"Internal_Number": "STOXXID", "SEDOL": "SEDOL", "Date": "effectiveDate", 
                            "New_Weightfactor": "weightFactor", "Capfactor": "capFactor"})

# Save the result
# Input.to_csv(os.path.dirname(directory) + "//Output//IStudio_SD3P_daily.csv", index=False, line_terminator='\n', sep=";")
Input.to_csv( directory + "//Output//iStudioInput.csv", index=False, sep=";")
