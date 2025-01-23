import os
import pandas as pd

# Specify the directory containing the CSV files
directory = os.path.dirname(os.path.abspath(__file__))

file = "//Output//Liquidity_Comp_v2_WeightFactorCheck.csv"
filepath = directory + file

# Read the CSV file into a DataFrame
Input = pd.read_csv(filepath, index_col=0)

# Pick only the columns for iStudio Input
Input = Input[["Internal_Number", "SEDOL", "ISIN", "Date", "Weightfactor_calc", "Capfactor"]]

# Set Weightfactor equal to 1
# Input["Weightfactor"] = 1
# Input["Capfactor"] = 0.15

# Format date
Input["Date"] = pd.to_datetime(Input["Date"]).dt.strftime('%Y-%m-%d')
Input["Date"] = Input["Date"].str.replace("-", "")
Input['Internal_Number'] = ""
Input['SEDOL'] = ""
# Input['Internal_Number'] = Input['Internal_Number'].apply(lambda x: x.zfill(6))
# Input['Internal_Number'] = Input['Internal_Number'].replace('000EG2', 'EG2')

# Rename Columns
Input = Input.rename(columns={"Internal_Number": "STOXXID", "SEDOL": "SEDOL", "Date": "effectiveDate", 
                            "Weightfactor_calc": "weightFactor", "Capfactor": "capFactor"})

# Save the result
# Input.to_csv(os.path.dirname(directory) + "//Output//IStudio_SD3P_daily.csv", index=False, line_terminator='\n', sep=";")
Input.to_csv( directory + "//Output//iStudioInput_Liquid_SDGP_v2.csv", index=False, sep=";")



## Loop through each file in the directory


# for filename in os.listdir(directory):
#     if filename.endswith(".csv"):

#         # Construct the full path to the CSV file
#         filepath = os.path.join(directory, filename)
        
#         # Read the CSV file into a DataFrame
#         Input = pd.read_csv(filepath, index_col=0)

#         # Pick only the columns for iStudio Input
#         Input = Input[["Internal_Number", "SEDOL", "ISIN", "Date", "Weightfactor", "Capfactor"]]

#         # Set Weightfactor equal to 1
#         Input["Weightfactor"] = 1

#         # Format date
#         Input["Date"] = Input["Date"].str.replace("-", "")

#         # Rename Columns
#         Input = Input.rename(columns={"Internal_Number": "STOXXID", "SEDOL": "SEDOL", "Date": "effectiveDate", 
#                                     "Weightfactor": "weightFactor", "Capfactor": "capFactor"})

#         # Save the result
#         Input.to_csv(directory + "\iStudio_VN.csv", index=False, line_terminator="\n", sep=";")