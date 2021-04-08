
import pandas as pd

def join_files(excel_file, excel_sheets, target_dim, target_dim_replace, encoding=None):
    # files
    temp = pd.DataFrame()
    for ele in excel_sheets:
        df = pd.read_excel(excel_file, sheet_name=ele)
        df.columns = df.columns.str.lower()
        df.rename(columns={target_dim: target_dim_replace}, inplace=True)
        temp = temp.append(df, sort=False)
    return temp