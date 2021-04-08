import pandas as pd
import xlrd

def join_files(excel_file, excel_sheets, target_dim, target_dim_replace, encoding=None):
    # files
    temp = pd.DataFrame()
    for ele in excel_sheets:
        wb = xlrd.open_workbook(excel_file, encoding_override=encoding)
        df = pd.read_excel(wb, sheet_name=ele)
        df.columns = df.columns.str.lower()
        df.rename(columns={target_dim: target_dim_replace}, inplace=True)
        temp = temp.append(df, sort=False)
    return temp