import pandas as pd
from copy import deepcopy

if __name__ == '__main__':
    file_path = r"E:\workspace\pythonProject\Price_Survey\recycle_data\20241118\2024-11调研任务-住宅_秦琳_2024_11_18(未完成).xlsx"
    df = pd.read_excel(file_path)
    copy_df = deepcopy(df)
    copy_df.loc[:, '月份'] = '2024-10'
    print(copy_df.head())
