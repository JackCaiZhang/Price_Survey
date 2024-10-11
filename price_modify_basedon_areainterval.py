import os
import warnings

import numpy as np
import pandas as pd


warnings.filterwarnings('ignore')


def price_modify(price_df: pd.DataFrame, project_name: str, property_type: str,
                 month_date: str, build_area: float, org_price: float) -> float:
    price_df['月度时间'] = price_df['月度时间'].astype('datetime64[ns]').dt.strftime('%Y-%m')
    price: float = org_price
    month_date = pd.to_datetime(month_date).strftime('%Y-%m')
    for row in price_df.itertuples(index=False):
        condition1 = (project_name == row.总期名称)
        condition2 = (property_type == row.物业类型)
        condition3 = (month_date == row.月度时间)
        condition4 = (row.面积下限 <= round(build_area) <= row.面积上限)
        if condition1 and condition2 and condition3 and condition4:
            price = row.价格 * np.random.uniform(0.997, 1.003)
            break

    return price


if __name__ == '__main__':
    file_path = r"C:\Users\LangLang\Downloads\广州项目"
    price_path = r"D:\BaiduNetdiskWorkspace\周月度数据入库\广州\广州配置表.xlsx"
    price_df = pd.read_excel(price_path, engine='openpyxl', sheet_name='价格-按面积段')

    result_df: pd.DataFrame = pd.DataFrame()
    for file_name in os.listdir(file_path):
        df = pd.read_excel(os.path.join(file_path, file_name), engine='openpyxl')
        result_df = pd.concat([result_df, df], ignore_index=True)
    result_df = result_df.reset_index(drop=True)
    result_df['均价/单价（元/m²）'] = result_df.apply(
        lambda row: price_modify(price_df, row['项目总期名称'], row['细分物业类型'],
                                 row['数据时间'], row['建筑面积（m²）'], row['均价/单价（元/m²）']), axis=1)
    result_df['总价（元）'] = result_df.apply(lambda x: x['建筑面积（m²）'] * x['均价/单价（元/m²）'], axis=1)
    convert_dtypes = {
        "数据时间": "datetime64[ns]",
        "套数": int,
        "建筑总层数": int,
        "建筑面积（m²）": float,
        "总价（元）": float,
        "均价/单价（元/m²）": float,
        "原始均价/单价（元/m²）": float,
        "预售许可证名称": "str",
    }
    result_df = result_df.astype(convert_dtypes)
    result_df['预售许可证名称'] = result_df['预售许可证名称'].apply(lambda x: x.replace('.0', ''))
    # 保留两位小数，不足的补0
    decimal_cols = ["建筑面积（m²）", "总价（元）", "均价/单价（元/m²）"]
    result_df[decimal_cols] = result_df[decimal_cols].round(2)
    result_df["数据时间"] = result_df["数据时间"].apply(lambda x: x.strftime("%Y-%m-%d"))
    result_df['原始价格类型'] = '备案价'
    result_df.to_excel(r"C:\Users\LangLang\Downloads\广州项目-价格修改.xlsx", sheet_name='上传', index=False)
