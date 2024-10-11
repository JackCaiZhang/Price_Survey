import os

import pandas as pd


class PriceReview:
    def __init__(self, file_dir: str, save_path: str, abnormal_projects_path: str):
        self.file_dir = file_dir
        self.save_path = save_path
        self.abnormal_projects_path = abnormal_projects_path

    def merge_data(self) -> pd.DataFrame:
        abnormal_projects_df: pd.DataFrame = pd.read_excel(self.abnormal_projects_path, sheet_name='异常项目')
        admitted_price_dict: dict = abnormal_projects_df.set_index('unique_field')['admitted_price'].to_dict()
        file_names: list[str] = os.listdir(self.file_dir)
        result_df: pd.DataFrame = pd.DataFrame()
        for file_name in file_names:
            df: pd.DataFrame = pd.read_excel(os.path.join(self.file_dir, file_name))
            result_df = pd.concat([result_df, df])
        result_df = result_df.reset_index(drop=True)
        result_df['初调价格'] = result_df.apply(lambda x: x['精装价格'] if pd.notnull(x['精装价格']) else x['毛坯价格'], axis=1)
        result_df['采信价格'] = result_df['唯一键'].apply(lambda x: admitted_price_dict.get(x))
        result_df['[复核-初调]价格'] = result_df.apply(
            lambda x: x['复核价格']-x['初调价格'] if pd.notnull(x['复核价格']) else None, axis=1)
        result_df['[复核-采信]价格'] = result_df.apply(
            lambda x: x['复核价格']-x['采信价格'] if pd.notnull(x['复核价格']) else None, axis=1)
        result_df.to_excel(os.path.join(self.save_path, f'{os.path.basename(self.file_dir)}.xlsx'), index=False)

        return result_df


if __name__ == '__main__':
    file_dir: str = r'D:\BaiduNetdiskWorkspace\周月度数据入库\专项工作\价格复核\20240826复核结果'
    save_path: str = r'D:\BaiduNetdiskWorkspace\周月度数据入库\专项工作\价格复核'
    abnormal_projects_path: str = r"D:\BaiduNetdiskWorkspace\周月度数据入库\专项工作\价格复核\2024-08价格异常+复核项目.xlsx"
    pr = PriceReview(file_dir, save_path, abnormal_projects_path)
    df = pr.merge_data()