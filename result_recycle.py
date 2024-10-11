import datetime
import os
import sys
import warnings

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy.sql import text

from DBOperration import DBOperation


warnings.filterwarnings(action="ignore")


def price_processing(group):
    if len(group) > 1:
        # adopt_price = group['调研价格'].min()
        # price_diff = abs(group['调研价格'].iloc[0] - group['调研价格'].iloc[1])
        # price_diff_pct = round(price_diff / adopt_price * 100, 2)
        # is_abnormal = 1 if price_diff_pct > 5 else 0
        min_price_idx = group['调研价格'].idxmin()
        max_price_idx = group['调研价格'].idxmax()
        min_price = group.loc[min_price_idx, '调研价格']
        max_price = group.loc[max_price_idx, '调研价格']
        if min_price == max_price:
            group['采信价格'] = min_price
            group['价格差异'] = 0.00
            group['价格差异百分比'] = 0.00
            group['是否异常'] = 0
        else:
            price_diff = max_price - min_price
            price_diff_pct = round(price_diff / min_price * 100, 2)
            is_abnormal = 1 if price_diff_pct > 5 else 0

            # 最低价所在行
            group.at[min_price_idx, '采信价格'] = min_price
            group.at[min_price_idx, '价格差异'] = 0.00
            group.at[min_price_idx, '价格差异百分比'] = 0.00
            group.at[min_price_idx, '是否异常'] = 0

            # 最高价所在行
            group.at[max_price_idx, '采信价格'] = min_price
            group.at[max_price_idx, '价格差异'] = price_diff
            group.at[max_price_idx, '价格差异百分比'] = price_diff_pct
            group.at[max_price_idx, '是否异常'] = is_abnormal

    return group


class ResultRecycle:
    recycle_std_columns = ['城市ID', '城市', '月份', '项目id', '项目名称', '项目别名', 'newcode', '物业类型',
                           '城市排名', '精装价格(元/平方米)', '毛坯价格(元/平方米)', '主力在售装修情况',
                           '当月价格较上月变动（%）', '优惠情况', '销售状态', '备注', '负责人', '回收日期',
                           '调研方式', '是否交叉调研', '数据部门', '价格差异', '价格差异百分比', '是否异常',
                           '采信价格', '导入时间', '唯一键字段']
    recycle_db_columns = ['city_id', 'city', 'data_month', 'project_id', 'project_name', 'newhouse_name',
                          'newcode', 'property_type', 'city_ranking', 'decoration_price', 'rough_price',
                          'decoration_of_main_sale', 'chain_ratio', 'discount', 'sale_status', 'comment',
                          'person_in_charge', 'return_date', 'survey_method', 'is_cross', 'data_dept',
                          'price_difference', 'price_diff_ratio', 'is_abnormal', 'admitted_price',
                          'import_date', 'unique_field']
    price_dict_columns = ['city', 'city_id', 'sProperty_name', 'sProperty_id', 'app_date', 'property_type_new',
                          'admitted_price', 'price_source', 'modify_type', 'comment', 'import_date']
    city_ranking = pd.read_excel('城市排名.xlsx')
    dbo = DBOperation()

    def __init__(self, data_path: str, out_path: str, dept_flag: int = 1):
        self.data_path = data_path
        self.out_path = out_path
        self.dept_flag = dept_flag

    def merge_data(self) -> pd.DataFrame:
        """
        回收结果合并
        """
        result_df = pd.DataFrame()
        for file_name in os.listdir(self.data_path):
            file_path = os.path.join(self.data_path, file_name)
            data_df = pd.read_excel(file_path)
            data_df['主力在售装修情况'] = data_df['主力在售装修情况'].str.strip()
            data_df['销售状态'] = data_df['销售状态'].astype('str')
            data_df['销售状态'] = data_df['销售状态'].apply(lambda x: x.strip())
            result_df = pd.concat([result_df, data_df], ignore_index=True)
        result_df = result_df.reset_index(drop=True)
        data_date = datetime.date.today().strftime('%Y-%m-%d')
        result_df.to_excel(os.path.join(self.out_path, f'{data_date}_原始回收结果汇总.xlsx'), index=False)

        return result_df

    def basic_info_overview(self) -> None:
        """
        基础信息概览：检测关键字段基本信息
        """
        for file_name in os.listdir(self.data_path):
            file_path = os.path.join(self.data_path, file_name)
            df = pd.read_excel(file_path)
            df['主力在售装修情况'] = df['主力在售装修情况'].str.strip()
            df['销售状态'] = df['销售状态'].astype('str')
            df['销售状态'] = df['销售状态'].apply(lambda x: x.strip())
            print("=" * 34, f"{df.loc[0, '负责人']}-结果概览", "=" * 34)
            print(f"主力在售装修情况：{'、'.join(str(item) for item in df['主力在售装修情况'].unique().tolist())}")
            decoration_min_price = df['精装价格(元/平方米)'].min()
            decoration_max_price = df['精装价格(元/平方米)'].max()
            rough_min_price = df['毛坯价格(元/平方米)'].min()
            rough_max_price = df['毛坯价格(元/平方米)'].max()
            print(f"精装价格最小最大值：{decoration_min_price}，{decoration_max_price}；"
                  f"最高价格所在城市：{set(df['城市'][df['精装价格(元/平方米)'] == decoration_max_price]).pop()}")
            print(f"毛坯价格最小最大值：{rough_min_price}，{rough_max_price}；"
                  f"最高价格所在城市：{set(df['城市'][df['毛坯价格(元/平方米)'] == rough_max_price]).pop()}")
            print(f"精装缺价格条数：{df['精装价格(元/平方米)'][df['主力在售装修情况'] == '精装'].isnull().sum()}；"
                  f"毛坯缺价格条数：{df['毛坯价格(元/平方米)'][df['主力在售装修情况'] == '毛坯'].isnull().sum()}")
            sale_status_values = df['销售状态'].value_counts().reset_index().to_dict(orient='list')
            sale_status_values = {sale_status: count for sale_status, count
                                  in zip(sale_status_values['销售状态'], sale_status_values['count'])}
            print(f"销售状态数据情况：", end='')
            for key, value in sale_status_values.items():
                print(f'{key}: {value}', end='\t')
            print()
            print('=' * 80)

    def data_preprocessing(self) -> pd.DataFrame:
        """
        数据预处理：回收结果合并、添加必要字段、列顺序重排列、交叉调研项目处理
        """
        data_df: pd.DataFrame = self.merge_data()
        # 添加主键字段
        if self.dept_flag == 1:
            data_df['唯一键字段'] = data_df.apply(
                lambda x: ''.join([x['城市ID'], x['月份'], x['项目id'], x['物业类型'], x['负责人']]), axis=1)
        else:
            data_df['唯一键字段'] = data_df.apply(
                lambda x: ''.join([x['城市ID'], x['月份'], str(x['newcode']), x['物业类型'], x['负责人']]), axis=1)

        data_dept: str = '住宅' if self.dept_flag == 1 else '指数'
        # 添加城市排名
        city_ranking_dict: dict = self.city_ranking.set_index('城市')['排名'].to_dict()
        data_df['城市排名'] = data_df['城市'].apply(lambda x: city_ranking_dict[x])
        data_df['数据部门'] = data_dept
        data_df['导入时间'] = datetime.date.today()
        data_df.loc[:, '当月价格较上月变动（%）'] = data_df['当月价格较上月变动（%）'].apply(
            lambda x: float(str(x).replace('%', '')) if pd.notnull(x) else x)
        add_columns = ['价格差异', '价格差异百分比', '是否异常', '采信价格']
        data_df[add_columns] = None

        # 从数据库查询交叉调研项目，给回收结果打上是否交叉调研标记
        engine = self.dbo.get_engine(server='local')
        distribut_date: str = data_df.loc[0, '月份']
        data_date = datetime.date(year=int(distribut_date.split('-')[0]),
                                  month=int(distribut_date.split('-')[1]),
                                  day=1) + relativedelta(months=-1)
        data_month = '-'.join([str(data_date.year), str(data_date.month).zfill(2)])
        price_month: str = distribut_date
        if self.dept_flag == 1:
            query_stmt: str = f"""
                    SELECT city_id, city, data_month, project_id, project_name, newhouse_name,
                           newcode, property_type, city_ranking, decoration_price, rough_price,
                           decoration_of_main_sale, chain_ratio, discount, sale_status, comment,
                           person_in_charge, return_date, survey_method, is_cross, data_dept,
                           price_difference, price_diff_ratio, is_abnormal, admitted_price, 
                           import_date, unique_field
                    FROM dbo.result_recycle
                    WHERE data_dept = '{data_dept}'
                      AND data_month = '{price_month}'
                      AND is_cross = 1
                      AND (decoration_price IS NOT NULL OR rough_price IS NOT NULL)
                      AND admitted_price IS NULL 
                    """
            no_admitted_price_df: pd.DataFrame = pd.read_sql(text(query_stmt), engine)

            sql_stmt: str = f"""
            SELECT DISTINCT city_id, project_id, property_type 
            FROM dbo.task_distribution
            WHERE data_dept = '{data_dept}' AND data_month = '{data_month}' AND is_cross = 1
            """
            cross_projects_df: pd.DataFrame = pd.read_sql(text(sql_stmt), engine)
            cross_projects_df.columns = ['城市ID', '项目id', '物业类型']
            cross_projects_df['是否交叉调研'] = 1
            print(f'【{price_month}】月交叉调研项目数量：{cross_projects_df.shape[0] * 2}')

            data_df = data_df.merge(cross_projects_df, on=['城市ID', '项目id', '物业类型'], how='left')
            print(f'本次回收结果中交叉调研项目数量：{data_df[data_df["是否交叉调研"] == 1].shape[0]}')
            data_df['是否交叉调研'] = data_df['是否交叉调研'].apply(lambda x: 0 if pd.isnull(x) else x)

            # 列顺序重新排列
            result_df: pd.DataFrame = pd.DataFrame({col: data_df[col] for col in self.recycle_std_columns})
            if not no_admitted_price_df.empty:
                print('前面批次存在未处理的交叉调研项目！')
                no_admitted_price_df.columns = self.recycle_std_columns
                result_df = pd.concat([result_df, no_admitted_price_df],
                                      ignore_index=True).reset_index(drop=True)

            result_df['是否交叉调研'] = result_df['是否交叉调研'].astype('int')

            # 判断是否存在没有在同一批次回收的交叉调研项目
            cross_projects_df2: pd.DataFrame = result_df[result_df['是否交叉调研'] == 1]
            cross_projects_df2['交叉调研项目是否同一批次回收'] = cross_projects_df2.duplicated(
                subset=['城市ID', '项目id', '物业类型'],
                keep=False).astype(int)

            cross_projets_returned_nosimul_df = cross_projects_df2[
                cross_projects_df2['交叉调研项目是否同一批次回收'] == 0]
            if not cross_projets_returned_nosimul_df.empty:
                print('存在以下不在同一批次回收的交叉调研项目，本次仅入库原始信息，不做任何处理：')
                print(
                    cross_projets_returned_nosimul_df[['城市', '项目名称', '物业类型', '负责人']].drop_duplicates())
                del cross_projets_returned_nosimul_df['交叉调研项目是否同一批次回收']
                cross_projets_returned_nosimul_df.columns = self.recycle_db_columns
                # 删除数据库当月交叉调研项目中有价格但采信价格为空的项目
                delete_stmt = f"""
                DELETE
                FROM dbo.result_recycle
                WHERE data_dept = N'住宅'
                  AND data_month = '{price_month}'
                  AND is_cross = 1
                  AND (decoration_price IS NOT NULL OR rough_price IS NOT NULL)
                  AND admitted_price IS NULL
                """
                with engine.connect() as conn:
                    conn.autocommit = False # 关闭自动提交
                    try:
                        conn.execute(text(delete_stmt))
                        conn.commit()
                        print('数据删除成功！')
                    except Exception as e:
                        conn.rollback()
                        print(f'执行删除语句错误，回滚事务：{e}')
                cross_projets_returned_nosimul_df.to_sql(name='result_recycle', con=engine,
                                                         if_exists='append', index=False)
                print('未处理的交叉调研项目入库成功！')

                # 计算同一批次回收的交叉调研项目
                actual_cross_projects_df: pd.DataFrame = cross_projects_df2[
                    cross_projects_df2['交叉调研项目是否同一批次回收'] == 1]
                del actual_cross_projects_df['交叉调研项目是否同一批次回收']
            else:
                # 删除前面批次未处理，但在本批次找到配对的交叉调研项目
                delete_stmt = f"""
                                DELETE
                                FROM dbo.result_recycle
                                WHERE data_dept = N'住宅'
                                  AND data_month = '{price_month}'
                                  AND is_cross = 1
                                  AND (decoration_price IS NOT NULL OR rough_price IS NOT NULL)
                                  AND admitted_price IS NULL
                                """
                with engine.connect() as conn:
                    conn.autocommit = False  # 关闭自动提交
                    try:
                        conn.execute(text(delete_stmt))
                        conn.commit()
                        print('前面批次未处理，但在本批次找到配对的交叉调研项目删除成功！')
                    except Exception as e:
                        conn.rollback()
                        print(f'执行删除语句错误，回滚事务：{e}')
                actual_cross_projects_df = result_df[result_df['是否交叉调研'] == 1]
                print('本次所有交叉调研项目均在同一批次回收！')
        else:
            sql_stmt: str = f"""
                        SELECT DISTINCT city_id, newcode, property_type 
                        FROM dbo.task_distribution
                        WHERE data_dept = '{data_dept}' AND  data_month = '{price_month}' AND is_cross = 1
                        """
            cross_projects_df = pd.read_sql(text(sql_stmt), engine)
            cross_projects_df.columns = ['城市ID', 'newcode', '物业类型']
            cross_projects_df['newcode'] = cross_projects_df['newcode'].astype('str')
            cross_projects_df['是否交叉调研'] = 1
            print(f'【{price_month}】月交叉调研项目数量：{cross_projects_df.shape[0] * 2}')

            data_df['newcode'] = data_df['newcode'].astype('str')
            data_df = data_df.merge(cross_projects_df, on=['城市ID', 'newcode', '物业类型'], how='left')
            print(f'本次回收结果中交叉调研项目数量：{data_df[data_df["是否交叉调研"] == 1].shape[0]}')
            data_df['是否交叉调研'] = data_df['是否交叉调研'].apply(lambda x: 0 if pd.isnull(x) else x)

            # 列顺序重新排列
            result_df = pd.DataFrame({col: data_df[col] for col in self.recycle_std_columns})

            actual_cross_projects_df = result_df[result_df['是否交叉调研'] == 1]

        noncross_projects_df = result_df[result_df['是否交叉调研'] == 0]
        final_df = pd.concat([noncross_projects_df, actual_cross_projects_df],
                             ignore_index=True).reset_index(drop=True)

        return final_df

    def result_recycle(self, data_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        cross_projects_df: pd.DataFrame = data_df[data_df['是否交叉调研'] == 1]
        noncross_projects_df: pd.DataFrame = data_df[data_df['是否交叉调研'] == 0]
        data_date: str = datetime.date.today().strftime('%Y-%m-%d')

        # 先处理非交叉调研项目
        noncross_projects_df[['价格差异', '价格差异百分比', '是否异常']] = [0.00, 0.00, 0]
        noncross_projects_df['采信价格'] = noncross_projects_df.apply(
            lambda x: x['精装价格(元/平方米)'] if x['主力在售装修情况'] == '精装' else x['毛坯价格(元/平方米)'], axis=1)
        noncross_projects_df.loc[noncross_projects_df['采信价格'].isna(), ['价格差异', '价格差异百分比', '是否异常']] = None
        noncross_projects_df.to_excel(os.path.join(self.out_path, f'{data_date}_回收结果-非交叉调研项目.xlsx'), index=False)

        # 再处理交叉调研项目
        cross_projects_df['调研价格'] = cross_projects_df.apply(
            lambda x: x['精装价格(元/平方米)'] if x['主力在售装修情况'] == '精装' else x['毛坯价格(元/平方米)'], axis=1)
        cross_projects_df.loc[cross_projects_df['调研价格'].notna(), ['价格差异', '价格差异百分比', '是否异常']] = [0.00, 0.00, 0]
        cross_projects_noprice_df: pd.DataFrame = cross_projects_df[cross_projects_df['调研价格'].isna()].drop(columns=['调研价格'])
        cross_projects_withprice_df: pd.DataFrame = cross_projects_df[~cross_projects_df['调研价格'].isna()]
        cross_projects_withprice_df = (cross_projects_withprice_df
                                       .groupby(['城市ID', '项目id', '物业类型'])
                                       .apply(price_processing).reset_index(drop=True))

        # 对某些交叉调研项目，如果其中一条无价格，另一条有价格，则采信价格直接采用调研价格
        cross_projects_withprice_df['采信价格'] = cross_projects_withprice_df.apply(
            lambda row: row['调研价格']
            if pd.notnull(row['调研价格']) and pd.isnull(row['采信价格']) else row['采信价格'], axis=1)

        del cross_projects_withprice_df['调研价格']
        cross_projects_df = pd.concat([cross_projects_noprice_df, cross_projects_withprice_df],
                                      ignore_index=True).reset_index(drop=True)
        cross_projects_df.to_excel(os.path.join(self.out_path, f'{data_date}_回收结果-交叉调研项目.xlsx'), index=False)

        all_projects_df: pd.DataFrame = pd.concat([noncross_projects_df, cross_projects_df], ignore_index=True).reset_index(drop=True)
        all_projects_df['城市排名'] = all_projects_df['城市排名'].astype('int')
        all_projects_df['是否交叉调研'] = all_projects_df['是否交叉调研'].astype('int')
        all_projects_df.loc[all_projects_df['是否异常'].notnull(), '是否异常'] \
            = all_projects_df.loc[all_projects_df['是否异常'].notnull(), '是否异常'].astype('int')
        all_projects_df.to_excel(os.path.join(self.out_path, f'{data_date}_回收结果-所有项目.xlsx'), index=False)

        # 输出价格字典
        # 以下城市的价格为实际成交价，如果包含这些城市，需要排除
        real_deal_price_cities: list[str] = ['北京', '上海', '成都', '天津', '福州', '珠海', '常州', '绍兴', '芜湖',
                                             '绵阳', '漳州', '丽水', '南阳', '三明', '池州', '眉山', '南平', '启东',
                                             '泰安', '泉州', '中山', '江门', '宿州', '武汉', '重庆']
        price_columns: list[str] = ['城市', '城市ID', '项目名称', '项目id', '月份', '物业类型', '采信价格']
        price_dict_df: pd.DataFrame = all_projects_df.loc[(all_projects_df['采信价格'].notnull())
                                                          & (all_projects_df['项目id'].notnull())
                                                          & (all_projects_df['价格差异百分比'] <= 30.0), price_columns]
        price_dict_df = price_dict_df.drop_duplicates().reset_index(drop=True)
        price_dict_df = price_dict_df.rename(columns={'项目名称': '总期名称', '项目id': '总期ID',
                                                      '月份': '适用时间', '物业类型': '细分物业类型'})
        price_dict_df[['来源', '修改类型', '备注']] = ['电话调研', 'ALL', None]
        price_dict_df[['预售证', '楼栋', '面积段', '字典类型(1总体0细分)']] = [None, None, None, 1]
        # 剔除实际成交价的城市（若存在）
        price_dict_df = price_dict_df[~price_dict_df['城市'].isin(real_deal_price_cities)]
        # 单个项目剔除：广州——华润置地·公园上城
        price_dict_df = price_dict_df[~((price_dict_df['城市'] == '广州') & (price_dict_df['总期名称'] == '华润置地·公园上城'))]
        price_dict_df.to_excel(os.path.join(self.out_path, f'{data_date}_价格字典-上传.xlsx'), index=False)

        # 输出总体核验数据
        need_columns: list[str] = ['城市', '城市ID', '项目名称', '项目id', '项目别名', 'newcode', '月份', '物业类型',
                                   '精装价格(元/平方米)', '毛坯价格(元/平方米)', '主力在售装修情况', '当月价格较上月变动（%）',
                                   '优惠情况', '销售状态', '备注', '负责人', '采信价格', '价格差异百分比', '是否异常']
        summary_data_df: pd.DataFrame = all_projects_df[need_columns]
        summary_data_df['销售状态'] = summary_data_df['销售状态'].apply(lambda x: None if x == 'nan' else x)
        summary_data_df = summary_data_df[summary_data_df['是否异常'] != 1]
        summary_data_df = summary_data_df.rename(columns={'价格差异百分比': '价格差异', '是否异常': '价格异常'})
        summary_data_df['项目别名'] = summary_data_df['项目别名'].apply(
            lambda x: str(x).split('/')[0] if '/' in str(x) else x)
        summary_data_df['newcode'] = summary_data_df['newcode'].apply(
            lambda x: str(x).split('/')[0] if '/' in str(x) else x)
        dup_columns: list[str] = ['城市', '城市ID', '项目名称', '项目id', '项目别名', 'newcode', '月份', '物业类型', '采信价格']
        summary_data_df = summary_data_df.drop_duplicates(subset=dup_columns).reset_index(drop=True)
        summary_data_df.to_excel(os.path.join(self.out_path, f'{data_date}_总体核验数据.xlsx'), index=False)

        return all_projects_df, price_dict_df

    def data_import_database(self, all_projects_df: pd.DataFrame, price_dict_df: pd.DataFrame) -> None:
        """
        将调研结果和价格字典插入数据库
        """
        engine = self.dbo.get_engine(server='local')
        # data_date: str = datetime.date.today().strftime('%Y-%m-%d')
        all_projects_df.columns = self.recycle_db_columns
        all_projects_df.drop_duplicates(subset=['city_id', 'data_month', 'project_id',
                                                'property_type', 'person_in_charge'], inplace=True)
        price_dict_df['导入时间'] = datetime.date.today().strftime('%Y-%m-%d')
        price_dict_df.drop(columns=['预售证', '楼栋', '面积段', '字典类型(1总体0细分)'], inplace=True, axis=1)
        price_dict_df.columns = self.price_dict_columns

        print('回收结果（处理后）入库中...')
        try:
            all_projects_df.to_sql(name='result_recycle', con=engine, if_exists='append', index=False)
            print('回收结果入库成功！')
        except Exception as e:
            print(f'数据入库错误：{e}')

        print('价格字典入库中...')
        try:
            price_dict_df.to_sql(name='price_dict', con=engine, if_exists='append', index=False)
            print('价格字典入库成功！')
        except Exception as e:
            print(f'数据入库错误：{e}')

    def run(self) -> None:
        print(f'{"="*30} 一、回收结果概览 {"="*30}')
        self.basic_info_overview()
        tips_message: str = input('请检查回收结果，确认无问题继续下一步请输入【Y/y】，否则输入【N/n】结束程序！\n>>>').lower()
        if tips_message == 'y':
            print(f'{"="*30} 二、回收结果数据预处理 {"="*30}')
            data_df: pd.DataFrame = self.data_preprocessing()
            print(f'{"="*35} 三、输出目标文件 {"="*35}')
            all_projects_df, price_dict_df = self.result_recycle(data_df=data_df)
            print('数据回收处理完成，并保存到本地！')

            is_import = input('是否将处理结果插入数据库？确认请输入【Y/y】，否则输入【N/n】结束程序！\n>>>').lower()
            if is_import == 'y':
                self.data_import_database(all_projects_df, price_dict_df)
            elif is_import == 'n':
                sys.exit()
            else:
                raise ValueError('输入指令不正确！')

        elif tips_message == 'n':
            sys.exit()
        else:
            raise ValueError('输入指令不正确！')


# if __name__ == '__main__':
#     data_path = 'recycle_data/20240805'
#     out_path = 'result_recycle'
#
#     rr = ResultRecycle(data_path=data_path, out_path=out_path)
#     rr.run()
