import datetime
import math
import os.path
import random
import sys
from copy import deepcopy
from typing import List

import pandas as pd
import warnings
from datetime import timedelta

from dateutil.relativedelta import relativedelta

from DBOperration import DBOperation


warnings.filterwarnings(action="ignore")


def task_num_by_prop(tasks_num: int, proportions: List) -> List:
    """根据比例（即能力）计算每个人负责的调研量"""
    # 计算初始分配值和舍入误差
    initial_tasks = [int(tasks_num * p) for p in proportions]
    errors = [tasks_num * p - v for p, v in zip(proportions, initial_tasks)]

    # 计算初始分配值的和
    sum_initial = sum(initial_tasks)

    # 计算需要调整的数量
    adjust = tasks_num - sum_initial

    # 根据舍入误差对初始分配值进行调整
    sorted_indices = sorted(range(len(errors)), key=lambda i: errors[i], reverse=True)
    for i in range(adjust):
        initial_tasks[sorted_indices[i]] += 1

    return initial_tasks


class TaskDistributor:
    distribut_std_columns: list[str] = ['城市类型', '城市ID', '城市', '月份', '项目id', '项目名称', '项目别名', '项目别名ID',
                                        'newcode', '物业类型套数', '总套数', '物业类型', '城市排名', '精装价格(元/平方米)',
                                        '毛坯价格(元/平方米)', '主力在售装修情况', '当月价格较上月变动（%）', '优惠情况',
                                        '销售状态', '备注', '负责人', '回收日期', '调研方式', '是否交叉调研', '数据部门',
                                        '任务分发时间', '唯一键字段']
    distribut_db_columns: list[str] = ['city_type', 'city_id', 'city', 'data_month', 'project_id', 'project_name',
                                       'newhouse_name', 'newhouse_id', 'newcode', 'property_type_num', 'total_num',
                                       'property_type', 'city_ranking', 'person_in_charge', 'return_date', 'is_cross',
                                       'data_dept', 'distribute_date', 'unique_field']
    city_ranking: pd.DataFrame = pd.read_excel(r'城市排名.xlsx')
    dbo = DBOperation()

    def __init__(self, file_path: str, people: List, proportions: List,
                 cross_percentage: float, start_date: datetime, output_dir: str,
                 dept_flag: int = 1, recycle_interval_days: int = 7) -> None:
        self.file_path = file_path
        self.people = people
        self.proportions = proportions
        self.cross_percentage = cross_percentage
        self.start_date = start_date
        self.output_dir = output_dir
        self.dept_flag = dept_flag
        self.recycle_interval_days = recycle_interval_days

    def data_preprocessing(self):
        # result_df: pd.DataFrame = pd.DataFrame()
        if self.dept_flag == 1:
            # 将分期名称聚合为一行（因为存在一个总期匹配多个分期的情况）
            base_columns: list[str] = ['城市类型', '城市ID', '城市', '月份', '总期id', '总期名',
                                       '细分物业类型', '细分物业类型套数', '均价', '总套数']
            data_df: pd.DataFrame = pd.read_excel(self.file_path, dtype={'newcode': 'str'})
            data_df['匹配的分期ID'] = data_df['匹配的分期ID'].astype('str')
            data_df['匹配的分期名称'] = data_df['匹配的分期名称'].astype('str')
            data_df['newcode'] = data_df['newcode'].astype('str')
            agg_df: pd.DataFrame = data_df.groupby(base_columns).agg({
                '匹配的分期ID': '/'.join,
                '匹配的分期名称': '/'.join,
                'newcode': '/'.join,
            }).reset_index()
            result_df: pd.DataFrame = agg_df.rename(columns={
                '总期id': '项目id',
                '总期名': '项目名称',
                '细分物业类型': '物业类型',
                '细分物业类型套数': '物业类型套数',
                '匹配的分期ID': '项目别名ID',
                '匹配的分期名称': '项目别名',
            })
            result_df['项目别名ID'] = result_df['项目别名ID'].apply(lambda x: None if x == 'nan' else x)
            result_df['项目别名'] = result_df['项目别名'].apply(lambda x: None if x == 'nan' else x)
            result_df['newcode'] = result_df['newcode'].apply(lambda x: None if x == 'nan' else x)
        else:
            data_df: pd.DataFrame = pd.read_excel(self.file_path, dtype={'newcode': 'str'})
            engine = self.dbo.get_engine(server='house_test')
            query_stmt: str = f"""
            SELECT DISTINCT city_id, city_name, newcode, property_id, property_name
            FROM dbo.temp_property_instalment_relation
            WHERE is_del = 0 AND ISNULL(newcode, '') <> ''
            """
            newhouse_match_df: pd.DataFrame = pd.read_sql(query_stmt, engine)
            property_name_dict: dict = newhouse_match_df.set_index('newcode')['property_name'].to_dict()
            property_id_dict: dict = newhouse_match_df.set_index('newcode')['property_id'].to_dict()
            city_id_dict: dict = newhouse_match_df.set_index('city_name')['city_id'].to_dict()
            data_df['城市ID'] = data_df.apply(lambda row: city_id_dict[row['城市']], axis=1)
            data_df['项目名称'] = data_df.apply(lambda row: property_name_dict.get(row['newcode']), axis=1)
            data_df['项目id'] = data_df.apply(lambda row: property_id_dict.get(row['newcode']), axis=1)
            result_df = data_df.copy()

        # 城市排名处理
        self.add_city_ranking(result_df)    # 新增城市将其添加到城市排名表中
        city_rank_dict: dict = self.city_ranking.set_index('城市')['排名'].to_dict()
        result_df['城市排名'] = result_df.apply(lambda row: city_rank_dict[row['城市']], axis=1)

        # 列标准化
        result_df = self.data_format(result_df)

        return result_df

    def data_format(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.dept_flag == 1:
            add_columns = ['精装价格(元/平方米)', '毛坯价格(元/平方米)', '主力在售装修情况', '当月价格较上月变动（%）',
                           '优惠情况', '销售状态', '备注', '负责人', '回收日期', '调研方式', '是否交叉调研',
                           '任务分发时间', '唯一键字段']
            df[add_columns] = None
            df.loc[:, '数据部门'] = '住宅'
        else:
            add_columns: list[str] = ['回收日期', '项目别名ID', '城市类型', '物业类型套数',
                                      '总套数', '是否交叉调研', '任务分发时间', '唯一键字段']
            df[add_columns] = None
            df.loc[:, '数据部门'] = '指数'
            df['月份'] = df['月份'].astype('datetime64[ns]')
            df['月份'] = df['月份'].apply(lambda x: x.strftime('%Y-%m'))

        result_df = pd.DataFrame({col: df[col] for col in self.distribut_std_columns})

        return result_df

    def add_city_ranking(self, df: pd.DataFrame):
        """
        新增城市排名添加：若有新增城市，则将其添加到城市排名表中。
        注：排名只考虑百名，百名以外的新增城市，排名从101开始自增。
        """
        print('新增城市排名处理：')
        cities = set(df['城市'].unique().tolist())
        inherit_cities = set(self.city_ranking['城市'].unique().tolist())
        add_cities = cities.difference(inherit_cities)

        if add_cities:
            print(f'本次新增城市：{add_cities}')
            last_city_index = len(self.city_ranking)-1
            last_city_ranking = self.city_ranking.loc[last_city_index, '排名']
            for index, city in enumerate(add_cities):
                self.city_ranking.loc[last_city_index + index + 1, '城市'] = city
                self.city_ranking.loc[last_city_index + index + 1, '排名'] = last_city_ranking + index + 1
            self.city_ranking.to_excel('城市排名.xlsx', index=False)
            print('已将新增城市添加到城市排名表中！')
        else:
            print('本次无新增城市！\n')

    def cross_survey(self):
        """
        交叉调研处理：返回总调研项目=原始+交叉
        """
        data_df = self.data_preprocessing()
        num_projects = len(data_df)
        # 交叉调研数量只取整数部分
        cross_num = math.floor(self.cross_percentage * num_projects)
        cross_projects = data_df.sample(n=cross_num)    # 根据交叉调研数量进行采样
        combined_projects = pd.concat([data_df, cross_projects], ignore_index=True)

        if self.dept_flag == 1:
            # 交叉调研项目标记，重复项目都是交叉调研
            combined_projects['是否交叉调研'] = combined_projects.duplicated(subset=['城市ID', '项目id', '物业类型'],
                                                                             keep=False).astype(int)
            # 添加回收日期：先根据“是否交叉调研”排序，然后再添加分批回收日期，这样做可以让交叉调研项目尽可能在同一个批次回收
            combined_projects = combined_projects.sort_values(by=['是否交叉调研', '城市排名', '项目id', '物业类型'],
                                                              ascending=[False, True, True, True])
        else:
            # 交叉调研项目标记，重复项目都是交叉调研
            combined_projects['是否交叉调研'] = combined_projects.duplicated(subset=['城市ID', 'newcode', '物业类型'],
                                                                             keep=False).astype(int)
            # 添加回收日期：先根据“是否交叉调研”排序，然后再添加分批回收日期，这样做可以让交叉调研项目尽可能在同一个批次回收
            combined_projects = combined_projects.sort_values(by=['是否交叉调研', '城市排名', 'newcode', '物业类型'],
                                                              ascending=[False, True, True, True])

        combined_projects = self.add_return_date(combined_projects)
        combined_projects = combined_projects.sample(frac=1, random_state=1).reset_index(drop=True) # 打乱项目顺序
        noncross_df = combined_projects[combined_projects['是否交叉调研'] == 0]
        cross_df = combined_projects[combined_projects['是否交叉调研'] == 1]

        return noncross_df, cross_df

    def task_distribution(self):
        """
        任务分发
        """
        noncross_df, cross_df = self.cross_survey()
        tasks_by_person = {person: [] for person in self.people}    # 任务列表字典

        # 计算每个人的任务数量，按比例分配
        total_tasks = noncross_df.shape[0] + cross_df.shape[0]
        task_num_dic = {person: task_num for person, task_num in
                        zip(self.people, task_num_by_prop(total_tasks, self.proportions))}

        # 开始给每个人分配任务
        print('开始执行任务分发：')
        print(f'任务分发前——调研总量：{total_tasks}；'
              f'其中，非交叉调研量：{noncross_df[noncross_df["是否交叉调研"] == 0].shape[0]}；'
              f'交叉调研量（比例：{self.cross_percentage}）：{cross_df[cross_df["是否交叉调研"] == 1].shape[0]}')
        print(f'每个人应分配任务数量：{task_num_dic}')

        distributed_keys = {person: set() for person in self.people}   # 创建唯一标识符的集合
        undistributed_projects = []    # 未分配项目列表
        if not cross_df.empty:
            for project in cross_df.itertuples(index=False):    # 先分配交叉调研项目
                if self.dept_flag == 1:
                    project_key = (project.城市ID, project.项目id, project.物业类型)
                else:
                    project_key = (project.城市ID, project.newcode, project.物业类型)
                assigned = False
                people = deepcopy(self.people)
                random.shuffle(people)  # 打乱人员列表顺序，每次都将任务随机分配给其中一人
                for person in people:
                    # 如果当前人员的任务数量小于他应分配的任务量 且 当前待分配项目不在他的任务列表中
                    if len(tasks_by_person[person]) < task_num_dic[person] and project_key not in distributed_keys[person]:
                        tasks_by_person[person].append(project)
                        distributed_keys[person].add(project_key)
                        assigned = True
                        break   # 当前项目已分配，防止同一个项目分配给多个人
                    else:
                        continue
                if not assigned:    # 如果项目未分配，则将其添加到未分配项目列表
                    undistributed_projects.append(project)
        for project in noncross_df.itertuples(index=False):    # 再分配非交叉调研项目
            if self.dept_flag == 1:
                project_key = (project.城市ID, project.项目id, project.物业类型)
            else:
                project_key = (project.城市ID, project.newcode, project.物业类型)
            assigned = False
            people = deepcopy(self.people)
            random.shuffle(people)  # 打乱人员列表顺序，每次都将任务随机分配给其中一人
            for person in people:
                # 如果当前人员的任务数量小于他应分配的任务量 且 当前待分配项目不在他的任务列表中
                if len(tasks_by_person[person]) < task_num_dic[person] and project_key not in distributed_keys[person]:
                    tasks_by_person[person].append(project)
                    distributed_keys[person].add(project_key)
                    assigned = True
                    break   # 当前项目已分配，防止同一个项目分配给多个人
                else:
                    continue
            if not assigned:    # 如果项目未分配，则将其添加到未分配项目列表
                undistributed_projects.append(project)

        if undistributed_projects:
            print(f"存在未分配项目【{len(undistributed_projects)}】个！")

        # 将任务转换为DataFrame并合并
        whole_task_df = pd.DataFrame(columns=self.distribut_std_columns, data=None)
        for person, tasks in tasks_by_person.items():
            tasks_df = pd.DataFrame(tasks, columns=self.distribut_std_columns)
            tasks_df.loc[:, '负责人'] = person
            whole_task_df = pd.concat((whole_task_df, tasks_df), ignore_index=True)
        whole_task_df.reset_index(drop=True, inplace=True)

        print('任务分发完成！')
        print(f"分发完成后——调研总量：{whole_task_df.shape[0]}；"
              f"非交叉调研量：{len(whole_task_df[whole_task_df['是否交叉调研'] == 0])}；"
              f"交叉调研总量：{len(whole_task_df[whole_task_df['是否交叉调研'] == 1])}")

        print('实际每个人的任务分配情况：')
        for person in self.people:
            print(f"{person}：总调研量【{len(whole_task_df[whole_task_df['负责人'] == person])}】；"
                  f"其中，非交叉调研量："
                  f"{len(whole_task_df[(whole_task_df['负责人'] == person) & (whole_task_df['是否交叉调研'] == 0)])}，"
                  f"交叉调研量："
                  f"{len(whole_task_df[(whole_task_df['负责人'] == person) & (whole_task_df['是否交叉调研'] == 1)])}")
        print()

        if self.dept_flag == 1:
            whole_task_df.loc[:, '唯一键字段'] = whole_task_df.apply(
                lambda x: ''.join([x['城市ID'], x['月份'], str(x['项目id']), x['物业类型'], x['负责人']]), axis=1)
        else:
            whole_task_df.loc[:, '唯一键字段'] = whole_task_df.apply(
                lambda x: ''.join([x['城市ID'], x['月份'], str(x['newcode']), x['物业类型'], x['负责人']]), axis=1)

        return whole_task_df

    def add_return_date(self, tasks_df):
        tasks_df = tasks_df.reset_index(drop=True)
        if self.dept_flag == 1:
            batch_size = len(tasks_df) // 4
            remainder = len(tasks_df) % 4
            dates = [self.start_date + timedelta(days=self.recycle_interval_days * (i + 1)) for i in range(4)]

            for i in range(4):
                # 如果整除，则每个批次的回收数量相同；若不整除，则将余数放到最后一个批次。
                batch_end = (i + 1) * batch_size
                if remainder > 0 and i == 3:
                    batch_end += remainder
                tasks_df.loc[i * batch_size:batch_end, '回收日期'] = dates[i]
        else:
            tasks_df.loc[:, '回收日期'] = self.start_date + timedelta(days=3)

        return tasks_df

    def save_distribution_result(self, tasks_df: pd.DataFrame) -> None:
        """
        将分发结果保存到本地并写入数据库
        """
        output_columns: list[str] = ['城市ID', '城市', '月份', '项目id', '项目名称', '项目别名', 'newcode', '物业类型',
                                     '精装价格(元/平方米)', '毛坯价格(元/平方米)', '主力在售装修情况', '当月价格较上月变动（%）',
                                     '优惠情况', '销售状态', '备注', '负责人', '回收日期', '调研方式', '城市排名']
        tasks_df['任务分发时间'] = datetime.date.today()
        # 计算任务分发日期
        data_date: str = tasks_df.loc[0, '月份']
        if self.dept_flag == 1:
            task_date: [datetime.date | str] = datetime.date(year=int(data_date.split('-')[0]),
                                                             month=int(data_date.split('-')[1]),
                                                             day=1) + relativedelta(months=1)
            task_date = '-'.join([str(task_date.year), str(task_date.month).zfill(2)])

            print('将分发任务保存到本地：')
            tasks_df_copy: pd.DataFrame = tasks_df.copy()
            tasks_df_copy['月份'] = task_date  # 将月份修改为任务分发月份
            tasks_df_copy['项目别名'] = tasks_df_copy['项目别名'].apply(
                lambda x: str(x).split('/')[0] if '/' in str(x) else x)
            tasks_df_copy['newcode'] = tasks_df_copy['newcode'].apply(
                lambda x: str(x).split('/')[0] if '/' in str(x) else x)
            tasks_df_copy = tasks_df_copy.sort_values(by=['回收日期', '城市排名', '项目id', '物业类型'],
                                                      ascending=[True, True, True, True])
            tasks_df_copy.to_excel(os.path.join(self.output_dir, f'住宅部门/{task_date}调研任务-住宅.xlsx'), index=False)
            for person in self.people:
                task_i_df: pd.DataFrame = tasks_df_copy[output_columns][tasks_df_copy['负责人'] == person]
                # 按城市排名顺序输出
                task_i_df = task_i_df.sort_values(by=['回收日期', '城市排名', '项目id', '物业类型'],
                                                  ascending=[True, True, True, True])
                del task_i_df['城市排名']
                file_name: str = os.path.join(self.output_dir, f'住宅部门/{task_date}调研任务-住宅_{person}.xlsx')
                task_i_df.to_excel(file_name, index=False)
                print(f"Tasks for {person} saved to {file_name}")
        elif self.dept_flag == 2:
            task_date = data_date
            print('将分发任务保存到本地：')
            tasks_df_copy: pd.DataFrame = tasks_df.copy()
            tasks_df_copy['月份'] = task_date  # 将月份修改为任务分发月份
            tasks_df_copy = tasks_df_copy.sort_values(by=['回收日期', '城市排名', '项目id', '物业类型'],
                                                      ascending=[True, True, True, True])
            tasks_df.to_excel(os.path.join(self.output_dir, f'指数部门/{task_date}调研任务-指数.xlsx'), index=False)
            for person in self.people:
                task_i_df: pd.DataFrame = tasks_df_copy[output_columns][tasks_df_copy['负责人'] == person]
                # 按城市排名顺序输出
                task_i_df = task_i_df.sort_values(by=['回收日期', '城市排名', '项目id', '物业类型'],
                                                  ascending=[True, True, True, True])
                del task_i_df['城市排名']
                file_name: str = os.path.join(self.output_dir, f'指数部门/{task_date}调研任务-指数_{person}.xlsx')
                task_i_df.to_excel(file_name, index=False)
                print(f"Tasks for {person} saved to {file_name}")
        else:
            print('部门标志错误！1：住宅；2：指数')

        tips_message: str = input('将分发任务插入数据库（Y/N）？').lower()
        if tips_message == 'y':
            del_columns = ['精装价格(元/平方米)', '毛坯价格(元/平方米)', '主力在售装修情况',
                           '当月价格较上月变动（%）', '优惠情况', '销售状态', '备注', '调研方式']
            tasks_df = tasks_df.drop(columns=del_columns).reset_index(drop=True)
            tasks_df.columns = self.distribut_db_columns
            dbo = DBOperation()
            try:
                con = dbo.get_engine(server='local')
                tasks_df.to_sql(name='task_distribution', con=con, if_exists='append', index=False)
                print('数据入库完成！')
            except Exception as e:
                print(f'数据入库错误：{e}')
        elif tips_message == 'n':
            sys.exit(0)
        else:
            raise ValueError('输入指令错误！')
        print('任务分发完成！')

    def run(self) -> None:
        """
        执行任务分发流程
        """
        whole_task_df = self.task_distribution()
        self.save_distribution_result(whole_task_df)
