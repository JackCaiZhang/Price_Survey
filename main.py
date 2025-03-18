import sys
from textwrap import dedent

import pandas as pd

from task_distributor import TaskDistributor
from result_recycle import ResultRecycler


def task_executor(params_list: list[dict]) -> None:
    task_dist_params, task_recycle_params = params_list
    message: str = input(dedent(f"""
                                    价格调研任务执行：
                                        d/D：任务分发
                                        r/R：结果回收
                                    请输入指令
                                    """)).lower()
    if message == 'd':
        task_id: str = input(dedent(f"""
                                        任务分发执行：
                                            1：任务分发（住宅部门）
                                            2：任务分发（指数部门）
                                        请输入任务编号
                                        """))
        dept_flag: int | None = None
        if task_id in ['1', '2']:
            dept_flag = int(task_id)
        else:
            print('指令错误，请重新运行程序！')
            sys.exit()
        distributor = TaskDistributor(file_path=task_dist_params['file_path'],
                                      people=task_dist_params['people'],
                                      proportions=task_dist_params['proportions'],
                                      cross_percentage=task_dist_params['cross_percentage'],
                                      start_date=task_dist_params['start_date'],
                                      output_dir=task_dist_params['output_dir'],
                                      dept_flag=dept_flag,
                                      recycle_interval_days=task_dist_params['recycle_interval_days'])
        distributor.run()
    elif message == 'r':
        task_id: str = input(dedent(f"""
                                        任务回收执行：
                                            1：任务回收（住宅部门）
                                            2：任务回收（指数部门）
                                        请输入任务编号
                                        """))
        dept_flag: int | None = None
        if task_id in ['1', '2']:
            dept_flag = int(task_id)
        else:
            print('指令错误，请重新运行程序！')
            sys.exit()
        recycler = ResultRecycler(data_path=task_recycle_params['data_path'],
                                  out_path=task_recycle_params['out_path'],
                                  dept_flag=dept_flag)
        recycler.run()
    else:
        print('指令错误，请重新运行程序！')
        sys.exit()


if __name__ == '__main__':
    distribut_params_dict: dict = {
        'file_path': r"task_distribution/原始调研项目数据/3月21日中午12点前返回.xlsx",  # 原始调研任务数据
        'people': ['陈岩', '苏静', '陈晨', '陈瑞'],                               # 调研人员, '秦琳', '李雨涵'
        'proportions': [0.26, 0.26, 0.26, 0.22],                                # 调研比例（可按能力分配）
        'cross_percentage': 0.4,                                               # 交叉调研比例
        'start_date': pd.to_datetime('2025-03-18'),                             # 回收起始日期
        'output_dir': r'task_distribution',                                     # 任务分发结果保存目录
        'recycle_interval_days': 3,                                             # 两个批次回收间隔天数，可选（默认7天）
    }
    recycle_params_dict: dict = {
        'data_path': r'recycle_data/20250317',  # 回收结果路径
        'out_path': r'result_recycle'           # 回收结果保存目录
    }

    task_executor(params_list=[distribut_params_dict, recycle_params_dict])
