import sys

import pandas as pd

from task_distributor import TaskDistributor
from result_recycle import ResultRecycle


def task_executor(task_no: int, params_dict: dict) -> None:
    if task_no == 1:  # 任务分发（住宅部门）
        file_path = params_dict['file_path']
        people = params_dict['people']
        proportions = params_dict['proportions']
        cross_percentage = params_dict['cross_percentage']
        start_date = params_dict['start_date']
        output_dir = params_dict['output_dir']
        recycle_interval_days = params_dict['recycle_interval_days']

        td = TaskDistributor(file_path=file_path,
                             people=people,
                             proportions=proportions,
                             cross_percentage=cross_percentage,
                             start_date=start_date,
                             output_dir=output_dir,
                             dept_flag=1,
                             recycle_interval_days=recycle_interval_days)
        td.run()
    elif task_no == 2:  # 任务分发（指数部门）
        file_path = params_dict['file_path']
        people = params_dict['people']
        proportions = params_dict['proportions']
        cross_percentage = params_dict['cross_percentage']
        start_date = params_dict['start_date']
        output_dir = params_dict['output_dir']
        recycle_interval_days = params_dict['recycle_interval_days']

        td = TaskDistributor(file_path=file_path,
                             people=people,
                             proportions=proportions,
                             cross_percentage=cross_percentage,
                             start_date=start_date,
                             output_dir=output_dir,
                             dept_flag=2,
                             recycle_interval_days=recycle_interval_days)
        td.run()
    elif task_no == 3:  # 任务回收（住宅部门）
        data_path = params_dict['data_path']
        out_path = params_dict['out_path']

        rr = ResultRecycle(data_path=data_path, out_path=out_path, dept_flag=1)
        rr.run()
    elif task_no == 4:  # 任务回收（指数部门）
        data_path = params_dict['data_path']
        out_path = params_dict['out_path']

        rr = ResultRecycle(data_path=data_path, out_path=out_path, dept_flag=2)
        rr.run()
    else:
        print('无效的任务编号，请重新运行程序！')
        sys.exit()


if __name__ == '__main__':
    distribut_params_dict: dict = {
        'file_path': r"task_distribution/原始调研项目数据/9月价格-20241011.xlsx",  # 原始调研任务数据
        'people': ['陈岩', '陈瑞', '苏静', '陈晨', '秦琳', '李瑾如'],                               # 调研人员
        'proportions': [0.2, 0.2, 0.2, 0.2, 0.1, 0.1],                                # 调研比例（可按能力分配）
        'cross_percentage': 0.3,                                               # 交叉调研比例
        'start_date': pd.to_datetime('2024-10-11'),                             # 回收起始日期
        'output_dir': r'task_distribution',                                     # 任务分发结果保存目录
        'recycle_interval_days': 5,                                             # 两个批次回收间隔天数，可选（默认7天）
    }
    recycle_params_dict: dict = {
        'data_path': r'recycle_data/20241008',  # 回收结果路径
        'out_path': r'result_recycle'           # 回收结果保存目录
    }

    message: str = input("""
价格调研任务执行：
    d/D：任务分发
    r/R：结果回收
请输入指令
""").lower()
    if message == 'd':
        task_id: int = int(input(f"""
任务分发执行：
    1：任务分发（住宅部门）
    2：任务分发（指数部门）
请输入任务编号
"""))

        # 任务分发
        task_executor(task_no=task_id, params_dict=distribut_params_dict)
    elif message == 'r':
        task_id: int = int(input(f"""
任务回收执行：
    3：任务回收（住宅部门）
    4：任务回收（指数部门）
请输入任务编号
"""))
        # 结果回收
        task_executor(task_no=task_id, params_dict=recycle_params_dict)
    else:
        print('指令错误，请重新运行程序！')
        sys.exit()
