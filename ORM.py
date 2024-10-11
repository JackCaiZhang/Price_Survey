from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, NVARCHAR, INT, DECIMAL, DATE

base = declarative_base()

# 任务分发表 ORM 模型
class TaskDistribution(base):
    __tablename__ = 'task_distribution'
    city_type = Column(NVARCHAR(10))                            # 城市类型
    city_id = Column(NVARCHAR(50), primary_key=True)            # 城市ID
    city = Column(NVARCHAR(30))                                 # 城市
    data_month = Column(NVARCHAR(30), primary_key=True)         # 数据日期（月份）
    project_id = Column(NVARCHAR(100), primary_key=True)        # 项目id
    project_name = Column(NVARCHAR(100))                        # 项目名称
    newhouse_name = Column(NVARCHAR(500))                       # 新房名称
    newhouse_id = Column(NVARCHAR(2000))                        # 新房id
    newcode = Column(NVARCHAR(2000))                            # 新房newcode
    property_type_num = Column(INT)                             # 物业类型套数
    total_num = Column(INT)                                     # 总套数
    property_type = Column(NVARCHAR(20), primary_key=True)      # 物业类型
    city_ranking = Column(INT)                                  # 城市排名
    person_in_charge = Column(NVARCHAR(20), primary_key=True)   # 负责人
    return_date = Column(DATE)                                  # 回收日期
    is_cross = Column(INT)                                      # 是否交叉调研（1--是；0--否）
    data_dept = Column(NVARCHAR(20))                            # 数据部门
    distribute_date = Column(DATE)                              # 任务分发时间


# 结果回收 ORM 模型
class TaskRecycle(base):
    __tablename__ = 'result_recycle'
    city_id = Column(NVARCHAR(50), primary_key=True)            # 城市ID
    city = Column(NVARCHAR(30))                                 # 城市
    data_month = Column(NVARCHAR(30), primary_key=True)         # 数据日期（月份）
    project_id = Column(NVARCHAR(100), primary_key=True)        # 项目id
    project_name = Column(NVARCHAR(100))                        # 项目名称
    newhouse_name = Column(NVARCHAR(500))                       # 新房名称
    newcode = Column(NVARCHAR(2000))                            # 新房newcode
    property_type = Column(NVARCHAR(20), primary_key=True)      # 物业类型
    city_ranking = Column(INT)                                  # 城市排名
    decoration_price = Column(DECIMAL(precision=18, scale=2))   # 精装价格
    rough_price = Column(DECIMAL(precision=18, scale=2))        # 毛坯价格
    decoration_of_main_sale = Column(NVARCHAR(20))              # 主力在售装修情况
    chain_ratio = Column(DECIMAL(6, 2))           # 环比（当月价格较上月变动）
    discount = Column(NVARCHAR(100))                            # 优惠情况
    sale_status = Column(NVARCHAR(50))                          # 销售状态
    comment = Column(NVARCHAR(200))                             # 备注
    person_in_charge = Column(NVARCHAR(20), primary_key=True)   # 负责人
    return_date = Column(DATE)                                  # 回收日期
    survey_method = Column(NVARCHAR(50))                        # 调研方式
    is_cross = Column(INT)                                      # 是否交叉调研（1--是；0--否）
    data_dept = Column(NVARCHAR(20))                            # 数据部门
    price_difference = Column(DECIMAL(precision=10, scale=2))   # 价格差异
    price_diff_ratio = Column(DECIMAL(precision=6, scale=2))    # 价格差异百分比
    is_abnormal = Column(INT)                                   # 是否异常（1--是；0--否）
    admitted_price = Column(DECIMAL(precision=18, scale=2))     # 采信价格
    import_date = Column(DATE)                                  # 导入时间
    # is_recheck = Column(INT)                                    # 是否复核（1--是；0--否）


# 价格字典 ORM 模型
class PriceDict(base):
    __tablename__ = 'price_dict'
    city = Column(NVARCHAR(30))                                 # 城市
    city_id = Column(NVARCHAR(50), primary_key=True)            # 城市ID
    sProperty_name = Column(NVARCHAR(100))                      # 总期名称
    sProperty_id = Column(NVARCHAR(100), primary_key=True)      # 总期ID
    app_date = Column(NVARCHAR(20), primary_key=True)           # 适用时间
    property_type_new = Column(NVARCHAR(20), primary_key=True)  # 细分物业类型
    admitted_price = Column(DECIMAL(precision=18, scale=2))     # 采信价格
    price_source = Column(NVARCHAR(20))                         # 价格来源
    modify_type = Column(NVARCHAR(10))                          # 修改类型
    comment = Column(NVARCHAR(100))                             # 备注
    import_date = Column(DATE)                                  # 导入时间
