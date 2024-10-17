import datetime
from operator import index

import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.core.reshape.pivot import pivot_table, pivot

from DBOperration import DBOperation

current_month: str = '2024-10'
current_date: datetime.datetime = datetime.datetime.strptime(current_month, '%Y-%m')
one_month_ago: datetime.datetime | str = current_date - relativedelta(months=1)
tow_month_ago: datetime.datetime | str = current_date - relativedelta(months=2)
one_month_ago = one_month_ago.strftime('%Y-%m')
tow_month_ago = tow_month_ago.strftime('%Y-%m')
data_months: tuple = (one_month_ago, tow_month_ago)
sql = f"""
WITH RecentData AS (SELECT city,
                           project_name,
                           property_type,
                           data_month,
                           admitted_price,
                           ROW_NUMBER() OVER (PARTITION BY city, project_name,
                               property_type ORDER BY data_month DESC) AS rn
                    FROM result_recycle
                    WHERE data_dept = N'住宅'
                      AND data_month IN {data_months}
                      AND admitted_price IS NOT NULL),
     FilteredProject AS (SELECT city,
                                project_name,
                                property_type
                         FROM RecentData
                         WHERE rn <= 2
                         GROUP BY city, project_name, property_type
                         HAVING COUNT(*) >= 1 -- 找出至少有最近2个月价格的项目
     )
SELECT DISTINCT rd.city,
                rd.project_name,
                rd.property_type,
                rd.data_month,
                rd.admitted_price
FROM RecentData rd
         JOIN FilteredProject fp
              ON rd.city = fp.city
                  AND rd.project_name = fp.project_name
                  AND rd.property_type = fp.property_type
WHERE rd.rn <= 2 -- AND rd.city = '邯郸'
ORDER BY rd.city, rd.project_name, rd.property_type, rd.data_month DESC
"""

db = DBOperation()
engine = db.get_engine(server='local')
df: pd.DataFrame = pd.read_sql(sql, engine)
pivot_df = df.pivot_table(
    index=['city', 'project_name', 'property_type'],
    columns='data_month',
    values='admitted_price',
    aggfunc='first'
).reset_index()
pivot_df.columns = ['city', 'project_name', 'property_type', 'm-2_price', 'm-1_price']
pivot_df.sort_values(by=['city', 'project_name', 'property_type'], ascending=[True, True, True], inplace=True)
pivot_df.to_excel(r'项目最近2月调研价格变化.xlsx', index=False)
