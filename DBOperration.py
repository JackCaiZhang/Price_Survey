from typing import Type

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Mapper


class DBOperation:
    db_info = {
        'local': {'server': 'localhost',
                  'database': 'price_survey',
                  'username': 'sa',
                  'password': '123456',
                  'driver': 'ODBC Driver 17 for SQL Server',
                  'port': '1433'},
        'house_test': {'server': '10.24.64.167:1433',
                       'database': 'house_test',
                       'username': 'house_test_admin',
                       'password': 'klS7xjs3',
                       'driver': 'ODBC Driver 17 for SQL Server',
                       'port': '1433'}
    }

    def get_engine(self, server: str):
        conn_str: str = ''
        if server == 'local':
            conn_str = rf"mssql+pyodbc://{self.db_info['local']['username']}:{self.db_info['local']['password']}@{self.db_info['local']['server']}/{self.db_info['local']['database']}?driver={self.db_info['local']['driver']}&port={self.db_info['local']['port']}"
        elif server == 'house_test':
            conn_str = rf"mssql+pyodbc://{self.db_info['house_test']['username']}:{self.db_info['house_test']['password']}@{self.db_info['house_test']['server']}/{self.db_info['house_test']['database']}?driver={self.db_info['house_test']['driver']}&port={self.db_info['house_test']['port']}"
        else:
            print('服务器标志错误！')
        engine = create_engine(conn_str)

        return engine

    def insert_data_batch(self, orm: Type | Mapper, df: pd.DataFrame) -> None:
        # 创建 Session
        Session = sessionmaker(bind=self.get_engine(server='local'))
        session = Session()

        # 将数据转换为字典列表
        data_dicts = df.to_dict(orient='records')

        # 批量插入数据
        try:
            session.bulk_insert_mappings(orm, data_dicts)
            session.commit()
            print('数据入库成功！')
        except Exception as e:
            session.rollback()
            print(f'数据插入错误：{e}')
        finally:
            session.close()