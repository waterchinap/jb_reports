import pandas as pd
import akshare as ak
import fire
import sqlite3
import time
import warnings
import logging
from datetime import datetime
from functools import wraps

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 手动注册日期适配器和转换器
sqlite3.register_adapter(datetime, lambda val: val.isoformat())
sqlite3.register_converter('datetime', lambda val: datetime.fromisoformat(val.decode()))

# 重试装饰器
def retry(max_retries=3, retry_delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f'Attempt {attempt + 1} failed: {e}')
                    if attempt < max_retries - 1:
                        logging.info(f'Retrying in {retry_delay} seconds...')
                        time.sleep(retry_delay)
                    else:
                        logging.error(f'Failed to execute {func.__name__} after {max_retries} attempts.')
                        raise
        return wrapper
    return decorator

# 创建季度列表
def create_qs(end_y, end_q):
    quarters = ['0331', '0630', '0930', '1231']
    qs = [f'{year}{quarter}' for year in range(2010, end_y + 1) for quarter in quarters]
    return qs[:-4 + end_q] if end_q < 4 else qs

# 获取数据并更新数据库
@retry(max_retries=3, retry_delay=2)
def get_a(rdate, db_name, table_name):
    df = ak.stock_yjbb_em(date=rdate)
    df['rdate'] = rdate
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        update_db(df, db_name, table_name)
    logging.info(f'Finished {rdate}')
    time.sleep(2)

# 更新数据库
def update_db(df, db_name, table_name):
    conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    columns = ', '.join([f'"{col}"' for col in df.columns])
    insert_sql = f'INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({", ".join(["?"] * len(df.columns))})'
    cursor.executemany(insert_sql, [tuple(x) for x in df.values])
    conn.commit()
    conn.close()
    logging.info(f'Data saved to database! Inserted {cursor.rowcount} rows.')

# 加载上次更新日期
def load_last_update_date(db_name, table_name):
    try:
        conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        cursor.execute(f'SELECT MAX("rdate") FROM {table_name}')
        last_date = cursor.fetchone()[0] or '20100331'
        conn.close()
        return last_date
    except sqlite3.OperationalError:
        logging.warning('Database does not exist or table is empty. Rebuilding database...')
        ensure_table_exists(db_name, table_name, ak.stock_yjbb_em(date='20100331'))
        return '20100331'

# 确保表存在
def ensure_table_exists(db_name, table_name, df):
    conn = sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cursor.execute(f'CREATE TABLE {table_name} ({columns}, PRIMARY KEY ("rdate", "股票代码"))')
        logging.info('Database created!')
    conn.close()

# 主函数
def main(end_y, end_q):
    db_name, table_name = 'yjbb.db', 'yjbb_data'
    last_date = load_last_update_date(db_name, table_name)
    qs = [q for q in create_qs(end_y, end_q) if q >= last_date]
    if qs:
        logging.info(f'Updating data from {qs[0]} to {qs[-1]}...')
        for q in qs:
            get_a(q, db_name, table_name)
        logging.info('All Data saved to database!')
    else:
        logging.info('No new data to update.')

if __name__ == '__main__':
    fire.Fire(main)