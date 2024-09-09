import pandas as pd
import akshare as ak
import fire
import sqlite3
import time

def create_qs(end_y, end_q):
    quarters = ['0331', '0630', '0930', '1231']
    qs = [f'{year}{quarter}' for year in range(2010, end_y + 1) for quarter in quarters]
    if end_q < 4:
        qs = qs[:-4 + end_q]
    return qs

# def get_a(rdate):
    # df = ak.stock_yjbb_em(date=rdate)
    # df['rdate'] = rdate
    # print(f'Finished {rdate}')
    # time.sleep(2)
    # return df
# the following func can rewrite to decorator format
def get_a(rdate, db_name, table_name):
    max_retries = 3
    retry_delay = 2  # 重试间隔时间（秒）
    for attempt in range(max_retries):
        try:
            df = ak.stock_yjbb_em(date=rdate)
            df['rdate'] = rdate
            update_db(df, db_name, table_name)
            print(f'Finished {rdate}')
            time.sleep(2)
            return df
        except Exception as e:
            print(f'Attempt {attempt + 1} failed: {e}')
            if attempt < max_retries - 1:
                print(f'Retrying in {retry_delay} seconds...')
                time.sleep(retry_delay)
            else:
                print(f'Failed to get data for {rdate} after {max_retries} attempts.')
                return None

def get_yjbbs(qs, db_name, table_name):
    dfs = [get_a(q, db_name, table_name) for q in qs]
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        print('No valid data retrieved.')
        return None
    merged_df = pd.concat(dfs, ignore_index=True)
    fn = f'yjbb{qs[-1]}.pkl.gz'
    merged_df.to_pickle(fn)
    print('All Data saved!')
    return merged_df

def create_db(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table with all columns
    columns = ', '.join([f'"{col}"' for col in df.columns])
    create_table_sql = f'''
        CREATE TABLE {table_name} (
            {columns},
            PRIMARY KEY ("rdate", "股票代码")
        )
    '''
    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()
    print('Database created!')

def update_db(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Insert data using INSERT OR IGNORE
    columns = ', '.join([f'"{col}"' for col in df.columns])
    insert_sql = f'''
        INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({', '.join(['?'] * len(df.columns))})
    '''
    data = [tuple(x) for x in df.values]
    cursor.executemany(insert_sql, data)
    inserted_rows = cursor.rowcount
    ignored_rows = len(data) - inserted_rows
    conn.commit()
    conn.close()
    print(f'Data saved to database! Inserted {inserted_rows} rows, ignored {ignored_rows} rows.')

def load_last_update_date(db_name, table_name):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f'SELECT MAX("rdate") FROM {table_name}')
        last_date = cursor.fetchone()[0]
        conn.close()
        return last_date
    except sqlite3.OperationalError:
        print('Database does not exist or table is empty. Rebuilding database...')
        return None

def main(end_y, end_q):
    """
    end_y: 4 digit int eg.2024
    end_q: int, one of (1, 2, 3, 4)
    """
    db_name = 'yjbb.db'
    table_name = 'yjbb_data'

    last_date = load_last_update_date(db_name, table_name)
    if last_date is None:
        last_date = '20100331'  # 假设最早的数据从2010年第一季度开始
        create_db(None, db_name, table_name)

    qs = create_qs(end_y, end_q)
    qs = [q for q in qs if q >= last_date]
    if qs:
        print(f'Updating data from {qs[0]} to {qs[-1]}...')
        get_yjbbs(qs, db_name, table_name)
    else:
        print('No new data to update.')

if __name__ == '__main__':
    fire.Fire(main)