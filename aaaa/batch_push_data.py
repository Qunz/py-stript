
import path_config
from common.connect_mysql import connect_mysql
from common.connect_mysql import MySQLConnectionPool
from common.logger import logger

mysql = MySQLConnectionPool()

def query_table_batch(table_name, last_id=0, batch_size=100):
    """
    返回 id > last_id 的一批记录，按 id 升序，数量由 batch_size 决定。
    """
    conn = connect_mysql()
    cursor = conn.cursor()
    sql = f"SELECT * FROM {table_name} WHERE id > %s ORDER BY id ASC LIMIT %s"
    logger.info(f"Executing SQL: {sql} with last_id={last_id}, batch_size={batch_size}")
    cursor.execute(sql, (last_id, batch_size))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def batch_process_table(table_name, batch_size=100):
    """
    按批次处理表数据：
    - 每次查询 batch_size 条（按 id 升序，id 必须存在）。
    """
    last_id = 0
    while True:
        sql = f"SELECT * FROM {table_name} WHERE id > %s ORDER BY id ASC LIMIT %s"
        rows = mysql.select_all(sql, (last_id, batch_size))
        # rows = query_table_batch(table_name, last_id, batch_size)
        if not rows:
            break

        for row in rows:
            print(row)
            last_id = row.get("id", last_id)

        logger.info(f"Processed batch up to id={last_id}, fetched {len(rows)} rows.")


if __name__ == "__main__":
    table_name = "family_user"  # 替换为你的表名
    batch_process_table(table_name)