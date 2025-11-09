
from typing import Dict, Any, Optional
import os
import logging
import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB


logger = logging.getLogger(__name__)


# 默认使用环境变量
DEFAULT_ENV = os.getenv("APP_ENV", "aaaa")

# 连接池字典，按环境区分
CONNECTION_POOLS: Dict[str, PooledDB] = {}

# 默认连接池配置
DEFAULT_POOL_CONFIG = {
    "mincached": 2,      # 初始空闲连接数
    "maxcached": 5,      # 最大空闲连接数
    "maxshared": 3,      # 最大共享连接数
    "maxconnections": 10, # 最大连接数
    "blocking": True,    # 当连接数达到最大时是否阻塞等待
    "maxusage": 100,     # 单个连接最大重用次数
    "setsession": [],    # 可选的会话命令列表
    "ping": 1,           # 检查连接是否存活 (0=从不, 1=请求时, 2=从缓存获取时, 4=创建时, 7=总是)
}


# 根据环境变量构建数据库配置字典
def build_db_config(env_prefix: str) -> Dict[str, Any]:
    return {
        "host": os.getenv(f"DB_{env_prefix}_HOST", "127.0.0.1"),
        "port": int(os.getenv(f"DB_{env_prefix}_PORT", 3306)),
        "user": os.getenv(f"DB_{env_prefix}_USER", "root"),
        "password": os.getenv(f"DB_{env_prefix}_PASSWORD", "password"),
        "database": os.getenv(f"DB_{env_prefix}_NAME", ""),
        "charset": "utf8mb4",
        "cursorclass": DictCursor,
        "autocommit": True,
        "connect_timeout": 10,
    }


# 全局数据库配置字典，按环境区分
CONFIG: Dict[str, Dict[str, Any]] = {
    "AAAA": build_db_config("AAAA")
}


# 获取指定环境的数据库配置，默认使用环境变量 APP_ENV
def get_db_config(env: Optional[str] = None) -> Dict[str, Any]:
    env = (env or DEFAULT_ENV).upper()
    if env not in CONFIG:
        raise KeyError(f"未找到数据库配置: {env}. 可用环境：{list(CONFIG.keys())}")
    # 返回一份拷贝以便后续不改动全局配置
    cfg = dict(CONFIG[env])
    return cfg

def get_connection_pool(env: Optional[str] = None, pool_config: Optional[Dict[str, Any]] = None) -> PooledDB:
    """
    获取或创建指定环境的连接池
    """
    env = (env or DEFAULT_ENV).upper()
    
    # 如果连接池已存在，直接返回
    if env in CONNECTION_POOLS:
        return CONNECTION_POOLS[env]
    
    # 获取数据库配置
    db_config = get_db_config(env)
    
    # 合并连接池配置
    final_pool_config = DEFAULT_POOL_CONFIG.copy()
    if pool_config:
        final_pool_config.update(pool_config)
    
    # 创建连接池
    try:
        pool = PooledDB(
            creator=pymysql,
            **db_config,
            **final_pool_config
        )
        
        CONNECTION_POOLS[env] = pool
        logger.info(f"创建了 {env} 环境的MySQL连接池")
        return pool
        
    except Exception as e:
        logger.error(f"创建 {env} 环境连接池失败: {e}")
        raise


def connect_mysql(env: Optional[str] = None, use_pool: bool = True) -> "pymysql.connections.Connection":
    """
    获取MySQL连接
    - use_pool: 是否使用连接池（默认True）
    - env: 环境名称
    - overrides: 可覆盖的配置项
    """
    if pymysql is None:
        raise ImportError("缺少 pymysql 库，请先安装：pip install pymysql")
    
    # 使用连接池获取连接
    try:
        pool = get_connection_pool(env)
        conn = pool.connection()
        logger.debug(f"从连接池获取连接 (env: {env or DEFAULT_ENV})")
        return conn
    except Exception as e:
        logger.error(f"从连接池获取连接失败: {e}")


# 上下文管理器，用于自动管理连接
class MySQLConnection:
    """
    上下文管理器，自动管理连接的获取和释放
    """
    def __init__(self, env: Optional[str] = None, use_pool: bool = True, **overrides):
        self.env = env
        self.use_pool = use_pool
        self.overrides = overrides
        self.conn = None
    
    def __enter__(self) -> "pymysql.connections.Connection":
        self.conn = connect_mysql(self.env, self.use_pool, **self.overrides)
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def open(self):
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)  # 表示读取的数据为字典类型
        return self.conn, self.cursor

    def close(self, cursor, conn):
        cursor.close()
        conn.close()

    def select_one(self, sql, *args):
        """查询单条数据"""
        self.cursor.execute(sql, args)
        return self.cursor.fetchone()

    def select_all(self, sql, args):
        """查询多条数据"""
        self.cursor.execute(sql, args)
        return self.cursor.fetchall()

    def insert_one(self, sql, args):
        """插入单条数据"""
        self.execute(sql, args, isNeed=True)

    def insert_all(self, sql, datas):
        """插入多条批量插入"""
        conn, cursor = self.open()
        try:
            cursor.executemany(sql, datas)
            conn.commit()
            return {'result': True, 'id': int(cursor.lastrowid)}
        except Exception as err:
            conn.rollback()
            return {'result': False, 'err': err}

    def update_one(self, sql, args):
        """更新数据"""
        self.execute(sql, args, isNeed=True)

    def execute(self, sql, args, isNeed=False):
        """
        执行
        :param isNeed 是否需要回滚
        """
        conn, cursor = self.open()
        if isNeed:
            try:
                cursor.execute(sql, args)
                conn.commit()
            except:
                conn.rollback()
        else:
            cursor.execute(sql, args)
            conn.commit()
        self.close(conn, cursor)