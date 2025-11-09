import logging
import os
import sys

# 日志文件路径：运行文件所在目录
log_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'app.log')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('common_logger')
# 使用运行脚本的文件名（不含扩展名）作为 logger 名称，失败时回退到 'common_logger'
_script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0] or 'common_logger'
logger = logging.getLogger(_script_name)