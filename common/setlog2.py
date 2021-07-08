import logging, os
import logging.handlers
from logging.handlers import SMTPHandler
'''
项目1，纯Python项目：
    在 holo_reveal.py 中：
    
    from common import setlog
    
    if __name__ == '__main__':
    
        timeUtil = Timeutils()
        appname = os.path.splitext(os.path.basename(__file__))[0]
        env = getEnv(sys.argv[1:])
        logger = setlog2.set_logger(f"{appname}.{env}")
        logger.info (f"{appname}开始运行了...")
    
        logger.error('need help...') # 此时，会发送邮件给服务的关注者
    预置 holodata@enovatemotors.com 作为发送方     
    
项目2，Flask项目：
    在 main app中：
    from common.setlog2 import set_logger
    
    if __name__ == "__main__":
    logger = set_logger(os.path.splitext(os.path.basename(__file__))[0])
    logger.info (f"{appname}开始运行了...")

项目3，Flask+blueprint项目
    在其他 py中：
    with app.app_context():
        logger.info("在blueprint中运行的info")
'''

logger = logging.getLogger()

def set_logger(logfilename = 'app'):
    logger = logging.getLogger()

    # 自行设置log文件的明明，默认是程序入口的所在文件名
    LOG_basename = f'{logfilename}.log'

    # 自行修改log文件的存储路径，默认是程序运行的同级 log/ 下
    LOG_path = 'log/'

    # 完整的日志存储文件路径和名称
    if not os.path.exists(LOG_path):
        os.makedirs(LOG_path)
    LOG_FILENAME = os.path.join(LOG_path, LOG_basename)

    # logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(process)d-%(threadName)s - '
                                  '%(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    # 添加在console显示
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 添加在log文件中输出
    file_handler = logging.handlers.TimedRotatingFileHandler(
        LOG_FILENAME,  when="D", interval=1, backupCount=90, encoding="utf-8", delay=False, utc=True)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Email Handler
    mail_handler = SMTPHandler(
        mailhost=('mail.enovatemotors.com', 587),
        fromaddr='holodata@enovatemotors.com',
        toaddrs=['guoliang@enovatemotors.com'],
        subject=LOG_basename + ': Application Error',
        secure=("", ""),
        credentials=('holodata', '@123qwe')
    )
    mail_handler.setLevel(logging.ERROR)
    mail_handler.setFormatter(logging.Formatter(
        "[%(asctime)s][%(module)s:%(lineno)d][%(levelname)s][%(thread)d] - %(message)s"
    ))
    # logger.addHandler(mail_handler)


    logger.info("setlog2.py is runing....")
    return logger