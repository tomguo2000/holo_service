from flask import Flask,request,render_template,send_from_directory
import os
from common.config import CONFIG
from common.timeUtils import Timeutils
from api.overall_by_vin import *
from api.tj32960 import *
from api.tjms import *
from api.daily import *
from api.ecuversion import *
from api.holoview import *
from common.setlog2 import set_logger
from flask_gzip import Gzip


app = Flask(__name__)
app.register_blueprint(overall_by_vin, url_prefix="/api")
app.register_blueprint(daily, url_prefix="/api/daily")
app.register_blueprint(holoview, url_prefix="/api/holoview")
app.register_blueprint(tj32960, url_prefix="/api/tj32960")
app.register_blueprint(tjms, url_prefix="/api/tjms")
app.register_blueprint(ecuversion, url_prefix="/api/ecuversion")

gzip = Gzip(app)

@app.before_request
def logger_request_info():
    headers = {'Content-Type': request.headers.get('Content-Type'), 'Authorization': request.headers.get('Authorization')}
    X_Forwarded_For = request.headers.get('X-Forwarded-For')
    Fx_Remote_Addr = request.headers.get('Fx-Remote-Addr')
    logger.info(f"请求URL:{request.url}. 来源IP地址:{Fx_Remote_Addr}. 来源X_Forwarded_For地址:{X_Forwarded_For}"
                f"请求方法:{request.method}. HEADER:{headers}. BODY:{request.data}")


@app.after_request
def logger_response_info(response):
    X_Forwarded_For = request.headers.get('X-Forwarded-For')
    Fx_Remote_Addr = request.headers.get('Fx-Remote-Addr')
    logger.info(f"请求URL:{request.url}. 来源IP地址:{Fx_Remote_Addr}. 来源X_Forwarded_For地址:{X_Forwarded_For}. Response:{str(response.data)[:50]}")
    return response


appname = os.path.splitext(os.path.basename(__file__))[0]
env = CONFIG['env']
logger = set_logger(f"{appname}.{env}")
os.environ['HOLO_APPNAME']=appname


if __name__ == "__main__":
    logger.info (f"{appname}开始运行了...")
    app.run(host='0.0.0.0', port=8678, debug=False)