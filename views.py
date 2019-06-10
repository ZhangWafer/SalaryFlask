import os
import pandas as pd
import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, create_engine
from sqlalchemy import extract
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from werkzeug import security
import json


app = Flask(__name__)

app.config.from_object('config')
db = SQLAlchemy(app)
db.init_app(app)

print("服务器已启动....")
class Staff(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    工号 = db.Column(db.String(10), index=True, unique=True, nullable=False)
    姓名 = db.Column(db.String(20), nullable=False)
    登录密码 = db.Column(db.String(128))
    密码有效 = db.Column(db.Boolean)
    所属部门 = db.Column(db.String(50))

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

    def __repr__(self):
        return '<工号=%r>' % (self.工号)


# 固定项金额
class Fixed(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    工号 = db.Column(db.String(10), index=True,  nullable=False)
    个人账号 = db.Column(db.String(20))
    基本职能 = db.Column(db.Float)
    基本业绩 = db.Column(db.Float)
    业绩能力 = db.Column(db.Float)
    满勤补助 = db.Column(db.Float)
    住房补助 = db.Column(db.Float)
    职位补助 = db.Column(db.Float)
    绩效奖金 = db.Column(db.Float)
    工种补助 = db.Column(db.Float)
    职称补助 = db.Column(db.Float)
    固定加班费 = db.Column(db.Float)
    住房公积金 = db.Column(db.Float)
    社会保险费 = db.Column(db.Float)
    工会会费 = db.Column(db.Float)
    版本号 = db.Column(db.String(20))

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

    def __repr__(self):
        return '<工号=%r>' % (self.工号)


# 计算项金额
class Variable(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    工号 = db.Column(db.String(10), index=True, nullable=False)
    出勤天数 = db.Column(db.Float)
    夜勤补助 = db.Column(db.Float)
    交通补助 = db.Column(db.Float)
    其它补助 = db.Column(db.Float)
    加班费 = db.Column(db.Float)  # 正式系统要根据加班调休计算，暂时只进行导入
    所得税 = db.Column(db.Float)
    水电费 = db.Column(db.Float)
    欠勤 = db.Column(db.Float)
    其它 = db.Column(db.Float)  # 工资异常时用
    # 请假，时数
    事假 = db.Column(db.Float)
    病假 = db.Column(db.Float)
    旷工 = db.Column(db.Float)
    迟到早退 = db.Column(db.Float)
    婚假 = db.Column(db.Float)
    丧假 = db.Column(db.Float)
    产假 = db.Column(db.Float)
    年休假 = db.Column(db.Float)
    工伤假 = db.Column(db.Float)
    产检假 = db.Column(db.Float)
    陪产假 = db.Column(db.Float)
    流产假 = db.Column(db.Float)
    路程假 = db.Column(db.Float)
    其它假 = db.Column(db.Float)
    # 加班调休，时数
    调休时数 = db.Column(db.Float)
    平日时数 = db.Column(db.Float)
    休日时数 = db.Column(db.Float)
    假日时数 = db.Column(db.Float)
    # 总计
    应发合计 = db.Column(db.Float)
    实发合计 = db.Column(db.Float)

    日期 = db.Column(db.Date)

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

    def __repr__(self):
        return '<工号=%r>' % (self.工号)


# 加班调休情况表
class Attendance(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    工号 = db.Column(db.String(10), nullable=False)
    调休时数 = db.Column(db.Float)
    加班时数 = db.Column(db.Float)
    加班类型 = db.Column(db.Integer)  # 分为1平时加班，2休日加班，3假日加班
    日期 = db.Column(db.String(10))


    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

    def __repr__(self):
        return '<工号=%r>' % (self.工号)

db.create_all()

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,session_id')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS,HEAD')
    # 这里不能使用add方法，否则会出现 The 'Access-Control-Allow-Origin' header contains multiple values 的问题
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/account', methods=['POST'])
def login():
    # 获取传入参数
    userNo = request.form['userNo']
    pwd = request.form['pwd']

    data = Staff.query.filter_by(工号=userNo).first()
    if data is not None:
        if security.check_password_hash(data.登录密码, pwd):  # returns True
            # 转换返回参数为json
            data = {'result': True, 'valid': data.密码有效, 'errorMsg': ""}
        else:
            data = {'result': False, 'errorMsg': "密码不匹配"}
    else:
        data = {'result': False, 'errorMsg': "工号不存在"}
    return json.dumps(data)


@app.route('/change', methods=['POST'])
def change_password():
    # 获取传入参数
    user_no = request.form['userNo']
    pwd = request.form['pwd']

    data = Staff.query.filter_by(工号=user_no).first()
    if data is not None:
        data.登录密码 = security.generate_password_hash(pwd)
        data.密码有效 = True
        db.session.commit()
    return jsonify(result=True)


@app.route('/imformation', methods=['POST'])
def imformation():
    # 获取传入参数
    userNo = request.form['userNo']
    monthSelect = request.form['month']

    data = Fixed.query.filter_by(工号=userNo,版本号=monthSelect).first()
    # 人员表转字典去掉密码元素
    data1 = Staff.query.filter_by(工号=userNo).first()
    data1 = data1.to_dict()
    data1.pop('id')
    data1.pop('工号')
    data1.pop('登录密码')
    data1.pop('密码有效')
    # 可变值表
    data2 = Variable.query.filter_by(工号=userNo,日期=datetime.datetime.strptime((monthSelect+"-01"),'%Y-%m-%d').date()).first()
    # 加班调休表

    OTlist = {}
    TXlist = {}
    keynum = 1

    for icount in range(1,31):
        data3 = Attendance.query.filter_by(工号=userNo,日期=monthSelect+"-"+str(icount)).first()
        if data3 is None:
            return jsonify(Msg="没有数据")
        else:
            JB = "加班" + str(keynum) + "日"
            TX = "调休" + str(keynum) + "日"
            OTlist.update({str(JB): str(data3.加班时数)})
            TXlist.update({str(TX): str(data3.调休时数)})
            keynum += 1


    # 把多个字典拼在一起
    dictMerged2 = dict(data.to_dict(), **data2.to_dict(), **OTlist, **TXlist, **data1)

    return jsonify(dictMerged2)


@app.route('/file_upload', methods=['POST'])
def file_upload():
    if request.method == 'POST':
        # check if the post request has the file part
        file = request.files['fafafa']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            return jsonify(Msg="文件名不得为空")
        try:
            df = pd.read_excel(file,
                               usecols='B:O,Q:X,Y,Z:AC,AE:AG,AI:AZ,BB,AH,BD:DM', dtype={'工号': str, '个人账号': str})
            df.rename(columns={'迟到/早迟': '迟到早退'}, inplace=True)

            # df = df[df.工号 != "nan"]
            df.fillna(value=0, inplace=True)
            # df.dropna(subset=['hire_date'], inplace=True)
            df.replace(['管理部管理课', '管理部'], '管理课', inplace=True)
            ##############################
            staff = Staff.query.filter_by(工号='admin').first()
            if staff is None:
                staff = Staff(工号='admin', 姓名='管理员',
                              登录密码=security.generate_password_hash('888888'), 密码有效=False)
                db.session.add(staff)
            # 添加 staff
            for index, row in df.iterrows():
                staff = Staff.query.filter_by(工号=row.工号).first()
                if staff is None:
                    staff = Staff(工号=row.工号, 姓名=row.姓名, 所属部门=row.部门,
                                  登录密码=security.generate_password_hash('123456'), 密码有效=False)
                    db.session.add(staff)
                else:
                    db.session.所属部门 = row.部门
            # 添加 staff
            for index, row in df.iterrows():
                # 固定费用
                fixed = Fixed.query.filter_by(工号=row.工号,版本号=str(row.年)+"-"+str(row.月)).first()
                if fixed is None:
                    fixed = Fixed(工号=row.工号, 个人账号=row.个人账号,
                                  基本职能=row.基本职能, 基本业绩=row.基本业绩, 业绩能力=row.业绩能力,
                                  满勤补助=row.满勤补, 住房补助=row.住宅补, 职位补助=row.职位补,
                                  绩效奖金=row.绩效奖, 工种补助=row.工种补, 职称补助=row.职称补,
                                  固定加班费=row.固定加班费, 住房公积金=row.住房公积金,
                                  社会保险费=row.保险费, 工会会费=row.工会会费 ,版本号=str(row.年)+"-"+str(row.月))
                    db.session.add(fixed)

                # 计算费用
                variable = Variable.query.filter_by(工号=row.工号, 日期=datetime.datetime.strptime((str(row.年)+"-"+str(row.月)+"-01"),'%Y-%m-%d').date()).first()
                if variable is None:
                    variable = Variable(工号=row.工号,
                                        出勤天数=row.出勤天数, 夜勤补助=row.夜勤补, 交通补助=row.交通补, 其它补助=row.其它补,
                                        加班费=row.加班费, 所得税=row.个人所得税, 水电费=row.水电费, 欠勤=row.欠勤, 其它=row.其它,
                                        事假=row.事假, 病假=row.病假, 旷工=row.旷工, 迟到早退=row.迟到早退, 婚假=row.婚假,
                                        丧假=row.丧假, 产假=row.产假, 年休假=row.年休假, 工伤假=row.工伤假, 产检假=row.产检假,
                                        陪产假=row.陪产假, 流产假=row.流产假, 路程假=row.路程假, 其它假=row.其它假,
                                        调休时数=row.调休时数, 平日时数=row.平日时数,
                                        休日时数=row.休日时数, 假日时数=row.假日时数, 应发合计=row.应发合计, 实发合计=row.实发合计,
                                        日期=datetime.datetime.strptime((str(row.年)+"-"+str(row.月)+"-01"), "%Y-%m-%d").date())
                    db.session.add(variable)

                # 加班调休表
                attendance = Attendance.query.filter_by(工号=row.工号,日期= str(row.年)+"-"+str(row.月) + "-1" ).first()
                if attendance is None:
                    for i in range(31):
                        attendance = Attendance(工号=row.工号, 调休时数=float(row["调休%s日" % str(i + 1)]),
                                                加班时数=float(row["加班%s日" % str(i + 1)]),
                                                日期= str(row.年)+"-"+str(row.月) + "-" + str(i + 1))
                        db.session.add(attendance)

            # 提交即保存到数据库:
            db.session.commit()
            return jsonify(Msg=True)
        except Exception as e:
            print(str(e))
            return jsonify(Msg=False)


@app.route('/reset', methods=['POST'])
def reset_password():
    # 获取传入参数
    user_no = request.form['userNo']
    data = Staff.query.filter_by(工号=user_no).first()
    if data is not None:
        data.登录密码 = security.generate_password_hash("123456")
        data.密码有效 = True
        db.session.commit()
    return jsonify(result=True)



if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
