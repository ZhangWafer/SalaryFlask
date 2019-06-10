import os
import pandas as pd
from sqlalchemy import create_engine, types, Column, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
from werkzeug import security

basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'app.db'))
df = pd.read_excel('1月工资条全部.xlsx',
                   usecols='B:O,Q:X,Y,Z:AC,AE:AG,AI:AZ,BB,AH,BD:DM', dtype={'工号': str, '个人账号': str})

# df = pd.read_excel('工资201809.xlsx', sheet_name='工资计算模板', header=7,
#                    usecols='C,F:H,K,M:P,R:T,Y,AD,AJ', dtype={'工号': str, '个人账号': str})
# df.columns = ['hire_date', "employee", "username", 'department', 'skill_grade', "bank_account",
#               "base_skills", "performance", "job_ability", "housing_subsidy", "leader_subsidy",
#               "fixed_overtime", "traffic_subsidy", "housing_fund", "trade_union"]
df.rename(columns={'迟到/早迟': '迟到早退'}, inplace=True)

# df = df[df.工号 != "nan"]
df.fillna(value=0, inplace=True)
# df.dropna(subset=['hire_date'], inplace=True)
df.replace(['管理部管理课', '管理部'], '管理课', inplace=True)

BaseModel = declarative_base()


class Staff(BaseModel):
    __tablename__ = 'staff'

    id = Column(types.Integer, primary_key=True)
    工号 = Column(types.String(10), index=True, unique=True, nullable=False)
    姓名 = Column(types.String(20), nullable=False)
    登录密码 = Column(types.String(128))
    密码有效 = Column(types.Boolean)
    所属部门 = Column(types.String(50))


class Fixed(BaseModel):
    __tablename__ = 'fixed'
    id = Column(types.Integer, primary_key=True)

    工号 = Column(types.String(10), index=True, unique=True, nullable=False)
    个人账号 = Column(types.String(20))
    基本职能 = Column(types.Float)
    基本业绩 = Column(types.Float)
    业绩能力 = Column(types.Float)
    满勤补助 = Column(types.Float)
    住房补助 = Column(types.Float)
    职位补助 = Column(types.Float)
    绩效奖金 = Column(types.Float)
    工种补助 = Column(types.Float)
    职称补助 = Column(types.Float)
    固定加班费 = Column(types.Float)
    住房公积金 = Column(types.Float)
    社会保险费 = Column(types.Float)
    工会会费 = Column(types.Float)


class Variable(BaseModel):
    __tablename__ = 'variable'
    id = Column(types.Integer, primary_key=True)

    工号 = Column(types.String(10), index=True, unique=True, nullable=False)
    出勤天数 = Column(types.Float)
    夜勤补助 = Column(types.Float)
    交通补助 = Column(types.Float)
    其它补助 = Column(types.Float)
    加班费 = Column(types.Float)  # 正式系统要根据加班调休计算，暂时只进行导入
    所得税 = Column(types.Float)
    水电费 = Column(types.Float)
    欠勤 = Column(types.Float)
    其它 = Column(types.Float)  # 工资异常时用
    # 请假，时数
    事假 = Column(types.Float)
    病假 = Column(types.Float)
    旷工 = Column(types.Float)
    迟到早退 = Column(types.Float)
    婚假 = Column(types.Float)
    丧假 = Column(types.Float)
    产假 = Column(types.Float)
    年休假 = Column(types.Float)
    工伤假 = Column(types.Float)
    产检假 = Column(types.Float)
    陪产假 = Column(types.Float)
    流产假 = Column(types.Float)
    路程假 = Column(types.Float)
    其它假 = Column(types.Float)
    # 加班调休，时数
    调休时数 = Column(types.Float)
    平日时数 = Column(types.Float)
    休日时数 = Column(types.Float)
    假日时数 = Column(types.Float)
    #总计
    应发合计 = Column(types.Float)
    实发合计 = Column(types.Float)

class Attendance(BaseModel):
    __tablename__ = 'attendance'
    id = Column(types.Integer, primary_key=True)
    工号 = Column(types.String(10), nullable=False)
    调休时数 = Column(types.Float)
    加班时数 = Column(types.Float)
    加班类型 = Column(types.Integer)#分为1平时加班，2休日加班，3假日加班
    日期=Column(types.String(10))

BaseModel.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# 添加 admin
staff = session.query(Staff).filter_by(工号='admin').first()
if staff is None:
    staff = Staff(工号='admin', 姓名='管理员',
                  登录密码=security.generate_password_hash('888888'), 密码有效=False)
    session.add(staff)
# 添加 staff
for index, row in df.iterrows():
    staff = session.query(Staff).filter_by(工号=row.工号).first()
    if staff is None:
        staff = Staff(工号=row.工号, 姓名=row.姓名, 所属部门=row.部门,
                      登录密码=security.generate_password_hash('123456'), 密码有效=False)
        session.add(staff)
    else:
        staff.所属部门 = row.部门

    # 固定费用
    fixed = session.query(Fixed).filter_by(工号=row.工号).first()
    if fixed is None:
        fixed = Fixed(工号=row.工号, 个人账号=row.个人账号,
                      基本职能=row.基本职能, 基本业绩=row.基本业绩, 业绩能力=row.业绩能力,
                      满勤补助=row.满勤补, 住房补助=row.住宅补, 职位补助=row.职位补,
                      绩效奖金=row.绩效奖, 工种补助=row.工种补, 职称补助=row.职称补,
                      固定加班费=row.固定加班费, 住房公积金=row.住房公积金,
                      社会保险费=row.保险费, 工会会费=row.工会会费)
        session.add(fixed)

    # 计算费用
    variable = session.query(Variable).filter_by(工号=row.工号).first()
    if variable is None:
        variable = Variable(工号=row.工号,
                            出勤天数=row.出勤天数, 夜勤补助=row.夜勤补, 交通补助=row.交通补, 其它补助=row.其它补,
                            加班费=row.加班费, 所得税=row.个人所得税, 水电费=row.水电费, 欠勤=row.欠勤, 其它=row.其它,
                            事假=row.事假, 病假=row.病假, 旷工=row.旷工, 迟到早退=row.迟到早退, 婚假=row.婚假,
                            丧假=row.丧假, 产假=row.产假, 年休假=row.年休假, 工伤假=row.工伤假, 产检假=row.产检假,
                            陪产假=row.陪产假, 流产假=row.流产假, 路程假=row.路程假, 其它假=row.其它假,
                            调休时数=row.调休时数, 平日时数=row.平日时数,
                            休日时数=row.休日时数, 假日时数=row.假日时数, 应发合计=row.应发合计, 实发合计=row.实发合计)
        session.add(variable)

    #加班调休表
    attendance = session.query(Attendance).filter_by(工号=row.工号).first()
    if attendance is None:
        for i in range(31):
            attendance = Attendance(工号=row.工号, 调休时数=float(row["调休%s日" % str(i+1)]), 加班时数=float(row["加班%s日" % str(i+1)]),
                                    日期=str(row.月)+"-"+str(i+1))
            session.add(attendance)

# 提交即保存到数据库:
session.commit()

# 关闭session:
session.close()
