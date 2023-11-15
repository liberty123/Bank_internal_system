import time
import numpy as np
import pandas as pd
import pymysql
import tensorflow as tf
from config import configer

# get database config
host = configer.get('database', 'host')
port = int(configer.get('database', 'port'))
user = configer.get('database', 'user')
password = configer.get('database', 'password')
db = configer.get('database', 'db')
table = configer.get('database', 'table')

# load model
model = tf.keras.models.load_model('model.h5')

# connect mysql
conn = pymysql.connect(
    host=host,
    user=user,
    password=password,
    port=port,
    db=db,
    charset='utf8',
    autocommit=True
)
cur = conn.cursor()
# SQL
sql = "select * from {} WHERE loanstatus = ''; ".format(table)


def fill_mort_acc(total_acc, mort_acc, total_acc_avg):
    """
    Accept the total_acc and mort_acc values for each row
    Checks if mort_acc is NaN, and if so, it returns the avg mort_acc value
    """
    if np.isnan(mort_acc):
        return total_acc_avg[total_acc]
    else:
        return mort_acc


def bulid_data(i):
    """
    :param i: sql query result
    :return: dict
    """
    d = {
        "loan_amnt": i[18],
        "term": i[19],
        "int_rate": i[20],
        "installment": i[21],
        "grade": i[22],
        "sub_grade": i[23],
        "home_ownership": i[24],
        "annual_inc": i[25],
        "verification_status": i[26],
        "issue_d": i[27],
        "purpose": i[29],
        "dti": i[30],
        "earliest_cr_line": i[31],
        "open_acc": i[32],
        "pub_rec": i[33],
        "revol_bal": i[34],
        "revol_util": i[35],
        "total_acc": i[36],
        "initial_list_status": i[37],
        "application_type": i[38],
        "pub_rec_bankruptcies": i[39],
        "mort_acc": i[40],
        "A2": 0,
        "A3": 0,
        "A4": 0,
        "A5": 0,
        "B1": 0,
        "B2": 0,
        "B3": 0,
        "B4": 0,
        "B5": 0,
        "C1": 0,
        "C2": 0,
        "C3": 0,
        "C4": 0,
        "C5": 0,
        "D1": 0,
        "D2": 0,
        "D3": 0,
        "D4": 0,
        "D5": 0,
        "E1": 0,
        "E2": 0,
        "E3": 0,
        "E4": 0,
        "E5": 0,
        "F1": 0,
        "F2": 0,
        "F3": 0,
        "F4": 0,
        "F5": 0,
        "G1": 0,
        "G2": 0,
        "G3": 0,
        "G4": 0,
        "G5": 0,
        "verification_status_Source Verified": 0,
        "verification_status_Verified": 0,
        "application_type_Joint App": 0,
        "initial_list_status_w": 0,
        "purpose_credit_card": 0,
        "purpose_debt_consolidation": 0,
        "purpose_educational": 0,
        "purpose_home_improvement": 0,
        "purpose_house": 0,
        "purpose_major_purchase": 0,
        "purpose_medical": 0,
        "purpose_moving": 0,
        "purpose_other": 0,
        "purpose_renewable_energy": 0,
        "purpose_small_business": 0,
        "purpose_vacation": 0,
        "purpose_wedding": 0,
        "OTHER": 0,
        "OWN": 0,
        "RENT": 0
    }

    if d['sub_grade'] != 'A1':
        d[d['sub_grade']] = 1
    # verification_status
    if d['verification_status'] == 'Source Verified':
        d['verification_status_Source Verified'] = 1
    elif d['verification_status'] == 'Verified':
        d['verification_status_Verified'] = 1
    # application_type
    if d['application_type'] == 'Joint App':
        d['application_type_Joint App'] = 1
    # initial_list_status
    if d['initial_list_status'] == 'w':
        d['initial_list_status_w'] = 1
    # purpose
    if d['purpose'] == 'credit_card':
        d["purpose_credit_card"] = 1
    elif d['purpose'] == 'debt_consolidation':
        d["purpose_debt_consolidation"] = 1
    elif d['purpose'] == 'educational':
        d["purpose_educational"] = 1
    elif d['purpose'] == 'home_improvement':
        d["purpose_home_improvement"] = 1
    elif d['purpose'] == 'house':
        d["purpose_house"] = 1
    elif d['purpose'] == 'major_purchase':
        d["purpose_major_purchase"] = 1
    elif d['purpose'] == 'medical':
        d["purpose_medical"] = 1
    elif d['purpose'] == 'moving':
        d["purpose_moving"] = 1
    elif d['purpose'] == 'other':
        d["purpose_other"] = 1
    elif d['purpose'] == "renewable_energy":
        d["purpose_renewable_energy"] = 1
    elif d['purpose'] == 'small_business':
        d["purpose_small_business"] = 1
    elif d['purpose'] == 'vacation':
        d["purpose_vacation"] = 1
    elif d['purpose'] == 'wedding':
        d["purpose_wedding"] = 1
    if d['home_ownership'] == 'RENT':
        d["RENT"] = 1
    elif d['home_ownership'] == 'OWN':
        d["OWN"] = 1
    else:
        d["OTHER"] = 1
    return d


def data_preprocess(d):
    """
    :param d:  builded data
    :return: dataform data
    """
    df = pd.DataFrame([d])
    df['term'] = df['term'].apply(lambda term: int(term[:3]))
    df['earliest_cr_year'] = df['earliest_cr_line'].apply(lambda date: int(date[-4:]))
    df = df.drop(['grade', 'verification_status', 'application_type', 'initial_list_status', 'purpose', 'sub_grade',
                  'home_ownership', 'issue_d', 'earliest_cr_line'], axis=1)
    return df


def main():
    while True:
        #execute
        cur.execute(sql)
        data = cur.fetchall()
        if data:
            i = data[0]
            # build data from sql query result
            d = bulid_data(i)
            # preprocess data
            df = data_preprocess(d)
            X_test = df.values
            X_test = X_test.astype(np.float32)
            # predict
            predictions = model.predict_classes(X_test)
            if predictions[0][0] == 1:
                Suggestion = "Recommended to adopt"
            else:
                Suggestion = "Recommendation not adopted"
            update_sqli = "update {} set loanstatus='{}' where id={}".format(table, Suggestion, i[0])
            cur.execute(update_sqli)
            print("end one")
        else:
            print("no new data")
        time.sleep(30)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
