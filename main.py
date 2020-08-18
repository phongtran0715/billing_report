import ftplib
import os
from configparser import SafeConfigParser
import smtplib
import ssl
import datetime

import pandas as pd
import simplemysql
from os import path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import re
import numpy as np


def send_email(receiver_email, title, body, attachment=None):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = title
    msg.attach(MIMEText(body, 'plain'))

    if attachment is None:
        pass
    else:
        if path.exists(attachment):
            # open the file to be sent
            file = open(attachment, "rb")
            # instance of MIMEBase and named as p
            p = MIMEBase('application', 'octet-stream')
            # To change the payload into encoded form
            p.set_payload(file.read())
            # encode into base64
            encoders.encode_base64(p)
            p.add_header('Content-Disposition', "attachment; filename= %s" % attachment)
            # attach the instance 'p' to instance 'msg'
            msg.attach(p)

    s = smtplib.SMTP(smtp_server, smtp_port)
    # start TLS for security
    s.starttls()
    # Authentication
    s.login(sender_email, password)
    # Converts the Multipart msg into a string
    text = msg.as_string()
    # sending the mail
    s.sendmail(sender_email, receiver_email, text)
    s.quit()


def validate_excel_name(file_name):
    regex = r"((\d{2})-(\d{2})-(\d{4})-)(([A-Z]+[0-9]+)+)-(RAW)-(\w+)-([A-Z]+)-(\d{2})-(\d{4})(.XLSX)"
    regex_check = re.compile(regex)

    # test_str = "09-08-2020-DA0005D50020130-RAW-DEL1L0AP-LTL-08-2020.XLSX"

    if regex_check.search(file_name):
        matches = re.finditer(regex, file_name, re.MULTILINE)

        for matchNum, match in enumerate(matches, start=1):

            for groupNum in range(0, len(match.groups())):
                groupNum = groupNum + 1

                # check Day is valid
                if groupNum == 2:
                    day_string = match.group(groupNum)
                    if 0 > int(day_string) or int(day_string) > 32:
                        return False
                        pass

                # check Month is valid
                if groupNum == 3 or groupNum == 10:
                    month_string = match.group(groupNum)
                    if 0 > int(month_string) or int(month_string) > 12:
                        return False
                        pass

                # check Year is valid
                if groupNum == 4 or groupNum == 11:
                    year_string = match.group(groupNum)
                    if 0 > int(year_string) or int(year_string) > 9999:
                        return False
                        pass

                # check Extension is correct or not
                if groupNum == 12:
                    extension_string = match.group(groupNum)
                    if extension_string != '.XLSX' and extension_string != '.xlsx':
                        return False
                        pass

        return True


def extractInfoFromFileName():
    file_prop = file_name.split('.')[0].split('-')
    data = dict()
    data["day"] = file_prop[0]
    data["month"] = file_prop[1]
    data["year"] = file_prop[2]
    data["code"] = file_prop[3]
    data["user_name"] = file_prop[5]
    data["billing_type"] = file_prop[6]
    data["billing_month"] = file_prop[7]
    data["billing_year"] = file_prop[8]
    return data


def get_kn_ref_no():
    sql_query = "SELECT MAX(KNREFNO) FROM disinftltemplate WHERE BILLING_YEAR = '" + file_data['billing_year'] \
                + "' AND BILLING_MONTH = '" + file_data['billing_month'] + "' AND CHILD_CLIENT_CODE = '" + file_data[
                    'code'] \
                + "' AND TYPE='" + file_data['billing_type'] + "';"
    val = sql_conn.query(sql_query)
    try:
        result = int(val) + 1
    except:
        result = -1
    return result


def check_record_existed():
    sql = "SELECT COUNT(*) FROM dthrawdata WHERE CHILD_CLIENT_CODE = '" + file_data['code'] \
          + "' AND BILLING_MONTH = '" + file_data['billing_month'] + \
          "' AND BILLING_YEAR = '" + file_data['billing_year'] + "';"
    val = sql_conn.query(sql)
    try:
        result = int(val.fetchone()[0])
        if result > 0:
            return True
        else:
            return False
    except:
        print("check_record_existed is false")
        return False


def insert_dthrawdata(item):
    pass


def insert_fileimport():
    sql_conn.insert('fileimport', {'file_name_orig': file_name,
                                   'file_name_db': file_name,
                                   'file_location': os.path.join(ftp_source, file_name),
                                   'file_for_client': file_data['code'],
                                   'file_for_country': 'IN',
                                   'cby': '1',
                                   'cbyname': file_data['user_name'],
                                   'cbyip': '0.0.0.0',
                                   'file_ac_path': os.path.join(ftp_source, file_name),
                                   'ctime': datetime.datetime.now()})


if __name__ == '__main__':
    # read info from config file
    parser = SafeConfigParser()
    parser.read('app.conf')
    # load ftp info
    ftp_server = parser.get('ftp-info', 'server')
    ftp_user = parser.get('ftp-info', 'user')
    ftp_password = parser.get('ftp-info', 'password')
    ftp_source = parser.get('ftp-info', 'source')
    ftp_dest = parser.get('ftp-info', 'dest')

    # load email info
    smtp_server = parser.get('email-info', 'smtp_server')
    smtp_port = parser.get('email-info', 'port')
    sender_email = parser.get('email-info', 'sender_email')
    password = parser.get('email-info', 'password')

    # load database info
    db_server = parser.get('database-info', 'server')
    db_user = parser.get('database-info', 'user')
    db_password = parser.get('database-info', 'password')
    db_port = parser.get('database-info', 'port')
    db_name = parser.get('database-info', 'db_name')

    admin_email = parser.get('global', 'admin_email')

    # open ftp connection
    files = []
    with ftplib.FTP(ftp_server) as ftp:
        try:
            ftp.encoding = "utf-8"
            ftp.login(ftp_user, ftp_password)
            # change the current working directory to source
            ftp.cwd(ftp_source)
            for file_name in ftp.nlst():
                files.append(file_name)
                print(f"{file_name}")
                # Validate excel file name
                # Download file from FTP to local
                local_file = os.path.join('/tmp/', file_name)
                ftp.retrbinary("RETR " + file_name, open(local_file, 'wb').write)
                if not validate_excel_name(file_name):
                    # send email to user
                    bodyMsg = "File name is invalid : {}. Please update and re-upload again!".format(file_name)
                    send_email(admin_email, "[Report] File name is invalid",
                               bodyMsg, local_file)
                    print("File name : {} is not valid".format(file_name))
                    continue
                raw_df = pd.read_excel(local_file)
                excel_df = raw_df.replace(np.nan, '', regex=True)

                # print(excel_df)

                # init database
                sql_conn = simplemysql.SimpleMysql(db=db_name, user=db_user, passwd=db_password,
                                                   host=db_server, port=db_port, autocommit=True)
                file_data = extractInfoFromFileName()
                # Insert file data to fileimport table
                insert_fileimport()
                # Insert file data to dthrawdata table
                file_import_id = sql_conn.lastId()
                sql_query = "SELECT ctime FROM fileimport WHERE fileimport_id = 27;"
                itime = sql_conn.getOne('fileimport', "*", ('fileimport_id=%s', [file_import_id]))['ctime']
                kn_ref_no = get_kn_ref_no()
                kn_job_ref = "{}-{}-{}-{}-{}".format(file_data['billing_year'],
                                                     file_data['code'],
                                                     file_data['billing_type'],
                                                     file_data['billing_month'],
                                                     kn_ref_no)
                if check_record_existed():
                    # update data
                    print('Update data')
                    for index, row in excel_df.iterrows():
                        sql_conn.update('dthrawdata', {'IMPID': file_import_id,
                                                       'IBY': file_data['user_name'],
                                                       'ITIME': itime,
                                                       'RSTATUS': 'PENDING',
                                                       'TYPE': file_data['billing_type'],
                                                       'DISPATCH_TYPE': file_data['billing_type'],
                                                       'KNREFNO': kn_ref_no,
                                                       'KN_JOB_REF': kn_job_ref,
                                                       'RAWDATA1': row['SOURCE NO/MOVE ORDER NUMBER'],
                                                       'RAWDATA2': row['TRANSACTION DATE'],
                                                       'RAWDATA3': row['DC NUMBER'],
                                                       'RAWDATA4': row['DC DATE'],
                                                       'RAWDATA5': row['ITEM CODE'],
                                                       'RAWDATA6': row['ITEM DESCRIPTION'],
                                                       'RAWDATA7': row['TRANSACTED QUANTITY'],
                                                       'RAWDATA8': row['TOTAL VALUE OF THE TRANSACTED QUANTITY'],
                                                       'RAWDATA9': row['FROM SUBINVENTORY LOCATOR CODE ADDRESS'],
                                                       'RAWDATA10': row['TO SUBINVENTORY LOCATOR CODE']},
                                        ("BILLING_YEAR=%s AND BILLING_MONTH=%s AND CHILD_CLIENT_CODE=%s",
                                         ([file_data['billing_year'], file_data['billing_month'], file_data['code']]))
                                        )
                else:
                    print('Insert data')
                    for index, row in excel_df.iterrows():
                        sql_conn.insert('dthrawdata', {'IMPID': file_import_id,
                                                       'IBY': file_data['user_name'],
                                                       'ITIME': itime,
                                                       'RSTATUS': 'PENDING',
                                                       'CHILD_CLIENT_CODE': file_data['code'],
                                                       'TYPE': file_data['billing_type'],
                                                       'DISPATCH_TYPE': file_data['billing_type'],
                                                       'BILLING_YEAR': file_data['billing_year'],
                                                       'BILLING_MONTH': file_data['billing_month'],
                                                       'KNREFNO': kn_ref_no,
                                                       'KN_JOB_REF': kn_job_ref,
                                                       'RAWDATA1': row['SOURCE NO/MOVE ORDER NUMBER'],
                                                       'RAWDATA2': row['TRANSACTION DATE'],
                                                       'RAWDATA3': row['DC NUMBER'],
                                                       'RAWDATA4': row['DC DATE'],
                                                       'RAWDATA5': row['ITEM CODE'],
                                                       'RAWDATA6': row['ITEM DESCRIPTION'],
                                                       'RAWDATA7': row['TRANSACTED QUANTITY'],
                                                       'RAWDATA8': row['TOTAL VALUE OF THE TRANSACTED QUANTITY'],
                                                       'RAWDATA9': row['FROM SUBINVENTORY LOCATOR CODE ADDRESS'],
                                                       'RAWDATA10': row['TO SUBINVENTORY LOCATOR CODE']})
                # TODO : create excel report
                # TODO : move processed file to destination folder
            ftp.quit()
        except ftplib.all_errors as e:
            print('Error:', e)
    print("Done")
