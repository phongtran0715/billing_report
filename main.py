import ftplib
import os
import sys
from configparser import ConfigParser
from datetime import datetime
from logging import handlers

import pandas as pd
import re
import numpy as np
from threading import Thread, Event
import send_mail as send_email
import simplemysql as simple_mysql
import report as report
import logging


def scan():
    # open ftp connection
    files = []
    with ftplib.FTP(ftp_server) as ftp:
        try:
            ftp.encoding = "utf-8"
            ftp.login(ftp_user, ftp_password)
            # change the current working directory to source
            ftp.cwd(ftp_source)
            if len(ftp.nlst()) <= 0:
                logging.info("Not found any excel file on FTP server")
            for file_name in ftp.nlst():
                files.append(file_name)
                logging.info("Found new excel file : {}".format(file_name))

                # Download file from FTP to local
                logging.info("Download file to local")
                local_file = os.path.join(local_path, file_name)
                ftp.retrbinary("RETR " + file_name, open(local_file, 'wb').write)
                # validate file name
                if not validate_excel_name(file_name):
                    logging.info("File name is not valid. Send email to admin.")
                    # send email to user
                    body_msg = "File name is invalid : {}. Please update and re-upload again!".format(file_name)
                    send_email_obj.send_email(admin_email, "[Report] File name is invalid",
                                              body_msg, local_file)
                    continue
                raw_df = pd.read_excel(local_file)
                excel_df = raw_df.replace(np.nan, '', regex=True)
                excel_df = excel_df.astype(str)

                file_data = extractInfoFromFileName(file_name)
                # Insert file data to fileimport table
                insert_fileimport(file_name, file_data)
                # Insert file data to dthrawdata table
                file_import_id = sql_conn.lastId()
                itime = sql_conn.getOne('fileimport', "*", ('fileimport_id=%s', [file_import_id]))['ctime']
                if check_record_existed(file_data):
                    # update data
                    logging.info('Update data to database')
                    for index, row in excel_df.iterrows():
                        if not row['DC NUMBER']:
                            row['DC NUMBER'] = row['SOURCE NO/MOVE ORDER NUMBER']
                        if not row['DC DATE']:
                            row['DC DATE'] = row['TRANSACTION DATE']
                        sql_conn.update('dthrawdata', {'IMPID': file_import_id,
                                                       'IBY': file_data['user_name'],
                                                       'ITIME': '2020-08-23 17:51:15',
                                                       'RSTATUS': 'PENDING',
                                                       'TYPE': file_data['billing_type'],
                                                       'DISPATCH_TYPE': file_data['billing_type'],
                                                       # 'KNREFNO': kn_ref_no,
                                                       # 'KN_JOB_REF': kn_job_ref,
                                                       'RAWDATA1': row['SOURCE NO/MOVE ORDER NUMBER'],
                                                       'RAWDATA2': row['TRANSACTION DATE'],
                                                       'RAWDATA3': row['DC NUMBER'],
                                                       'RAWDATA4': row['DC DATE'],
                                                       'RAWDATA5': row['ITEM CODE'],
                                                       'RAWDATA6': row['ITEM DESCRIPTION'],
                                                       'RAWDATA7': row['TRANSACTED QUANTITY'],
                                                       'RAWDATA8': row['TOTAL VALUE OF THE TRANSACTED QUANTITY'],
                                                       'RAWDATA9': row['FROM SUBINVENTORY LOCATOR CODE'],
                                                       'RAWDATA10': row['TO SUBINVENTORY LOCATOR CODE']},
                                        ("BILLING_YEAR=%s AND BILLING_MONTH=%s AND CHILD_CLIENT_CODE=%s",
                                         ([file_data['billing_year'], file_data['billing_month'],
                                           file_data['code']]))
                                        )
                else:
                    logging.info('Insert data to database')
                    for index, row in excel_df.iterrows():
                        kn_ref_no = get_kn_ref_no(file_data)
                        kn_job_ref = "{}-{}-{}-{}-{}".format(file_data['billing_year'],
                                                             file_data['code'],
                                                             file_data['billing_type'],
                                                             file_data['billing_month'],
                                                             kn_ref_no)
                        if not row['DC NUMBER']:
                            row['DC NUMBER'] = row['SOURCE NO/MOVE ORDER NUMBER']
                        if not row['DC DATE']:
                            row['DC DATE'] = row['TRANSACTION DATE']
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
                                                       'RAWDATA9': row['FROM SUBINVENTORY LOCATOR CODE'],
                                                       'RAWDATA10': row['TO SUBINVENTORY LOCATOR CODE']})
                # Delete file on FTP server
                logging.info('Delete file on FTP server')
                ftp.delete(file_name)
            ftp.quit()
        except ftplib.all_errors as e:
            logging.error('Error:', e)
    logging.info("Done")


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

                # check Month is valid
                if groupNum == 3 or groupNum == 10:
                    month_string = match.group(groupNum)
                    if 0 > int(month_string) or int(month_string) > 12:
                        return False

                # check Year is valid
                if groupNum == 4 or groupNum == 11:
                    year_string = match.group(groupNum)
                    if 0 > int(year_string) or int(year_string) > 9999:
                        return False

                # check Extension is correct or not
                if groupNum == 12:
                    extension_string = match.group(groupNum)
                    if extension_string != '.XLSX' and extension_string != '.xlsx':
                        return False

        return True


def extractInfoFromFileName(file_name):
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


def get_kn_ref_no(file_data):
    sql_query = "SELECT MAX(KNREFNO) FROM dthrawdata WHERE BILLING_YEAR = '" + file_data['billing_year'] \
                + "' AND BILLING_MONTH = '" + file_data['billing_month'] + "' AND CHILD_CLIENT_CODE = '" + file_data[
                    'code'] \
                + "' AND TYPE='" + file_data['billing_type'] + "';"
    val = sql_conn.query(sql_query)
    try:
        result = str(val.fetchone()[0] + 1)
    except:
        result = 0
    return result


def check_record_existed(file_data):
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
        return False


def insert_fileimport(file_name, file_data):
    sql_conn.insert('fileimport', {'file_name_orig': file_name,
                                   'file_name_db': file_name,
                                   'file_location': os.path.join(ftp_source, file_name),
                                   'file_for_client': file_data['code'],
                                   'file_for_country': 'IN',
                                   'cby': '1',
                                   'cbyname': file_data['user_name'],
                                   'cbyip': '0.0.0.0',
                                   'file_ac_path': os.path.join(ftp_source, file_name),
                                   'ctime': datetime.now()})


class ScanThread(Thread):
    def __init__(self, event, timer_interval):
        Thread.__init__(self)
        self.stopped = event
        self.timer_interval = timer_interval

    def run(self):
        while not self.stopped.wait(self.timer_interval):
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            logging.info("------------")
            logging.info("Scan timer wakeup at: {}".format(current_time))
            scan()


if __name__ == '__main__':
    # read info from config file
    parser = ConfigParser()
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
    local_path = parser.get('global', 'local_path')
    scan_interval = parser.get('global', 'scan_interval')
    report_interval = parser.get('global', 'report_interval')

    # init logging
    log_file = "app_log.txt"
    if os.path.exists(log_file):
        os.remove(log_file)
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    log_format = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s", "%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(log_format)
    if log.hasHandlers():
        log.handlers.clear()
    log.addHandler(ch)

    fh = handlers.RotatingFileHandler(log_file, maxBytes=(1048576 * 5), backupCount=7, mode='w')
    fh.setFormatter(log_format)
    log.addHandler(fh)

    # validate input
    if not os.path.exists(local_path):
        logging.error('Local directory does not exist. Please check your configuration file!')
        exit(1)

    # init database
    sql_conn = simple_mysql.SimpleMysql(db=db_name, user=db_user, passwd=db_password,
                                        host=db_server, port=db_port, autocommit=True)

    report_sql_conn = simple_mysql.SimpleMysql(db=db_name, user=db_user, passwd=db_password,
                                               host=db_server, port=db_port, autocommit=True)

    # init send email function
    send_email_obj = send_email.SendEmail(smtp_server=smtp_server, smtp_port=smtp_port,
                                          sender_email=sender_email, password=password)

    # init scan file thread
    scan_stop_flag = Event()
    scan_thread_obj = ScanThread(scan_stop_flag, int(scan_interval))
    scan_thread_obj.start()
    logging.info("Started scan file thread (interval = {} seconds)".format(scan_interval))

    # init create report thread
    report_stop_flag = Event()
    report_thread_obj = report.ReportThread(report_stop_flag, report_sql_conn, send_email_obj, int(report_interval))
    report_thread_obj.start()
    logging.info("Started report thread (interval = {} seconds)".format(report_interval))
