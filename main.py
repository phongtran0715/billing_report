import ftplib
from configparser import SafeConfigParser
import smtplib
import ssl
import pandas as pd
import sql
from os import path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


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
    # TODO : implement validate file name function
    # Excel template : DD-MM-YYYY-CODE-RAW-USER-BILLING_TYPE-BILLING_MM-BILLING_YYYY.xlsx
    return True


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
                if not validate_excel_name(file_name):
                    # send email to user
                    bodyMsg = "File name is invalid : {}. Please update and re-upload again!".format(file_name)
                    send_email("phongtran0715@gmail.com", "[Report] File name is invalid",
                               bodyMsg, "/home/jack/Downloads/Document/BCT_LTL_TEMPLATE.xls")
                    print("File name : {} is not valid".format(file_name))
                    continue

                # TODO : read excel data
                local_file = "/tmp/" + file_name
                ftp.retrbinary("RETR " + file_name, open(local_file, 'wb').write)
                # excel_df = pd.read_excel(local_file)
                # print(excel_df)

                # TODO : insert data to database
                sql_conn = sql.Sql(database=db_name, user=db_user, password=db_password, host=db_server, port=db_port)
                # TODO : move processed file to destination folder
            ftp.quit()
        except ftplib.all_errors as e:
            print('Error:', e)
    print("Done")
