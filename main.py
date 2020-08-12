import ftplib
from configparser import SafeConfigParser
import smtplib
import ssl
import pandas as pd
import sql


def send_email(receiver_email, message):
    # Create a secure SSL context
    context = ssl.create_default_context()
    # Try to log in to server and send email
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()  # Can be omitted
        server.starttls(context=context)
        server.ehlo()  # Can be omitted
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)


def validate_excel_name(file_name):
    # TODO : implement validate file name function
    # Excel template : DD-MM-YYYY -CODE-RAW-USER-BILLING_TYPE-BILLING_MM-BILLING_YYYY.xlsx
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
    port = parser.get('email-info', 'port')
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
                    print("File name : {} is not valid".format(file_name))
                    continue

                # TODO : read excel data
                local_file = "/tmp/" + file_name
                ftp.retrbinary("RETR " + file_name, open(local_file, 'wb').write)
                excel_df = pd.read_excel(local_file)
                print(excel_df)

                # TODO : insert data to database
                sql_conn = sql.Sql(database=db_name, user=db_user, password=db_password, host=db_server, port=db_port)
                # TODO : move processed file to destination folder
            ftp.quit()
        except ftplib.all_errors as e:
            print('FTP error:', e)
    print("Done")
