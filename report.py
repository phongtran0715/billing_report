from configparser import SafeConfigParser
from datetime import datetime

import pandas as pd
from threading import Thread, Event
import logging


class ReportThread(Thread):
    sql_conn = None
    email_obj = None
    timer_interval = None

    def __init__(self, event, sql_conn, send_email_obj, timer_interval):
        Thread.__init__(self)
        self.sql_conn = sql_conn
        self.email_obj = send_email_obj
        self.timer_interval = timer_interval
        self.stopped = event

    def run(self):
        while not self.stopped.wait(self.timer_interval):
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")
            logging.info("++++++++++++")
            logging.info("Report timer wakeup at: {}".format(current_time))
            self.create_report()

    def get_location_table(self, field, where, param):
        sql_query = "SELECT " + field + " FROM locationmaster WHERE " + where + " = '" + param + "';"
        cursor = self.sql_conn.query(sql_query)
        val = cursor.fetchone()
        if val is None:
            val = ''
        else:
            val = val[0]
        return val

    def update_record_status(self, row_id):
        self.sql_conn.update('dthrawdata', {'RSTATUS': 'COMPLETE'}, ['ID={}'.format(row_id)])

    def get_user_email(self, user_name):
        sql = "SELECT useremail FROM users WHERE user_name = '" + user_name + "';"
        val = self.sql_conn.query(sql)
        result = val.fetchone()
        return result

    def create_report(self):
        df = pd.DataFrame()
        file_path = 'report.xlsx'
        # Select all user from dthrawdata
        sql_query = "SELECT DISTINCT(IBY) FROM dthrawdata WHERE RSTATUS	= 'PENDING';"
        cursor = self.sql_conn.query(sql_query)
        users = cursor.fetchall()
        if len(users) <= 0:
            logging.info("Not found any user with pending status")
            return
        for user in users:
            logging.info("Creating report for user : " + user[0])
            # SELECT all record from dthrawdata for each user
            sql_query = "SELECT * FROM dthrawdata WHERE IBY = '" + user[0] + "';"
            cursor = self.sql_conn.query(sql_query)
            records = cursor.fetchall()
            for i in range(len(records)):
                row = records[i]
                logging.info("Record {}/{}".format(i + 1, len(records)))
                df2 = pd.DataFrame({"KN_JOB_REF": [row[11]],
                                    "ORIGIN_LOCATION": [
                                        self.get_location_table('DESTINATION', 'ORACLE_LOCATOR_CODE', row[20])],
                                    "ORIGIN_ZONE": [self.get_location_table('DEST_ZONE',
                                                                            'ORACLE_LOCATOR_CODE', row[20])],
                                    "DISTRIBUTOR_LOCATOR_CODE": [row[21]],
                                    "DISTRIBUTOR_NAME": [self.get_location_table('NAME',
                                                                                 'ORACLE_LOCATOR_CODE', row[21])],
                                    "DESTINATION_LOCATION": [self.get_location_table('DESTINATION',
                                                                                     'ORACLE_LOCATOR_CODE', row[21])],
                                    "DESTINATION_ZONE": [self.get_location_table('DEST_ZONE',
                                                                                 'ORACLE_LOCATOR_CODE', row[21])],
                                    "DESTINATION_PIN_CODE": [
                                        self.get_location_table('DESTINATION_PINCODE',
                                                                'ORACLE_LOCATOR_CODE', row[21])],
                                    "ORDER_RECEVIED_DATE": '',
                                    "ORDER_RECEVIED_TIME": '',
                                    "CUTOFF(16:00_HRS)": '',
                                    "ROAD_PERMIT_RECEIVED_DATE": '',
                                    "ROAD_PERMIT_RECEIVED_TIME": '',
                                    "DC_NO": [row[14]],
                                    "DC_DATE": [row[15]],
                                    "ITEM_CODE": [row[16]],
                                    "ITEM_DESCRIPTION": [row[17]],
                                    "ITEM_QTY": [row[18]],
                                    "INVOICE_VALUE": [row[19]],
                                    "TYPE_OF_DISPATCH": '',
                                    "MODE_OF_DISPATCH": '',
                                    "VEHICLE_TYPE": '',
                                    "VEHICLE_NO": '',
                                    "TRANSPORTER_NAME": '',
                                    "LR_NO": '',
                                    "LR_DATE": '',
                                    "NO_OF_BOX": '',
                                    "ACTUAL_DATE_OF_DELIVERY": '',
                                    "POD_STATUS": '',
                                    "REASON": '',
                                    "WARAI/MATHADI/UNION/OCTROI_CHARGES": '',
                                    "RE-ATTEMPT": '',
                                    "REMARK": ''})
                df = df.append(df2)
                self.update_record_status(row[0])

            if df.empty:
                logging.info("Not found any pending record")
                continue
            # Create a Pandas Excel writer using XlsxWriter as the engine.
            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            # Get the xlsxwriter workbook and worksheet objects.
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            worksheet.set_column('A:AG', 30)
            # Add a header format.
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1})
            header_format.set_align('center')
            header_format.set_align('vcenter')

            cell_format = workbook.add_format()
            cell_format.set_align('center')
            cell_format.set_align('vcenter')

            # Format another row
            for row_num in range(len(df.index)):
                for col_num in range(len(df.columns)):
                    worksheet.write(row_num + 1, col_num, df.iloc[row_num][col_num], cell_format)

            # Write the column headers with the defined format.
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # Close the Pandas Excel writer and output the Excel file.
            writer.save()

            # Send email to user
            email = self.get_user_email(user[0])
            if not email:
                logging.warning("Can not find email of user : " + user[0])
            else:
                logging.info("Send email to : " + email[0])
                body_msg = "This is billing report. Please check the attachment!"
                self.email_obj.send_email(email[0], "[Report] Billing report",
                                          body_msg, file_path)
