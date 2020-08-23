from configparser import SafeConfigParser
import pandas as pd
import simplemysql


def get_location_table(sql_conn, field, where, param):
    sql_query = "SELECT " + field + " FROM locationmaster WHERE " + where + " = '" + param + "';"
    cursor = sql_conn.query(sql_query)
    val = cursor.fetchone()
    if val is None:
        val = ''
    else:
        val = val[0]
    return val


def update_record_status(sql_conn, row_id):
    sql_conn.update('dthrawdata', {'RSTATUS': 'COMPLETE'}, ['ID={}'.format(row_id)])


def get_user_email(sql_conn, user_id):
    email = sql_conn.getOne('users', "useremail", ('user_name=%s', [user_id]))
    return email


def create_report(sql_conn):
    df = pd.DataFrame()
    file_path = 'report.xlsx'
    # Select all user from dthrawdata
    sql_query = "SELECT DISTINCT(IBY) FROM dthrawdata;"
    cursor = sql_conn.query(sql_query)
    users = cursor.fetchall()
    for user in users:
        print("Creating report for user : " + user[0])
        # TODO: SELECT all record from dthrawdata for each user
        sql_query = "SELECT * FROM dthrawdata WHERE IBY = '" + user[0] + "';"
        cursor = sql_conn.query(sql_query)
        records = cursor.fetchall()
        for i in range(len(records)):
            row = records[i]
            print("Record {}/{}".format(i, len(records)))
            df2 = pd.DataFrame({"KN_JOB_REF": [row[11]],
                                "ORIGIN_LOCATION": [get_location_table(sql_conn, 'DESTINATION', 'ORACLE_LOCATOR_CODE',
                                                                       row[20])],
                                "ORIGIN_ZONE": [get_location_table(sql_conn, 'DEST_ZONE', 'ORACLE_LOCATOR_CODE',
                                                                   row[20])],
                                "DISTRIBUTOR_LOCATOR_CODE": [row[21]],
                                "DISTRIBUTOR_NAME": [get_location_table(sql_conn, 'NAME', 'ORACLE_LOCATOR_CODE',
                                                                        row[21])],
                                "DESTINATION_LOCATION": [get_location_table(sql_conn, 'DESTINATION',
                                                                            'ORACLE_LOCATOR_CODE', row[21])],
                                "DESTINATION_ZONE": [get_location_table(sql_conn, 'DEST_ZONE',
                                                                        'ORACLE_LOCATOR_CODE', row[21])],
                                "DESTINATION_PIN_CODE": [get_location_table(sql_conn, 'DESTINATION_PINCODE',
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
            update_record_status(sql_conn, row[0])

        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        # Add a header format.
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1})
        # Write the column headers with the defined format.
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num + 1, value, header_format)

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()

        # Send email to user
        email = get_user_email(sql_conn, user)
        if not email:
            pass
