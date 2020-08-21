# This function will create a excel report file
from configparser import SafeConfigParser

from pandas import *
from billing_report import simplemysql


def create_report(sql_conn):
    # set file path

    file_path = '/tmp/report.xlsx'

    # create sql connection
    # sql_conn = simplemysql.SimpleMysql(db=db_name, user=db_user, passwd=db_password,
    #                                    host=db_server, port=db_port, autocommit=True)

    # fill report content
    # open sample excel file
    sample_wb = ExcelFile('template.xlsx')
    data = sample_wb.parse(sample_wb.sheet_names[0])
    data_dictionary = data.to_dict()

    # list value declare
    dict_val = []

    # get raw data from database (select [raw data] from dthrawdata table)
    raw_data = sql_conn.getAll('dthrawdata',
                               ['KN_JOB_REF', 'RAWDATA3', 'RAWDATA4', 'RAWDATA5', 'RAWDATA6', 'RAWDATA7', 'RAWDATA8',
                                'RAWDATA9', 'RAWDATA10'])

    kn_job_ref = []
    raw_data3 = []
    raw_data4 = []
    raw_data5 = []
    raw_data6 = []
    raw_data7 = []
    raw_data8 = []
    raw_data9 = []
    raw_data10 = []

    for val in raw_data:
        kn_job_ref.append(val.get('KN_JOB_REF'))
        raw_data3.append(val.get('RAWDATA3'))
        raw_data4.append(val.get('RAWDATA4'))
        raw_data5.append(val.get('RAWDATA5'))
        raw_data6.append(val.get('RAWDATA6'))
        raw_data7.append(val.get('RAWDATA7'))
        raw_data8.append(val.get('RAWDATA8'))
        raw_data9.append(val.get('RAWDATA9'))
        raw_data10.append(val.get('RAWDATA10'))

    # check raw_data
    # print(kn_job_ref)
    # print(raw_data3)
    # print(raw_data4)
    # print(raw_data5)
    # print(raw_data6)
    # print(raw_data7)
    # print(raw_data8)
    # print(raw_data9)
    # print(raw_data10)

    # KN_JOB_REF: KN_JOB_REF → select from dthrawdata table
    if kn_job_ref is None:
        data_dictionary['KN_JOB_REF'] = {}
    else:
        data_dictionary['KN_JOB_REF'] = kn_job_ref

    # DC_NO: RAWDATA3 → select from dthrawdata table
    if raw_data3 is None:
        data_dictionary['DC_NO'] = {}
    else:
        data_dictionary['DC_NO'] = raw_data3

    # DC_DATE: RAWDATA4 → select from dthrawdata table
    if raw_data4 is None:
        data_dictionary['DC_DATE'] = {}
    else:
        data_dictionary['DC_DATE'] = raw_data4

    # ITEM_CODE: RAWDATA5 → select from dthrawdata table
    if raw_data5 is None:
        data_dictionary['ITEM_CODE'] = {}
    else:
        data_dictionary['ITEM_CODE'] = raw_data5

    # ITEM_DESCRIPTION: RAWDATA6 → select from dthrawdata table
    if raw_data6 is None:
        data_dictionary['ITEM_DESCRIPTION'] = {}
    else:
        data_dictionary['ITEM_DESCRIPTION'] = raw_data6

    # ITEM_QTY: RAWDATA7 →select from dthrawdata table
    if raw_data7 is None:
        data_dictionary['ITEM_QTY'] = {}
    else:
        data_dictionary['ITEM_QTY'] = raw_data7

    # INVOICE_VALUE: RAWDATA8 → select from dthrawdata table
    if raw_data8 is None:
        data_dictionary['INVOICE_VALUE'] = {}
    else:
        data_dictionary['INVOICE_VALUE'] = raw_data8

    # DISTRIBUTOR_LOCATOR_CODE: RAWDATE10 → select from dthrawdata table
    if raw_data10 is None:
        data_dictionary['DISTRIBUTOR_LOCATOR_CODE'] = {}
    else:
        data_dictionary['DISTRIBUTOR_LOCATOR_CODE'] = raw_data10

    # set empty fields

    # ORDER_RECEVIED_DATE: empty
    data_dictionary['ORDER_RECEVIED_DATE'] = {}
    # ORDER_RECEVIED_TIME: empty
    data_dictionary['ORDER_RECEVIED_TIME'] = {}
    # CUTOFF(16:00_HRS): empty
    data_dictionary['CUTOFF(16:00_HRS)'] = {}
    # ROAD_PERMIT_RECEIVED_DATE: empty
    data_dictionary['ROAD_PERMIT_RECEIVED_DATE'] = {}
    # ROAD_PERMIT_RECEIVED_TIME: empty
    data_dictionary['ROAD_PERMIT_RECEIVED_TIME'] = {}
    # TYPE_OF_DISPATCH: empty
    data_dictionary['TYPE_OF_DISPATCH'] = {}
    # MODE_OF_DISPATCH: empty
    data_dictionary['MODE_OF_DISPATCH'] = {}
    # VEHICLE_TYPE: empty
    data_dictionary['VEHICLE_TYPE'] = {}
    # VEHICLE_NO: empty
    data_dictionary['VEHICLE_NO'] = {}
    # TRANSPORTER_NAME: empty
    data_dictionary['TRANSPORTER_NAME'] = {}
    # LR_NO: empty
    data_dictionary['LR_NO'] = {}
    # LR_DATE: empty
    data_dictionary['LR_DATE'] = {}
    # NO_OF_BOX : empty
    data_dictionary['NO_OF_BOX'] = {}
    # ACTUAL_DATE_OF_DELIVERY : empty
    data_dictionary['ACTUAL_DATE_OF_DELIVERY'] = {}
    # POD_STATUS : empty
    data_dictionary['POD_STATUS'] = {}
    # REASON: empty
    data_dictionary['REASON'] = {}
    # WARAI/MATHADI/UNION/OCTROI_CHARGES: empty
    data_dictionary['WARAI/MATHADI/UNION/OCTROI_CHARGES'] = {}
    # RE-ATTEMPT: empty
    data_dictionary['RE-ATTEMPT'] = {}
    # REMARK: empty
    data_dictionary['REMARK'] = {}

    # ORIGIN_LOCATION: Select DESTINATION From locationmaster where ORACLE_LOCATOR_CODE='RAWDATE9'
    # ORIGIN_ZONE: Select DEST_ZONE From locationmaster where ORACLE_LOCATOR_CODE='RAWDATE9'

    origin_location = []
    origin_zone = []
    for raw_condition in raw_data8:
        get_value = sql_conn.getAll('locationmaster', ['DESTINATION', 'DEST_ZONE'],
                                  ("ORACLE_LOCATOR_CODE=%s", ['96502']))
        # print(get_value)
        # origin_location.append(get_value.get('DESTINATION'))
        if get_value is not None:
            for data in get_value:
                origin_location.append(data.get('DESTINATION'))
                origin_zone.append(data.get('DEST_ZONE'))

    if origin_location is None:
        data_dictionary['ORIGIN_LOCATION'] = {}
    else:
        data_dictionary['ORIGIN_LOCATION'] = origin_location

    if origin_zone is None:
        data_dictionary['ORIGIN_ZONE'] = {}
    else:
        data_dictionary['ORIGIN_ZONE'] = origin_zone
    # print(origin_location)
    # print(origin_zone)

    print(data_dictionary)

    # create dataframe from dictionary (that have different row number)
    df = pandas.DataFrame(dict([(k, pandas.Series(v)) for k, v in data_dictionary.items()]))
    df.to_excel(file_path, sheet_name='Sheet1', index=False)


# Finish create file report


# Create a main function
# if __name__ == '__main__':
    # read info from config file
    # parser = SafeConfigParser()
    # parser.read('app.conf')

    # load ftp info
    # ftp_server = parser.get('ftp-info', 'server')
    # ftp_user = parser.get('ftp-info', 'user')
    # ftp_password = parser.get('ftp-info', 'password')
    # ftp_source = parser.get('ftp-info', 'source')
    # ftp_dest = parser.get('ftp-info', 'dest')

    # load email info
    # smtp_server = parser.get('email-info', 'smtp_server')
    # smtp_port = parser.get('email-info', 'port')
    # sender_email = parser.get('email-info', 'sender_email')
    # password = parser.get('email-info', 'password')

    # load database info
    # db_server = parser.get('database-info', 'server')
    # db_user = parser.get('database-info', 'user')
    # db_password = parser.get('database-info', 'password')
    # db_port = parser.get('database-info', 'port')
    # db_name = parser.get('database-info', 'db_name')
    #
    # admin_email = parser.get('global', 'admin_email')
    #
    # create_report()
