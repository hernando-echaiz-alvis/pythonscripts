def readOracleDatabaseTable(userName, password, host, port, sid, tableName):
    """
    Reads the table from the Oracle database.
    """
    import cx_Oracle
    import os
    import sys
    #import configparser

    #config = configparser.ConfigParser()
    #config.read('config.ini')
    #userName = "SYSTEM" #config['Oracle']['username']
    #password = "Nirvana1" #config['Oracle']['password']
    #host = "192.168.100.207" #config['Oracle']['host']
    #port = "32518" #config['Oracle']['port']
    #sid = "XE" #config['Oracle']['sid']
    #tableName = "SYSTEM.MFPM_TABLONF_2203_2206_S4"

    dsn = cx_Oracle.makedsn(host, port, sid)
    connection = cx_Oracle.connect(userName, password, dsn)
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM ALL_TABLES WHERE CONCAT(CONCAT(OWNER,'.'),\
                   TABLE_NAME)='{tableName.upper()}'")
    result=cursor.fetchone()
    if result[0]==1:
        cursor.execute("SELECT * FROM " + tableName +' WHERE ROWNUM<=5')
        rows = cursor.fetchall()
        col_name = [row[0] for row in cursor.description]
        cursor.close()
        connection.close()
        return rows,col_name
    else:
        raise Exception(f"La tabla {tableName} no existe")
		
userName="USRPREPROD"
password="TI_Soport3202010#"
host="20.228.129.81"
port="1521"
sid="DWO"
tableName="C19663.VAR_TABLON2_2208"
rows,col_name=readOracleDatabaseTable(userName, password, host, port, sid, tableName)
print(col_name)
print(rows[0])
