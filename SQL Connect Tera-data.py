# SQL test

import pyodbc
import teradatasql
import os
import sys

from logging import NullHandler

class NullCursorException(Exception):
    "Cursor object is null"
class ConfigFileNotfnd(Exception):
    "Configuration file not found: "


def getConnection():

    cnx = None

    ###############################
    # Extract configuration values
    ###############################
    try: 

        username = 'BZH3'
        password = 'trbf0f0F'
        database = "CMS_VIEW_BENE_PRD"
        server = 'cmsprodacop1.cmssvc.local' 
        driver = 'Teradata Database ODBC Driver 16.20'
        #cnx = pyodbc.connect("DRIVER={Teradata};DBCNAME=localhost;UID="+username+";PWD="+ password+"QUIETMODE=YES")
        #cnx = pyodbc.connect(f'DRIVER={host};DBCNAME={server};UID={username};PWD={password};logmech="LDAP";QUIETMODE=YES')

        host = "pz-nlb-common-in-thk-55ee045e137bee2c.elb.us-east-1.amazonaws.com"

        cnx = teradatasql.connect (host=host, user=username, password=password, database = "DBC", dbs_port= "1025",logmech="LDAP" )
        
        #query = "SELECT DISTINCT FieldName FROM Tables3VX WHERE DatabaseName = 'CMS_VDM_VIEW_MDCR_PRD' AND TableName = 'V2_MDCR_CLM'" 
        #df = pandas.read_sql(query, con) 
        #print(df.info())
     

        print("Connected to Database!")

    except pyodbc.Error as oerr:
        print("Something went wrong!")
        print(oerr)

    except teradatasql.Error as err:    
        print("Something went wrong!")
        print(err)

    return cnx

def getAllRows(parm1):

    try:
        #stmt = "SELECT familyMemberName from familymember"
        #stmt = "SELECT familyMemberName, date_format(DOB,'%b-%d-%Y') from familymember"
        stmt = """SELECT familyMemberName, date_format(DOB,'%Y-%m-%d'), truncate((datediff(curdate(), DOB) / 365),0) as age
                from familymember where familymemberID < %s"""

        cnx = getConnection()
        cursor = cnx.cursor()
        if cursor is None:
            raise NullCursorException()

        cursor.execute(stmt, (parm1,))
        #result = cursor.fetchone()
        records = cursor.fetchall()

        print("records: "+str(len(records)))
        for record in records:
            print("\nfull record: "+str(record))
            print("\nprint for obj in list:")
            for fld in record:
                print(fld)

            print("\nfor in range:")
            for i in range(len(record)):
                print(record[i])

    except pyodbc.Error as e:
        print("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()

#################################################
def main():
 
    #print("Teradata drivers:")
    driver_names = [x for x in pyodbc.drivers() if x.startswith('Teradata')]
    for driver in driver_names:
        print(driver)

    #for driver in pyodbc.drivers():
    #    print(driver)

    #print("data sources")
    #for ds in pyodbc.dataSources():
    #    print(ds)

    #sys.exit(0)

    #initGlobalVariables()
    cnx = getConnection()
    if cnx is None:
        print("could NOT connect to Tera-Data")
    else:
        print("connected to Tera-Data")    
    #getAllRows(10)
    #getManyRows(5,2)
    #oneRowAtATime(8)


if __name__ == "__main__":
    main()


