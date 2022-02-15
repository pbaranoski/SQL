# SQL test

import os
import configparser

import pyodbc
import mysql.connector
from mysql.connector import errorcode
from mysql.connector import cursor
from mysql.connector.cursor_cext import CMySQLCursor

from logging import NullHandler

class NullCursorException(Exception):
    "Cursor object is null"
class ConfigFileNotfnd(Exception):
    "Configuration file not found: "

###############################################
### Left-justified
#print("{:<20}".format(123456789))
### Right-justified
#print("{:>20}".format(123456789))

#num = 13
#binary = "{0:x}".format(num)
#print("Binary representation:", binary)



def initGlobalVariables():

    ###############################
    # Global variables
    ###############################
    global curDir
    global configPath
    global configPathFilename

    ###############################
    # Get DB config filename
    ###############################
    curDir = os.getcwd()
    configPath = os.path.join(curDir,"config")
    configPathFilename = os.path.join(configPath,"DBConfig.txt")
    if os.path.exists(configPathFilename):
        #print("File exists: "+configPathFilename)
        pass
    else:
        raise ConfigFileNotfnd(configPathFilename)


def getConnection():

    cnx = None

    ###############################
    # Extract configuration values
    ###############################
    config = configparser.ConfigParser()
    config.read(configPathFilename)
    print("Taylor: "+configPathFilename)

    DBUser = config.get('DBSect','DBUser')
    DBPswd = config.get('DBSect','DBPswd')
    DBHost = config.get('DBSect','host')
    DBDatabase = config.get('DBSect','database')

    DBUser="root"
    DBPswd="trba0A!8" 
    DBHost="127.0.0.1"
    DBDatabase="prod"

    try: 
        cnx = mysql.connector.connect(user=DBUser, password=DBPswd,
                                    host=DBHost,
                                    database=DBDatabase)
                                    #buffered=True)
        '''                            
        cnx = mysql.connector.connect(user='root', password='trba0A!8',
                                    host='127.0.0.1',
                                    database='prod')
                                    #database='prod', buffered=True)
        '''                            

        print("Connected to Database!")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access is denied!")
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print ("Bad Database error")
            print("Database does not exist")
        else:
            print("Something went wrong!")
            print(err)

    return cnx


def oneRowAtATime(parm1):

    try:
        stmt = """SELECT familyMemberName, date_format(DOB,'%Y-%m-%d') as DOB, truncate((datediff(curdate(), DOB) / 365),0) as age
                from familymember where familymemberID < %s"""

        cnx = getConnection()
        # Dictionary=True allows you to access results-set by column names
        ##cursor = cnx.cursor(buffered=True)
        cursor = cnx.cursor(buffered=True, dictionary=True)

        # odd syntax for including parms to SQL
        cursor.execute(stmt, (parm1,))
        
        for row in cursor:
            print (row)
            print(row["familyMemberName"])
            print(row["DOB"])
            print(row["age"])
        #row = cursor.fetchone()
        #while row is not None:
        #    print(row)
        #    for fld in row:
        #        print(fld)
        #    row = cursor.fetchone()
 
    except mysql.connector.errors.InternalError as ie:
        print("Unread result found")
        # do not re-raise error

    except mysql.connector.Error as e:
        print("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()



def getManyRows(parm1, rows2Read):
    try:
        stmt = """SELECT familyMemberName, date_format(DOB,'%Y-%m-%d'), truncate((datediff(curdate(), DOB) / 365),0) as age
                from familymember where familymemberID < %s"""

        cnx = getConnection()
        cursor = cnx.cursor(buffered=True)
        #cursor = cnx.cursor()

        if cursor is None:
            raise NullCursorException()

        cursor.execute(stmt, (parm1,))

        records = cursor.fetchmany(rows2Read)
        while records is not None:
            for record in records:  
                print(record)

            records = cursor.fetchmany(rows2Read)    

        '''
        for record in records:
            print("\nfull record: "+str(record))
            print("\nprint for obj in list:")
            for fld in record:
                print(fld)

            print("\nfor in range:")
            for i in range(len(record)):
                print(record[i])
        '''        

    except mysql.connector.errors.InternalError as ie:
        print("Unread result found")
        # do not re-raise error

    except mysql.connector.Error as e:
        print("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()


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

            # Method 1 to process flds in results-set
            print("\nprint fields in record:")
            for fld in record:
                print(fld)

            # Method 2 to process flds in results-set
            #print("\nfor in range:")
            #for i in range(len(record)):
            #    print(record[i])

    except mysql.connector.Error as e:
        print("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()


def getRowsFromProc():

    try:

        cnx = getConnection()
        cursor = cnx.cursor(buffered=True)

        if cursor is None:
            raise NullCursorException()

        cursor.callproc("testProc")

        print("results from Proc in MySQL")
        for result in cursor.stored_results():
            for row in result.fetchall():
                print(row)
                print("\n")
            #print(result.fetchall())
   
    except mysql.connector.errors.InternalError as ie:
        print("Unread result found")
        # do not re-raise error

    except mysql.connector.Error as e:
        print("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()


def preparedSELECT(parm1, parm2):
    try:
        stmt = """SELECT familyMemberName, date_format(DOB,'%Y-%m-%d') as DOB, truncate((datediff(curdate(), DOB) / 365),0) as age
                from familymember where familymemberID between %s and %s """
        #stmt = " SELECT * from familymember where familymemberID between %s and %s "

        tuple1 = (parm1, parm2)

        cnx = getConnection()
        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        cursor.execute(stmt, tuple1)
        rows = cursor.fetchall()

        for row in rows:
            # zip in essence marries two sets of data
            # so zip function with column names and row values
            # assigns a column name to each column value in the row
            result = dict(zip(cursor.column_names, row))
            # this is the workaround for not being able to use "dictionary=True" with prepared=True
            print(result["familyMemberName"])
            print(result["DOB"])
            print(result["age"])
            print("integer ref: "+row[0])
        
        # run the SQL a 2nd time
        print("\n#########################\n")
 
        tuple1 = (40, 48)
        cursor.execute(stmt, tuple1)
        rows = cursor.fetchall()

        for row in rows:
            result = dict(zip(cursor.column_names, row))
            # this is the workaround for not being able to use "dictionary=True" with prepared=True
            print(result["familyMemberName"])
            print(result["DOB"])
            print(result["age"])

    except mysql.connector.Error as e:
        print("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()


def insertRow(sqlStmt, tupParms):
    #########################################################################################
    # MySQL does not have a way to execute an INSERT statement directly from the connection
    # object
    #########################################################################################
    try:
        stmt = """INSERT into familymember
                (FamilyMembername, FatherID, MotherID, GenderIND, DOB, DOBEndRange, DOBFormat,DOBQualifier,
                DOD, DODEndRange, DODFormat, DODQualifier,
                Birthplace, DeathPlace, InternmentID, FactsID, HospitalID,
                FamilyMemberAKAName, MultipleMarriagesIND, DefaultSpouseInd)
                Values(%s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                       %s, %s, %s )
        """

        cnx = getConnection()
        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        tuple1 = ("Minnie Mouse", 0, 0, "Y", '1959-09-04', None, 'YYYY-MM-DD', "",
                     None, None, "", "", "Fantasy Land", "", 0, 0, 0, 
                     "", "N", "" )

        cursor.execute(stmt, tuple1)
        cursor.execute(sqlStmt, tupParms)
        cnx.commit()
        print("row inserted!")

    except mysql.connector.Error as e:
        print("Error with Insert: ", e) 
        cnx.rollback()
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()

def UpdateRow():
    #########################################################################################
    # MySQL does not have a way to execute an UPDATE statement directly from the connection
    # object
    #########################################################################################
    try:
        stmt = """Update familymember
                    set FamilyMemberName = 'Mickey Mouse III'
                    where familyMemberID = %s;
        """

        cnx = getConnection()
        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        iFamilyMemberID = str(2760)

        # parameters must be a tuple/list
        cursor.execute(stmt, (iFamilyMemberID,))
        cnx.commit()
        print("row updated!")

    except mysql.connector.Error as e:
        print("Error with Update: ", e) 
        cnx.rollback()
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()

def DeleteRow():
    #########################################################################################
    # MySQL does not have a way to execute an INSERT statement directly from the connection
    # object
    #########################################################################################
    try:
        stmt = """Delete From familymember
                    where familyMemberID = %s;
        """

        cnx = getConnection()
        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        iFamilyMemberID = str(2762)

        # parameters must be a tuple/list
        cursor.execute(stmt, (iFamilyMemberID,))
        cnx.commit()
        print("row removed!")

    except mysql.connector.Error as e:
        print("Error with Delete: ", e) 
        cnx.rollback()
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()

#################################################
def main():

    #i for i in pyodbc.drivers() if i.startswith('Microsoft Access Driver')
    #for driver in pyodbc.drivers():
    #    print(driver)

    #print("data sources")
    #for ds in pyodbc.dataSources():
    #    print(ds)

    initGlobalVariables()

    #main(5)
    getAllRows(10)
    #getManyRows(5,2)
    #oneRowAtATime(8)
    #preparedSELECT(1,7)
    #getRowsFromProc()

    #insertRow()
    #UpdateRow()
    #DeleteRow()


if __name__ == "__main__":
    main()


