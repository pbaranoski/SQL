# Generic file of MySQL DB functions. Import module into Python program.
# author: Paul Baranoski
# 2022-03-11
#

import logging
import os
import csv

###################
# ODBC connection
###################
from sqlalchemy import create_engine

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import cursor
from mysql.connector.cursor_cext import CMySQLCursor

from logging import NullHandler

class NullConnectException(Exception):
    "Connection object is null"

class NullCursorException(Exception):
    "Cursor object is null"

class ConfigFileNotfnd(Exception):
    "Configuration file not found: "


# Create log path+filename
log_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
logfile = os.path.join(log_dir,"SQLPythonMySQL.log")

# Config logfile
logger = logging.getLogger("SQLMYSQLFncts") 
logger.setLevel(logging.INFO)
fh = logging.FileHandler(logfile, "w", encoding='utf-8')
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)


##########################################
# MySQL Connection string values
##########################################
mysql_user="root"
mysql_password="trba0A!8" 
mysql_host="127.0.0.1"
mysql_database="prod"
mysql_port="3306"


###############################
# Functions
###############################

def closeConnection(cnx):

    logger.debug("start function closeConnection()")

    if cnx is not None:
        cnx.close()

def getConnection():

    logger.debug("start function getConnection()")

    cnx = None

    ###############################
    # Extract configuration values
    ###############################
    try: 

        ###################################################
        # Connect to MySQL
        ###################################################     
        cnx = mysql.connector.connect(user=mysql_user, password=mysql_password, host=mysql_host, database=mysql_database)
           
        logger.info("Connected to Database!")

    except  mysql.connector.Error as e:
        logger.error("Could NOT connect to MySQL") 
        logger.error(e)
        raise

    return cnx


def getConnectionEngine():

    logger.debug("start function getConnectionEngine()")

    eng = None

    try:

        ###################################################
        # Connect to MySQL
        ###################################################     
        eng = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(mysql_user, mysql_password, mysql_host, mysql_port, mysql_database))


        if eng is None:
            print("engine NOT created!")
        else:
            print("Engine created!")    

        logger.info("Connected to Database!")

    except  mysql.connector.Error as e:
        logger.error("Could NOT connect to MySQL") 
        logger.error(e)
        raise

    return eng


def getAllRows(sqlStmt, tupParms):
    ########################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #   2) tuple list of parms for SQL string (can be null). 
    #########################################################
    logger.info("start function getAllRows()")

    try:

        cnx = getConnection()
        cursor = cnx.cursor()
        if cursor is None:
            raise NullCursorException()

        # parameters must be a tuple
        if tupParms is None:
            cursor.execute(sqlStmt)
        else:
            cursor.execute(sqlStmt, tupParms)

        logger.debug("SQL: "+sqlStmt)

        records = cursor.fetchall()

        # create list of column names
        loadCursorColumnList(cursor.description)
        logger.debug("cursor columns: "+ str(cursor.description))

        return records


    except  mysql.connector.Error as e:
        logger.error("Error with Select: "+sqlStmt) 
        logger.error(e)
        raise

    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()
            cnx.close()    


def getManyRowsCursor(sqlStmt, tupParms):
    ##############################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #   2) tuple list of parms for SQL string. 
    #      Can be null  
    ##############################################  
    logger.info("start function getManyRowsCursor()")

    try:

        cnx = getConnection()
        cursor = cnx.cursor(buffered=True)

        if cursor is None:
            raise NullCursorException()

        # parameters must be a tuple
        if tupParms is None:
            cursor.execute(sqlStmt)
        else:
            cursor.execute(sqlStmt, tupParms)

        # create list of column names
        loadCursorColumnList(cursor.description)
        logger.debug("cursor columns: "+ str(cursor.description))

        return cursor    

    except  mysql.connector.Error as e:
        logger.error("Error with Select: "+sqlStmt) 
        logger.error(e)
        if cursor is not None:
            cursor.close()
        raise


def getManyRowsNext(cursor, rows2Read):
    ##############################################
    # function parms: 
    #   1) active cursor
    #   2) Rows to read from cursor  
    ##############################################   
    logger.info("start function getManyRowsNext()")
  
    try:

        if cursor is None:
            raise NullCursorException()

        records = cursor.fetchmany(rows2Read)

        return records

    except  mysql.connector.Error as e:
        logger.error("Error with Select: ") 
        logger.error(e)
        if cursor is not None:
            cursor.close()
        raise


def UpdateRow(cnx, sqlStmt, tupParms):
    #########################################################################################
    # MySQL does not have a way to execute an UPDATE statement directly from the connection
    # object.
    #########################################################################################
    ##########################################################
    # function parms: 
    #   1) connection obj
    #   2) SQL string w/parm markers/or no parms
    #   3) tuple list of parms for SQL string. (Can be null)  
    ##########################################################
    logger.debug("start function UpdateRow()")

    try:
        # Make sure connection is valid
        if cnx is None:
            raise NullCursorException()            

        #################################################################################
        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        #################################################################################
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        # parameters must be a tuple
        if tupParms is None:
            cursor.execute(sqlStmt)
        else:
            cursor.execute(sqlStmt, tupParms)

        # May want to remove this from here, and control commits from Main program.
        cnx.commit()
        logger.info("row updated!")

    except mysql.connector.Error as e:
        logger.error("Error with SQL Update: "+sqlStmt) 
        logger.error(e)
        raise

    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()


def DeleteRows(sqlStmt, tupParms):
    ##########################################################
    #
    ##########################################################
    logger.debug("start function DeleteRows()")

    try:

        cnx = getConnection()

        # Make sure connection is valid
        if cnx is None:
            raise NullCursorException()            

        cursor = cnx.cursor()
        if cursor is None:
            raise NullCursorException()

        # parameters must be a tuple
        if tupParms is None:
            cursor.execute(sqlStmt)
        else:
            cursor.execute(sqlStmt, tupParms)

        # May want to remove this from here, and control commits from Main program.
        cnx.commit()
        logger.info("rows removed!")


    except mysql.connector.Error as e:
        logger.error("Error with SQL Delete: "+sqlStmt) 
        logger.error(e)
        raise

    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()


def insertRow(cnx, sqlStmt, tupParms):
    #########################################################################################
    # MySQL does not have a way to execute an INSERT statement directly from the connection
    # object
    #########################################################################################
    logger.debug("start function insertRow()")

    try:
        # Make sure connection is valid
        if cnx is None:
            raise NullCursorException()  

        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        cursor.execute(sqlStmt, tupParms)
        # Should remove this from function. Commits should be done from main program
        cnx.commit()
        print("row inserted!")

    except mysql.connector.Error as e:
        logger.error("Error with SQL Update: "+sqlStmt) 
        logger.error(e)
        raise

    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()


def loadCursorColumnList(cursorDescription):
    
    global cursorColumnNames 
    cursorColumnNames = []

    for column in cursorDescription:
        cursorColumnNames.append(column[0])

    return True 


def createCSVFile(sFilename, header, rows, cDelim):
    ####################################################################
    # Rows = results-set
    # NOTE: QUOTE_ALL --> so that zip_code '00000' can be treated as 
    #       string and not integer
    ####################################################################
    logger.debug("start function createCSVFile()")

    with open(sFilename, 'w', newline='', encoding="utf-8") as csvfile:
        filewriter = csv.writer(csvfile, delimiter=cDelim,
                                quotechar='"', quoting=csv.QUOTE_ALL)
        if header != None:                        
            filewriter.writerow(header)
        filewriter.writerows(rows)


def convertNoValue2NullValue(row):
    ############################################
    # convert empty string to "None"
    ############################################
    conv = lambda fld : fld or None
    row = [conv(fld) for fld in row]

    return row  


def bulkInsrtUpdtCSVReader(cnx, sFilename, sSQLInsert, bHeader):
    ##############################################
    # sFilename = csv file
    ##############################################
    logger.info("start function bulkInsrtUpdtCSVReader()")

    try:

        with open(sFilename, 'r', newline='') as csvfile:
            with cnx.cursor () as cur:

                ######################################################################################
                # NOTE: csv.reader converts empty value to an empty string instead of Python None/Null
                # NOTE: This causes problem when inserting/updating a timestamp column (i.e., UPDT_TS)
                #  where a string is not a valid timestamp value. 
                # NOTE: Need to have keyword None in csv file where values to insert are NULL.
                ######################################################################################

                csv_reader = csv.reader(csvfile,quoting=csv.QUOTE_MINIMAL)
                if bHeader:
                    next(csv_reader)

                cur.execute (sSQLInsert, [ convertNoValue2NullValue(row) for row in csv_reader] )
                #cur.execute(sSQLInsert, Parms)
                
                cnx.commit()

        logger.info("rows inserted/updated!")   

    except Exception as e:
        logger.error("Error in function bulkInsrtUpdtCSVReader: "+sSQLInsert) 
        logger.error("csv filename: "+sFilename)
        logger.error(e)

        raise
