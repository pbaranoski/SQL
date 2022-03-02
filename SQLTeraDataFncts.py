
# Generic file of Teradata DB functions. Import module into Python program.
# author: Paul Baranoski
# 2022-02-12
#

import teradatasql
#pip install sqlalchemy-teradata (must be present on system to use sqlalchemy with teradata)
# NOTE: Alchemy appears to use ODBC connections to database. Need 64-bit ODBC teradata driver to use
#       with pandas. Can only find 32-bit drivers, and that is all that is installed on my machine.
# pyodbc python package - generic database package. Need ODBC 64-bit driver
#import teradataml (FYI)

#import datetime
import logging
import os
import csv
import re

from sqlalchemy import create_engine
#from dbmodule import connect

from logging import NullHandler

class NullConnectException(Exception):
    "Connection object is null"

class NullCursorException(Exception):
    "Cursor object is null"

class ConfigFileNotfnd(Exception):
    "Configuration file not found: "

DUPLICATE_ROW_ERROR_CD = "2801"


###############################
# Create log path+filename
###############################
log_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
logfile = os.path.join(log_dir,"SQLPythonTeraData.log")

###############################
# Config logfile
###############################
# NOTE: basicConfig function can only be called once from processing program, and needs to be
#       called before any logger messages written. Subsequent calls to basicConfig function within
#       program run are ignored.
#       https://docs.python.org/3/howto/logging.html

logger = logging.getLogger("SQLTeraDataFncts") 
logger.setLevel(logging.INFO)
fh = logging.FileHandler(logfile, "w", encoding='utf-8')
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

logger = logging.getLogger() 


##########################################
# TeraData Connection string values
##########################################
#td_username = 'IDRC_BEPSD_ETL_DEV'
#td_password = 't#9LBL717Dd7#Xd'
#td_hostx = "dd-proxy.biaaws.local"
td_username = 'BZH3'
td_password = 'trbf0f0F'
td_database = "DBC"
#td_hostx = "pz-nlb-common-in-thk-55ee045e137bee2c.elb.us-east-1.amazonaws.com"
td_hostx = "dz-nlb-common-in-thk-14c6b2e1d1e98dcc.elb.us-east-1.amazonaws.com"


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
        # Connect to TeraData
        ###################################################     
        #cnx = teradatasql.connect (host=td_hostx, user=td_username, password=td_password, database = "DBC", dbs_port= "1025",logmech="LDAP" )
        cnx = teradatasql.TeradataConnection(host=td_hostx, user=td_username, password=td_password, database = td_database, dbs_port= "1025",logmech="LDAP" )
        
        logger.info("Connected to Database!")
        logger.info(getDriverVersion(cnx))

    except teradatasql.Error as err:    
        logger.error("Could NOT connect to Database!")
        logger.error(err)
        raise

    return cnx


def getConnectionEngine():
    ##################################
    # need 64-bit ODBC driver for this
    ##################################

    logger.debug("start function getConnectionEngine()")

    eng = None

    try:

        ###################################################
        # Connect to Engine
        ###################################################     
        eng = create_engine(url="teradata://{0}:{1}@{2}:22/?authentication=LDAP?driver=Teradata)".format(td_username, td_password, td_hostx))

        #teradatasqlalchemy.teradatasql.connect

        if eng is None:
            print("engine NOT created!")
        else:
            print("Engine created!")    

        logger.info("Connected to Database!")
        logger.info(getDriverVersion(eng))

    except teradatasql.Error as err:    
        logger.error("Could NOT connect to Database!")
        logger.error(err)
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

    except teradatasql.Error as e:
        logger.error("Error with Select: "+sqlStmt) 
        logger.error(e)
        raise
    
    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()
            cnx.close()    


def getManyRowsCursor(sqlStmt, tupParms):
    #######################################################
    # function parms: 
    #   1) SQL string w/parm markers/or no parms
    #   2) tuple list of parms for SQL string. Can be null.  
    #######################################################  
    logger.info("start function getManyRowsCursor()")

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

        # create list of column names
        loadCursorColumnList(cursor.description)
        logger.debug("cursor columns: "+ str(cursor.description))

        return cursor    

    except teradatasql.Error as e:
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

    except teradatasql.Error as e:
        logger.error("Error with Select: ") 
        logger.error(e)
        if cursor is not None:
            cursor.close()
        raise


def UpdateRow(cnx, sqlStmt, tupParms):
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
        # "prepared=True" when defining cursor --> Exception
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
        logger.info("row updated!")


    except teradatasql.Error as e:
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


    except teradatasql.Error as e:
        logger.error("Error with SQL Delete: "+sqlStmt) 
        logger.error(e)
        raise

    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()


def insertRow(cnx, sqlStmt, tupParms):
    ##########################################################
    #
    ##########################################################
    logger.debug("start function insertRow()")

    try:
        # Make sure connection is valid
        if cnx is None:
            raise NullCursorException()  

        # "prepared=True" when defining cursor --> Exception
        cursor = cnx.cursor()
        if cursor is None:
            raise NullCursorException()

        cursor.execute(sqlStmt, tupParms)

        # Should remove this from function. Commits should be done from main program
        cnx.commit()
        logger.info("row inserted!")


    except teradatasql.Error as e:
        sErrorCode = getErrorCode(e)

        #if "[Error 2801]" in str (e):
        if sErrorCode == DUPLICATE_ROW_ERROR_CD:
            logger.info("Duplicate row ignored")
        else:        
            logger.error("Error Code: "+ sErrorCode)
            #logger.info(e.__module__)

            logger.error("Error with SQL INSERT: "+sqlStmt) 
            logger.error(e)
            raise

    finally: 
        if cnx is not None:
            if cursor is not None:
                cursor.close()


def getErrorCode(ex):

    sErrorCode = 0

    sErrorMsg = str(ex)
    idxStrt = sErrorMsg.find("Error ") 
    if idxStrt > 0:
        idxStrt += len("Error ")
        idxEnd = sErrorMsg.find("]",idxStrt)
        sErrorCode = sErrorMsg[idxStrt : idxEnd]

    return sErrorCode    

def getDriverVersion(cnx):

    with cnx.cursor () as cur:
        cur.execute ('{fn teradata_nativesql}Driver version {fn teradata_driver_version}  Database version {fn teradata_database_version}')
        return (cur.fetchone() [0])


def loadCursorColumnList(cursorDescription):

    global cursorColumnNames 
    cursorColumnNames = []

    for column in cursorDescription:
        cursorColumnNames.append(column[0])

    return True 


def createCSVFile(sFilename, header, rows, cDelim):
    ############################
    # Rows = results-set
    ############################
    logger.debug("start function createCSVFile()")

    with open(sFilename, 'w', newline='', encoding="utf-8") as csvfile:
        filewriter = csv.writer(csvfile, delimiter=cDelim,
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if header != None:                        
            filewriter.writerow(header)
        filewriter.writerows(rows)


# Cannot get this function to work.  Received the below error which doesn't make sense.
# [Teradata Database] [Error 3706] Syntax error: expected something between the beginning of the request and the word 'teradata_write_csv'
def getExportCSVFile(cnx, sFilename, sSQLSelect):

    logger.info("start function getExportCSVFile()")

    ####################################################################################
    # "{fn function}" is called an escape function and prepends the Select statement to use
    ####################################################################################
    sFastExportSQL = "{fn teradata_write_csv(" + sFilename + ")}" + sSQLSelect

    try:

        with cnx.cursor () as cur:
            #cur.execute (sFastExportSQL)
            cur.execute ("{fn teradata_write_csv(" + sFilename + ")} SELECT * FROM CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS")

            logger.info("Teradata Export completed!")


    except Exception as e:
        logger.error("Error with Export csv file: "+sFastExportSQL) 
        logger.error(e)
        raise


def bulkInsertTDReadCSV(cnx, csvFilename, sSQLInsert):

    logger.info("start function bulkInsertTDReadCSV()")

    sFastImportSQL = "{fn teradata_read_csv(" + csvFilename + ")} " + sSQLInsert

    try:

        with cnx.cursor () as cur:
            cur.execute (sFastImportSQL)

        logger.info("rows inserted!")   

    except Exception as e:
        logger.error("Error in function bulkInsertTDReadCSV: "+sFastImportSQL) 
        logger.error(e)
        raise


def bulkInsertTDFastLoad(cnx, sSQLInsert, lstParms):
    ##################################################
    # lstParms = Python List of sets of parameters
    # NOTE: This is not a csv file.
    # NOTE: Fastload only works on empty table, so 
    # cannot be used for "chunking" a set of new 
    # rows.
    ##################################################
    logger.info("start function bulkFastLoadTD()")

    sFastLoadSQL = "{fn teradata_try_fastload}" + sSQLInsert

    try:

        with cnx.cursor () as cur:
            cur.execute (sFastLoadSQL, lstParms)

        logger.info("rows inserted!")   

    except Exception as e:
        logger.error("Error in function bulkInsertTDFastLoad: "+sFastLoadSQL) 
        logger.error(e)
        raise


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

           
