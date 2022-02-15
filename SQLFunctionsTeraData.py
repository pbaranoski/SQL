# SQL test

import teradatasql
import datetime
import logging
import sys
import os

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import cursor
from mysql.connector.cursor_cext import CMySQLCursor

from logging import NullHandler

class NullCursorException(Exception):
    "Cursor object is null"
class ConfigFileNotfnd(Exception):
    "Configuration file not found: "

# Create log path+filename
log_dir = os.path.join(os.getcwd(), "temp")
logfile = os.path.join(log_dir,"SQLPythonTeraData.log")

# Config logfile
logging.basicConfig(
    #format="%(asctime)s %(levelname)-8s %(threadName)s %(funcName)s %(message)s", #--> %(name)s give logger name
    format="%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s",
    encoding='utf-8',
    datefmt="%Y-%m-%d %H:%M:%S", 
    #filename=logfile, 
    handlers=[
    logging.FileHandler(logfile),
    logging.StreamHandler(sys.stdout)],
    level=logging.INFO)

logger = logging.getLogger() 

###############################
# Functions
###############################

def getConnection():

    cnx = None

    ###############################
    # Extract configuration values
    ###############################

    try: 

        DBUser="root"
        DBPswd="trba0A!8" 
        DBHost="127.0.0.1"
        DBDatabase="prod"

        cnx = mysql.connector.connect(user=DBUser, password=DBPswd,
                                    host=DBHost,
                                    database=DBDatabase)
                                    #buffered=True)

        username = 'BZH3'
        password = 'trbf0f0F'
        database = "CMS_VIEW_BENE_PRD"
        hostx = "pz-nlb-common-in-thk-55ee045e137bee2c.elb.us-east-1.amazonaws.com"

        #cnx = teradatasql.connect (host=hostx, user=username, password=password, database = "DBC", dbs_port= "1025",logmech="LDAP" )
        
        logger.info("Connected to Database!")


    except teradatasql.Error as err:    
        logger.error("Could NOT connect to TeraData DB!!")
        logger.error(err)
        raise

    return cnx


def getAllRows(sqlStmt):

    try:

        cnx = getConnection()
        cursor = cnx.cursor()
        if cursor is None:
            raise NullCursorException()

        #cursor.execute(stmt, (parm1,))
        cursor.execute(sqlStmt)
        records = cursor.fetchall()

        return records


    except teradatasql.Error as e:
        logger.error("Error with Select: ", e) 
        raise

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()


def getManyRows(sqlStmt, rows2Read):
    try:

        cnx = getConnection()
        cursor = cnx.cursor(buffered=True)

        if cursor is None:
            raise NullCursorException()

        cursor.execute(sqlStmt)

        records = cursor.fetchmany(rows2Read)
        while records is not None:
            for record in records:  
                print(record)

            records = cursor.fetchmany(rows2Read)    

    finally: 
        if cnx.is_connected():
            cursor.close()
            cnx.close()

def UpdateRow(sqlStmt):
    #########################################################################################
    # MySQL does not have a way to execute an UPDATE statement directly from the connection
    # object.
    #########################################################################################
    try:

        cnx = getConnection()
        # Dictionary=True allows you to access results-set by column names
        # using both of the following does not work --> prepared=True,dictionary=True
        # This is known issue
        cursor = cnx.cursor(prepared=True)
        if cursor is None:
            raise NullCursorException()

        iFamilyMemberID = str(2760)

        # parameters must be a tuple/list
        #cursor.execute(sqlStmt, (iFamilyMemberID,))
        cursor.execute(sqlStmt)

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


#################################################
def main():

    # Insert eye-catcher
    logger.info("#########################")

    SqlStmt = """
        SELECT DISTINCT FieldName 
          FROM Tables3VX 
         WHERE DatabaseName = 'CMS_VDM_VIEW_MDCR_PRD' 
           AND TableName = 'V2_MDCR_CLM'" 
        """

    SqlStmt = """SELECT familyMemberName, date_format(DOB,'%Y-%m-%d'), truncate((datediff(curdate(), DOB) / 365),0) as age
                from familymember where familymemberID < 10"""

    #initGlobalVariables()

    try:
        ############################################
        # Test getting connection
        ############################################
        cnx = getConnection()
        if cnx is None:
            logger.error("Could NOT connect to Tera-Data")
        else:
            logger.info("Connected to Tera-Data! Yea!")    

        ############################################
        # Get all rows in results-set
        ############################################
        rows = getAllRows(SqlStmt)

        #getManyRows(5,2)

        ##################################################
        # Print results-set by 1) full row 2) flds in row
        ##################################################
        logger.info("NOF rows in results-set: "+str(len(rows)))
        for row in rows:
            logger.info("full record: "+str(row))
            logger.info("print fields in record:")
            for fld in row:
                logger.info(fld)

        ##################################################
        # Load results-set directly to pandas
        ##################################################
        #df = pandas.read_sql(sqlStmt, cnx) 
        #print(df.info())

    except teradatasql.Error as e:
        logger.error("Database error:", e) 

    finally: 
        logger.info("Processing complete.")


if __name__ == "__main__":
    main()


