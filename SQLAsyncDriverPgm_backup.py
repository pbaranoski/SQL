# Asynchronous Process Driver
# Author: Paul Baranoski
# 2022-02-14
#
from ast import Pass
import SQLTeraDataFncts as SQLFncts

from threading import Thread

import pandas as pd
import time
import os
import sys
import datetime
import logging
import configparser

# process and system utilities) is a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) 
#psutil (FYI)

class ConfigFileNotfnd(Exception):
    "Configuration file not found: "
    
MAX_NOF_ACTIVE_THREADS = 15
CHUNK_SIZE_NOF_ROWS = 10000

MAX_NOF_RETRIES = 3
NOF_STATES_2_PROCESS_AT_ONCE = 5
STATES_2_PROCESS = ""

sThreadRCMsgs = []

arrListOfStates = None

###############################
# Create log path+filename
###############################
app_dir = os.getcwd()
log_dir = os.path.join(app_dir, "logs")
data_dir = os.path.join(app_dir, "temp")

if not os.path.exists(log_dir):
    os.mkdir(log_dir)
if not os.path.exists(data_dir):
    os.mkdir(data_dir)

dttm = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
mainLogfile = os.path.join(log_dir,f"geoMainProcess_{dttm}.log")

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(threadName)-12s %(funcName)-22s %(message)s",
    encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
    handlers=[
    logging.FileHandler(mainLogfile),
    logging.StreamHandler(sys.stdout)],    
    level=logging.DEBUG)

rootLogger = logging.getLogger() 

###############################
# SQL
###############################
SqlStmtGeo2 = """
    SELECT ADR_LINE_1, ADR_LINE_2, ADR_LINE_3, ADR_LINE_4, ADR_LINE_5, ADR_LINE_6,
        ADR_FULL, 
        CITY_NAME, USPS_STATE_CD, POSTAL_CD, POSTAL_EXT,
        ADD_GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD,
        GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY,
        IDR_INSRT_TS, IDR_UPDT_TS
    FROM CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS
  ;
"""
SqlStmtGeo1 = """
   SELECT GEO_ADR_LINE_1_ADR, GEO_ADR_LINE_2_ADR, GEO_ADR_LINE_3_ADR, GEO_ADR_LINE_4_ADR, GEO_ADR_LINE_5_ADR, GEO_ADR_LINE_6_ADR,
        GEO_ADR_FULL_ADR, 
        GEO_ADR_CITY_NAME, GEO_USPS_STATE_CD, GEO_ZIP5_CD, GEO_ZIP4_CD,
        GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD,
        GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY,
        IDR_INSRT_TS, IDR_UPDT_TS
    FROM CMS_VIEW_GEO_CDEV.V1_GEO_ADR
    WHERE GEO_USPS_STATE_CD IN ( ? )
 ;
"""
#     AND GEO_ZIP5_CD = '21204'
##    AND GEO_ZIP5_CD = '21236'
#       AND IDR_UPDT_TS is NULL
#    AND GEO_ADR_FULL_ADR = '1441 N ROLLING ROAD' AND GEO_ZIP5_CD = '00000'
#    AND GEO_ADR_FULL_ADR = '8139 RITCHIE HWY' AND GEO_ADR_CITY_NAME = 'NA'

SqlInsert = """
        INSERT INTO CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS_BLK_INSRT (
        ADR_LINE_1, ADR_LINE_2, ADR_LINE_3, ADR_LINE_4, ADR_LINE_5, ADR_LINE_6,
        ADR_FULL, 
        CITY_NAME, USPS_STATE_CD, POSTAL_CD, POSTAL_EXT,
        ADD_GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD,
        GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY,
        IDR_INSRT_TS, IDR_UPDT_TS)
    Values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
    """

# Don't forget to update IDR_UPDT_TS
SqlUpdate = """
        UPDATE CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS_BLK_INSRT
        SET GEO_ADR_GIS_LON_QTY = ?
           ,GEO_ADR_GIS_LAT_QTY = ?
           ,GEO_ADR_GIS_MATCH_SCRE_NUM = ?
           ,GEO_ADR_GIS_MATCH_ADR = ?
           ,GEO_ADR_GIS_ADR_RULE_CD = ?
           ,IDR_UPDT_TS = CURRENT_TIMESTAMP(0)
        WHERE ADR_FULL = ? 
        AND ADD_GEO_SK = ?
        AND POSTAL_CD = ?
        AND POSTAL_EXT = ?
        AND CITY_NAME = ?
        AND USPS_STATE_CD = ? 
"""

#CURRENT_TIMESTAMP (format 'YYYY-MM-DDbHH:MI:SS')
# '2022-03-01 12:59:00'

SqlDelete = """
    DELETE FROM CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS_BLK_INSRT
"""

SqlListOfStates = """
    SELECT TRIM(GEO_USPS_STATE_CD) as GEO_USPS_STATE_CD
      FROM CMS_VIEW_GEO_CDEV.V1_GEO_USPS_STATE_CD 
    WHERE NOT GEO_USPS_STATE_CD IN  '~'
    AND       GEO_USPS_STATE_CD IN ('MD')
    ORDER BY 1
"""

###############################
# functions
###############################
def getDateTime():
    now = datetime.datetime.now()
    return now.strftime('%H:%M:%S.%f')


###############################################
# Asynchronous function
###############################################
def geoCodeThreadProcess(ThreadNum, rows):

    try:

        ###########################################
        # Set initial thread RC value
        ###########################################
        RC = 0

        ###########################################
        # Start main thread processing
        ###########################################
        rootLogger.info(f"Thread {ThreadNum} - Started at "+getDateTime())

        ###########################################
        # How many rows retrieved?
        ###########################################
        nofRows = len(rows)
        rootLogger.info(f"Thread {ThreadNum} - Retrieved {nofRows} rows.")

        ###########################################
        # Create delimited file from results-set
        ###########################################
        rootLogger.info(f"Thread {ThreadNum} - Converting results-set to csv file")

        csvFile = os.path.join(data_dir,f"resultsSet_{ThreadNum}.csv")
        #pdCSVFile = os.path.join(data_dir,f"pandasSet_{ThreadNum}.csv")
        SQLFncts.createCSVFile(csvFile, sCursorColNames, rows, ",")

        ##############################################
        # Call ArcGIS function to Geocode results-set
        ##############################################
        rootLogger.info(f"Thread {ThreadNum} - Calling geocoding module")
        time.sleep(3)

        ####################################################################
        # Format CSV file into correct order of fields   
        # needed for Update SQL statement.
        # NOTE: dtype=str --> prevents zip-code '00000' from being read and 
        #       written as '0'.
        ####################################################################
        df = pd.read_csv(csvFile, dtype=str, keep_default_na=False)
        #rootLogger.debug(pd)
        
        # Include ONLY required columns for SQL statement.
        df = df[['GEO_ADR_GIS_LON_QTY', 'GEO_ADR_GIS_LAT_QTY', 'GEO_ADR_GIS_MATCH_SCRE_NUM', 
            'GEO_ADR_GIS_MATCH_ADR','GEO_ADR_GIS_ADR_RULE_CD',
            'GEO_ADR_FULL_ADR', 'GEO_SK', 'GEO_ZIP5_CD', 'GEO_ZIP4_CD',
            'GEO_ADR_CITY_NAME', 'GEO_USPS_STATE_CD']]

        # Place columns in proper order
        dfReorderedCols = df.reindex(columns= ['GEO_ADR_GIS_LON_QTY', 'GEO_ADR_GIS_LAT_QTY', 
                                               'GEO_ADR_GIS_MATCH_SCRE_NUM', 
                                               'GEO_ADR_GIS_MATCH_ADR','GEO_ADR_GIS_ADR_RULE_CD', 
                                               'GEO_ADR_FULL_ADR', 'GEO_SK', 'GEO_ZIP5_CD', 'GEO_ZIP4_CD',
                                               'GEO_ADR_CITY_NAME', 'GEO_USPS_STATE_CD'])
                                              

        #rootLogger.debug(dfReorderedCols)
        ################################################## 
        # NOTE: keep_default_na causes 'NA' string to be
        #       converted to null value.
        ##################################################       
        dfReorderedCols.to_csv(csvFile, index=False)

        #########################################
        # Perform bulk insert into DB.
        #########################################
        rootLogger.info(f"Thread {ThreadNum} - Starting bulk insert/update into DB.")
        
        # NOTE: perform re-try logic if connection times out.
        bBulkUpdatedCompleted = False
        iRetryCount = 1
        
        while not bBulkUpdatedCompleted:
            try:
                #SQLFncts.bulkInsrtUpdtCSVReader(cnx, csvFile, SqlInsert, True)
                SQLFncts.bulkInsrtUpdtCSVReader(csvFile, SqlUpdate, True)
                bBulkUpdatedCompleted = True

            except Exception as e:
                rootLogger.info(f"Thread {ThreadNum} - Re-try Bulk Insert/Update.")
                iRetryCount += 1
                if iRetryCount > MAX_NOF_RETRIES:
                    bBulkUpdatedCompleted = True
                    # re-raise the error to be caught at end of this function
                    raise

        ##############################################
        # Thread clean-up: 1) Close DB connection
        #                  2) Remove files not needed.
        ##############################################
        rootLogger.info(f"Thread {ThreadNum} - Thread clean-up.")
        os.remove(csvFile)

        ###########################################
        # End main thread processing
        ###########################################
        rootLogger.info(f"Thread {ThreadNum} - Ended at "+getDateTime())

    except Exception as ex:
        rootLogger.error(ex)
        RC = 12

    ###########################################
    # write RC to RC file for driver to review
    ###########################################
    sThreadRCMsgs.append(f"Thread {str(ThreadNum)} RC = {str(RC)}")

    return RC


def mainThreadDriver(sSqlStmt,sStateList):

    ###############################
    # variables
    ###############################
    bEndofChunks = False
    activeThreads = []
    global iTotNOFRows
    iTotNOFRows = 0

    ###############################
    # Starting info
    ###############################
    rootLogger.info("Start function mainThreadDriver")

    ###################################################
    # get DB cursor
    ###################################################
    global sCursorColNames

    cursor = SQLFncts.getManyRowsCursor(sSqlStmt, None)
    sCursorColNames = SQLFncts.cursorColumnNames

    ###################################################
    # Create Initial x number of threads
    ###################################################
    rootLogger.info("Start initial threads")

    for i in range(1, MAX_NOF_ACTIVE_THREADS + 1):

        # get next chunk of data
        rows = SQLFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)
        #rootLogger.info("Column List: "+SQLFncts.getCursorColumnsCSVStr(","))

        # if no-more-data --> break from for-loop
        iNOFRows = len(rows)
        if iNOFRows == 0:
            bEndofChunks = True
            break
        else:
             iTotNOFRows += iNOFRows 

        th = Thread(target=geoCodeThreadProcess, args=(i, rows))
        activeThreads.append(th)

        rootLogger.info(f"Thread {th.name} started at "+getDateTime())
        th.start()
        rootLogger.info(f"Thread {th.name} id: "+str(th.native_id))

    ###################################################
    # Monitor processing of threads.
    # Continue to start threads until no-more-chunks
    # of data. Start a new thread only after a thread has 
    # completed.
    ###################################################
    rootLogger.info("Monitor threads")

    iThreadNum = MAX_NOF_ACTIVE_THREADS

    while not bEndofChunks:

        for t in activeThreads:
            if not t.is_alive():  
                rootLogger.info(f"Thread {t.name} completed.")

                # remove thread from array of active threads to monitor
                activeThreads.remove(t)

                # get next chunk of data
                rows = SQLFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)
                iNOFRows = len(rows)
                if iNOFRows == 0:
                    bEndofChunks = True
                    break
                else:
                    iTotNOFRows += iNOFRows 

                ################################        
                # start new thread
                ################################
                rootLogger.info("Start a new thread!")
                iThreadNum +=1
                th = Thread(target=geoCodeThreadProcess, args=(iThreadNum, rows))

                # add thread to list of threads to monitor
                activeThreads.append(th)

                rootLogger.info(f"Start new thread {th.name} started at "+getDateTime())
                th.start()
                rootLogger.info(f"Thread {th.name} id: "+str(th.native_id))
     
                
    ###################################################
    # wait for remaining active threads to complete
    ###################################################
    rootLogger.info("Waiting for threads to finish")

    # Wait for threads to complete
    for t in activeThreads:
        t.join()   



"""
def mainNoThreads():

    ###############################
    # variables
    ###############################
    bEndofChunks = False
    iTotNOFRows = 0
    iChunkNum = 0

    ###############################
    # Starting info
    ###############################
    StartJobTime = datetime.datetime.now()

    rootLogger.info("\n##########################")
    rootLogger.info("Start function mainNoThreads")

    ###################################################
    # get DB cursor
    ###################################################
    global sCursorColNames

    cursor = SQLFncts.getManyRowsCursor(SqlStmtGeo1, None)
    sCursorColNames = SQLFncts.cursorColumnNames

    ###################################################
    # Create Initial x number of threads
    ###################################################
    rootLogger.info("Start process")

    while bEndofChunks == False:
        # get next chunk of data
        rows = SQLFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)

        # if no-more-data 
        iNOFRows = len(rows)
        if iNOFRows == 0:
            bEndofChunks = True
            break
        else:
            iTotNOFRows += iNOFRows 
            iChunkNum += 1
            geoCodeThreadProcess(iChunkNum, rows)

                
    ###################################################
    # Set job RC from thread RCs.
    ###################################################        
    jobRC = 0
    rootLogger.info("jobRC = "+str(jobRC))

    ###################################################
    # Print statistics
    ################################################### 
    frmtNOFRows = "{:,}".format(iTotNOFRows)
    EndJobTime = datetime.datetime.now()
    ElapsedTime = str(EndJobTime - StartJobTime)

    rootLogger.info("Elapsed processing time: "+ str(ElapsedTime)) 
    rootLogger.info("Total NOF rows processed: "+ frmtNOFRows) 
    rootLogger.info("No Thread Process:")
    rootLogger.info("CHUNK_SIZE_NOF_ROWS:" + str(CHUNK_SIZE_NOF_ROWS))    

    sys.exit(jobRC)
"""

def getConfigValues():
    ###############################
    # Get App config filename
    ###############################
    try:

        rootLogger.info("Get App configuration values from config file.")

        curDir = os.getcwd()
        #configPath = os.path.join(curDir,"config")
        configPath = curDir
        configPathFilename = os.path.join(configPath,"GeoCodingConfig.cfg")
        if os.path.exists(configPathFilename):
            pass
        else:
            raise ConfigFileNotfnd(f"Configuration file notfnd: {configPathFilename}")

        ###############################
        # Extract configuration values
        ###############################
        config = configparser.ConfigParser()
        config.read(configPathFilename)

        global MAX_NOF_ACTIVE_THREADS
        global CHUNK_SIZE_NOF_ROWS
        global MAX_NOF_RETRIES
        global NOF_STATES_2_PROCESS_AT_ONCE
        global STATES_2_PROCESS

        MAX_NOF_ACTIVE_THREADS = config.get('RunParms','maxThreads')
        CHUNK_SIZE_NOF_ROWS = config.get('RunParms','chunkSize')
        MAX_NOF_RETRIES = config.get('RunParms','maxRetries')
        NOF_STATES_2_PROCESS_AT_ONCE = config.get('RunParms','NOFStatesInDBCursor')
        STATES_2_PROCESS = config.get('RunParms','States2Process')

        rootLogger.info(f"MAX_NOF_ACTIVE_THREADS={MAX_NOF_ACTIVE_THREADS}")
        rootLogger.info(f"CHUNK_SIZE_NOF_ROWS={CHUNK_SIZE_NOF_ROWS}")
        rootLogger.info(f"MAX_NOF_RETRIES={MAX_NOF_RETRIES}")
        rootLogger.info(f"NOF_STATES_2_PROCESS_AT_ONCE={NOF_STATES_2_PROCESS_AT_ONCE}")
        rootLogger.info(f"STATES_2_PROCESS={STATES_2_PROCESS}")

    except Exception as ex:
        rootLogger.error(ex)
        raise


def main():
    ########################################################
    # Main program driver: 
    #    Iterates thru all available states passing a subset 
    #    of states each time it calls mainThreadDriver
    ########################################################
    try:

        StartJobTime = datetime.datetime.now()

        rootLogger.info("\n##########################")
        rootLogger.info("Start function main")

        #SQLFncts.DeleteRows(SqlDelete, None)

        #################################################
        # get Configuration Values
        #################################################
        getConfigValues()

        sys.exit(0)

        #################################################
        # get all states that need addresses geocoded
        #################################################
        arrListOfStates = SQLFncts.getAllRows(SqlListOfStates, None)
        maxNOFStates = len(arrListOfStates)

        rootLogger.info(f"NOF States to process:{maxNOFStates}")

        #################################################
        # 1) Extract a subset of states 
        # 2) Format State In-Phrase
        # 3) Replace parm marker with State In-Phrase
        # 4) Call mainThreadDriver
        #################################################
        for i in range (0, maxNOFStates, NOF_STATES_2_PROCESS_AT_ONCE):
            stateList = arrListOfStates[i : i+NOF_STATES_2_PROCESS_AT_ONCE]

            l = [",".join(st) for st in stateList]
            sStateList = "'" + "','".join(l) + "'"
            rootLogger.info(f"List of States to process: {sStateList} ")

            #########################################################
            # Replace param marker with list of states in SELECT. 
            # NOTE: Did it this way because single parm with commas 
            #       was being treated as separate parms. 
            #########################################################
            sSQL2Process = SqlStmtGeo1.replace("?",sStateList)
            #rootLogger.debug(sSQL2Process)
            mainThreadDriver(sSQL2Process,sStateList)


        ###################################################
        # Set job RC from thread RCs.
        ###################################################        
        jobRC = 0
        rootLogger.debug("Thread RCs")

        for sRCMsg in sThreadRCMsgs:
            rootLogger.debug(sRCMsg)
            arrMsg = sRCMsg.split("=")
            threadRC = int(arrMsg[1].strip())
            if threadRC != 0:
                jobRC = threadRC

        ###################################################
        # Print statistics
        ################################################### 
        frmtNOFRows = "{:,}".format(iTotNOFRows)
        EndJobTime = datetime.datetime.now()
        ElapsedTime = str(EndJobTime - StartJobTime)

        rootLogger.info("jobRC = "+str(jobRC))

        rootLogger.info("Elapsed processing time: "+ str(ElapsedTime)) 
        rootLogger.info(f"States processed: {sStateList}") 
        rootLogger.info("Total NOF rows processed: "+ frmtNOFRows) 
        rootLogger.info("MAX_NOF_ACTIVE_THREADS: " + str(MAX_NOF_ACTIVE_THREADS))
        rootLogger.info("CHUNK_SIZE_NOF_ROWS: " + str(CHUNK_SIZE_NOF_ROWS))    

        sys.exit(jobRC)

    except Exception as ex:
        rootLogger.error("Processing terminated because of errors!")


if __name__ == "__main__":  # confirms that the code is under main function

    try:
        main()
        #mainNoThreads()
    except Exception as e:
        rootLogger.error(e)


