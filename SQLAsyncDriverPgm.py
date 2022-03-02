# Asynchronous Process Driver
# author: Paul Baranoski
# 2022-02-14
#
from threading import Thread

import SQLTeraDataFncts

import pandas as pd
import time
import os
import sys
import datetime
import logging

# process and system utilities) is a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) 
#psutil (FYI)

MAX_NOF_ACTIVE_THREADS = 15
CHUNK_SIZE_NOF_ROWS = 10000

sThreadRCMsgs = []
#TotRows = []

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

mainLogfile = os.path.join(log_dir,"geoMainProcess.log")

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
    WHERE GEO_USPS_STATE_CD = 'MD'
 ;
"""
#     AND GEO_ZIP5_CD = '21204'
##    AND GEO_ZIP5_CD = '21236'
#     AND GEO_ADR_LINE_1_ADR like '%BELAIR RD%'
#   AND GEO_ADR_LINE_1_ADR like '9657 BELAIR RD UNIT 1474%'

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
        SQLTeraDataFncts.createCSVFile(csvFile, sCursorColNames, rows, ",")

        ##############################################
        # Call ArcGIS function to Geocode results-set
        ##############################################
        rootLogger.info(f"Thread {ThreadNum} - Calling geocoding module")
        time.sleep(3)

        ##############################################
        # Format CSV file into correct order of fields   
        # needed for Update SQL statement.
        ##############################################
        df = pd.read_csv(csvFile)
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
        dfReorderedCols.to_csv(csvFile, index=False)

        #########################################
        # Perform bulk insert into DB.
        #########################################
        rootLogger.info(f"Thread {ThreadNum} - Starting bulk insert into DB.")
        
        cnx = SQLTeraDataFncts.getConnection()
        if cnx is None:
            raise SQLTeraDataFncts.NullConnectException(f"Thread {ThreadNum} could not connect to DB.")    

        #SQLTeraDataFncts.bulkInsertTDReadCSV(cnx, csvFile, SqlInsert)
        #SQLTeraDataFncts.bulkInsrtUpdtCSVReader(cnx, csvFile, SqlInsert, True)
        SQLTeraDataFncts.bulkInsrtUpdtCSVReader(cnx, csvFile, SqlUpdate, True)

        ##############################################
        # Thread clean-up: 1) Close DB connection
        #                  2) Remove files not needed.
        ##############################################
        SQLTeraDataFncts.closeConnection(cnx)

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


def main():

    ###############################
    # variables
    ###############################
    bEndofChunks = False
    iTotNOFRows = 0
    activeThreads = []

    ###############################
    # Starting info
    ###############################
    StartJobTime = datetime.datetime.now()

    rootLogger.info("\n##########################")
    rootLogger.info("Start function main")

    #SQLTeraDataFncts.DeleteRows(SqlDelete, None)

    ###################################################
    # get DB cursor
    ###################################################
    global sCursorColNames

    cursor = SQLTeraDataFncts.getManyRowsCursor(SqlStmtGeo1, None)
    sCursorColNames = SQLTeraDataFncts.cursorColumnNames

    ###################################################
    # Create Initial x number of threads
    ###################################################
    rootLogger.info("Start initial threads")

    for i in range(1, MAX_NOF_ACTIVE_THREADS + 1):

        # get next chunk of data
        rows = SQLTeraDataFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)
        #rootLogger.info("Column List: "+SQLTeraDataFncts.getCursorColumnsCSVStr(","))

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
                rows = SQLTeraDataFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)
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

    ###################################################
    # Set job RC from thread RCs.
    ###################################################        
    jobRC = 0
    rootLogger.debug("Thread RCs")

    for sRCMsg in sThreadRCMsgs:
        rootLogger.debug(sRCMsg)
        arrMsg = sRCMsg.split("=")
        threadRC = arrMsg[1]
        if threadRC != 0:
            jobRC = threadRC

    rootLogger.info("jobRC = "+str(jobRC))

    ###################################################
    # Print statistics
    ################################################### 
    frmtNOFRows = "{:,}".format(iTotNOFRows)
    EndJobTime = datetime.datetime.now()
    ElapsedTime = str(EndJobTime - StartJobTime)

    rootLogger.info("Elapsed processing time: "+ str(ElapsedTime)) 
    rootLogger.info("Total NOF rows processed: "+ frmtNOFRows) 
    rootLogger.info("MAX_NOF_ACTIVE_THREADS:" + str(MAX_NOF_ACTIVE_THREADS))
    rootLogger.info("CHUNK_SIZE_NOF_ROWS:" + str(CHUNK_SIZE_NOF_ROWS))    

    sys.exit(jobRC)


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

    cursor = SQLTeraDataFncts.getManyRowsCursor(SqlStmtGeo1, None)
    sCursorColNames = SQLTeraDataFncts.cursorColumnNames

    ###################################################
    # Create Initial x number of threads
    ###################################################
    rootLogger.info("Start process")

    while bEndofChunks == False:
        # get next chunk of data
        rows = SQLTeraDataFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)

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


if __name__ == "__main__":  # confirms that the code is under main function
    main()
    #mainNoThreads()


