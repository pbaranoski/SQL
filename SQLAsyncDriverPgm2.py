# Asynchronous Thread Process Driver
# author: Paul Baranoski
# 2022-02-14
#
from threading import Thread
import multiprocessing

import SQLTeraDataFncts

import time
import os
import sys
import datetime
import logging

# process and system utilities) is a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) 
#psutil (FYI)

MAX_NOF_ACTIVE_THREADS = 10
CHUNK_SIZE_NOF_ROWS = 15000

sThreadRCMsgs = []

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
    format="%(asctime)s %(levelname)-8s %(threadName)-12s %(funcName)-20s %(message)s",
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

###############################
# functions
###############################
def getDateTime():
    now = datetime.datetime.now()
    return now.strftime('%H:%M:%S.%f')


###############################################
# Asynchronous function
###############################################
def geoCodeChildProcess(ThreadNum, rows):

    try:

        ###########################################
        # Set initial thread RC value
        ###########################################
        RC = 0

        ###########################################
        # Create logger for thread
        ###########################################
        """
        threadLogfile = os.path.join(log_dir,f"GeoThread_{ThreadNum}.log")
        
        threadLogger = logging.getLogger(f"GeoThread_{ThreadNum}") 
        threadLogger.setLevel(logging.INFO)
        fh = logging.FileHandler(threadLogfile, "w", encoding='utf-8')
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s")
        
        fh.setFormatter(formatter)
        threadLogger.addHandler(fh)
        """
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
        SQLTeraDataFncts.createCSVFile(csvFile, sCursorColNames, rows, ",")

        rootLogger.info(f"Thread {ThreadNum} - Calling geocoding module")
        time.sleep(3)

        rootLogger.info(f"Thread {ThreadNum} - Starting bulk insert into DB.")
        time.sleep(3)

        #rootLogger.info(f"Thread {ThreadNum} - Geocoding process ended.")

        ###########################################
        # Thread clean-up: remove files not needed.
        ###########################################
        #os.listdir(os.getcwd())
        rootLogger.info(f"Thread {ThreadNum} - Thread clean-up.")
        os.remove(csvFile)

        ###########################################
        # End main thread processing
        ###########################################
        rootLogger.info(f"Thread {ThreadNum} - Ended at "+getDateTime())

    except Exception as ex:
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
    threads = []

    ###############################
    # Starting info
    ###############################
    StartJobTime = datetime.datetime.now()

    rootLogger.info("\n##########################")
    rootLogger.info("Start function main")

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


        th = multiprocessing.Process(target=geoCodeChildProcess, args=(i, rows))
        threads.append(th)

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

        for t in threads:
            if not t.is_alive():  
                rootLogger.info(f"Thread {t.name} completed.")

                # remove thread from array of active threads to monitor
                threads.remove(t)

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
                th = multiprocessing.Process(target=geoCodeChildProcess, args=(iThreadNum, rows))

                # add thread to list of threads to monitor
                threads.append(th)

                rootLogger.info(f"Start new thread {th.name} started at "+getDateTime())
                th.start()
                rootLogger.info(f"Thread {th.name} id: "+str(th.pid))
     
                
    ###################################################
    # wait for remaining active threads to complete
    ###################################################
    rootLogger.info("Waiting for threads to finish")

    # Wait for threads to complete
    for t in threads:
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


if __name__ == "__main__":  # confirms that the code is under main function
    main()
