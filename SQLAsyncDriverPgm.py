# Asynchronous Process Driver
# author: Paul Baranoski
# 2022-02-14
#
from threading import Thread

import SQLTeraDataFncts

import time
import os
import sys
import datetime
import logging

# process and system utilities) is a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) 
#psutil (FYI)

MAX_NOF_ACTIVE_THREADS = 5
CHUNK_SIZE_NOF_ROWS = 10

sThreadRCMsgs = []

###############################
# Create log path+filename
###############################
log_dir = os.path.join(os.getcwd(), "logs")
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

mainLogfile = os.path.join(log_dir,"geoMainProcess.log")

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

###############################
# functions
###############################
def getManyRows():

    ##################################################
    # Get results-set in subsets of records
    ##################################################
    cursor = SQLTeraDataFncts.getManyRowsCursor(SqlStmtGeo2, None)

    ##################################################
    # Print results-set by 1) full row 2) flds in row
    ##################################################
    SQLTeraDataFncts.logger.info("Fetching Many rows-->")
    rows = SQLTeraDataFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)
    while len(rows) > 0:
        SQLTeraDataFncts.logger.info("NOF rows in results-set: "+str(len(rows)))
        SQLTeraDataFncts.logger.info(len(rows))
        #for row in rows:
        #    SQLTeraDataFncts.logger.info("full record: "+str(row))
            #SQLTeraDataFncts.logger.info("print fields in record:")
            #for fld in row:
            #    SQLTeraDataFncts.logger.info(fld)
    
        rows = SQLTeraDataFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)        


def getDateTime():
    now = datetime.datetime.now()
    return now.strftime('%H:%M:%S.%f')


###############################################
# Asynchronous function
###############################################
def geoCodeChildProcess(ThreadNum, rows):

    ###########################################
    # Set initial thread RC value
    ###########################################
    RC = 0

    ###########################################
    # Create logger for thread
    ###########################################
    threadLogfile = os.path.join(log_dir,f"GeoThread_{ThreadNum}.log")
    
    threadLogger = logging.getLogger(f"GeoThread_{ThreadNum}") 
    threadLogger.setLevel(logging.INFO)
    fh = logging.FileHandler(threadLogfile, "w", encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(funcName)-12s %(message)s")
    
    fh.setFormatter(formatter)
    threadLogger.addHandler(fh)

    ###########################################
    # Start main thread processing
    ###########################################
    threadLogger.info(f"Thread {ThreadNum} - Started at "+getDateTime())

    ###########################################
    # Do processing
    ###########################################
    threadLogger.info(f"Thread {ThreadNum} - Geocoding process started.")
    time.sleep(5+ThreadNum)

    threadLogger.info(f"Thread {ThreadNum} - Retrieved results-set.")

    #for row in rows:
    #    threadLogger.info("full record: "+str(row))
    nofRows = len(rows)
    threadLogger.info(f"Thread {ThreadNum} - Retrieved {nofRows} rows.")

    threadLogger.info(f"Thread {ThreadNum} - Converted results-set to csv file")

    threadLogger.info(f"Thread {ThreadNum} - Calling geocoding module")
    time.sleep(1)

    threadLogger.info(f"Thread {ThreadNum} - Starting bulk insert into DB.")
    time.sleep(1)

    threadLogger.info(f"Thread {ThreadNum} - Geocoding process ended.")

    ###########################################
    # End main thread processing
    ###########################################
    threadLogger.info(f"Thread {ThreadNum} - Ended at "+getDateTime())

    ###########################################
    # write RC to RC file for driver to review
    ###########################################
    sThreadRCMsgs.append(f"Thread {str(ThreadNum)} RC = {str(RC)}")

    return RC


def main():

    ###############################
    # Config main logfile
    ###############################
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(threadName)-12s %(funcName)-20s %(message)s",
        encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
        handlers=[
        logging.FileHandler(mainLogfile),
        logging.StreamHandler(sys.stdout)],    
        level=logging.INFO)

    rootLogger = logging.getLogger() 

    rootLogger.info("\n##########################")
    rootLogger.info("Start function main")

    ###################################################
    # get DB cursor
    ###################################################
    #getManyRows()
    #sys.exit(0)

    bEndofChunks = False
    cursor = SQLTeraDataFncts.getManyRowsCursor(SqlStmtGeo2, None)

    ###################################################
    # Create Initial x number of threads
    ###################################################
    rootLogger.info("Start initial threads")

    threads = []

    for i in range(1, MAX_NOF_ACTIVE_THREADS + 1):

        # get next chunk of data
        rows = SQLTeraDataFncts.getManyRowsNext(cursor, CHUNK_SIZE_NOF_ROWS)
        # if no-more-data --> break from for-loop
        if len(rows) == 0:
            bEndofChunks = True
            break

        th = Thread(target=geoCodeChildProcess, args=(i, rows))
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
                if len(rows) == 0:
                    bEndofChunks = True
                    break

                ################################        
                # start new thread
                ################################
                rootLogger.info("Start a new thread!")
                iThreadNum +=1
                th = Thread(target=geoCodeChildProcess, args=(iThreadNum, rows))

                # add thread to list of threads to monitor
                threads.append(th)

                rootLogger.info(f"Start new thread {th.name} started at "+getDateTime())
                th.start()
                rootLogger.info(f"Thread {th.name} id: "+str(th.native_id))
     
                
    ###################################################
    # wait for remaining active threads to complete
    ###################################################
    rootLogger.info("Waiting for threads to finish")

    # Wait for threads to complete
    for t in threads:
        t.join()   
    
    jobRC = 0
    rootLogger.info("Thread RCs")

    for sRCMsg in sThreadRCMsgs:
        rootLogger.info(sRCMsg)
        arrMsg = sRCMsg.split("=")
        threadRC = arrMsg[1]
        if threadRC != 0:
            jobRC = threadRC

    rootLogger.info("jobRC = "+str(jobRC))

    sys.exit(jobRC)

if __name__ == "__main__":  # confirms that the code is under main function
    main()
