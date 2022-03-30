# Asynchronous Process Driver
# Author: Paul Baranoski
# 2022-02-14
#

#import arcpy
#from arcpy import management
#from arcpy import geocoding
#from arcpy import metadata as md

import SQLTeraDataFncts as SQLFncts

#from threading import Thread
import threading

import pandas as pd
import time
import os
import sys
import datetime
import logging
import logging.config
import configparser


class ConfigFileNotfnd(Exception):
    "Configuration file not found: "

class NextStateRunException(Exception):
    "A thread for a state run generated a RC 12. "

# global parameter values    
MAX_NOF_ACTIVE_THREADS = 15
CHUNK_SIZE_NOF_ROWS = 10000

MAX_NOF_RETRIES = 3
NOF_STATES_2_PROCESS_AT_ONCE = 2
STATES_2_PROCESS = ""

# Array that stores RC of threads
sThreadRCMsgs = []
# Array that stores list of job stats
arrJobStatMsgs = []

# Array of states to process for Job--> 'ALL' or csv-list of states
arrListOfStates = None

# Total NOF rows processed by Job
iTotNOFRows = 0

#####################################
# 1) Create and configure log file
# 2) Ensure that temp directory exists
#####################################
app_dir = os.getcwd()
log_dir = os.path.join(app_dir, "logs")
data_dir = os.path.join(app_dir, "temp")
config_dir = os.path.join(app_dir, "config")

if not os.path.exists(log_dir):
    os.mkdir(log_dir)
if not os.path.exists(data_dir):
    os.mkdir(data_dir)

dttm = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

rptFile = os.path.join(data_dir,f"geoCodingSummaryReport_{dttm}.csv")
mainLogfile = os.path.join(log_dir,f"geoMainProcess_{dttm}.log")

# Configure root logger
logging.config.fileConfig(os.path.join(config_dir,"loggerConfig.cfg"))
"""
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(threadName)-12s %(funcName)-22s %(message)s",
    encoding='utf-8', datefmt="%Y-%m-%d %H:%M:%S", 
    handlers=[
    logging.FileHandler(mainLogfile),
    logging.StreamHandler(sys.stdout)],    
    level=logging.DEBUG)
"""

rootLogger = logging.getLogger() 

# Add FileHandler to logger --> allows me to create dynamically named log files.
fh = logging.FileHandler(mainLogfile)
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(threadName)-12s %(funcName)-22s %(message)s", datefmt="%Y-%m-%d %H:%M:%S" )
fh.setFormatter(formatter)
rootLogger.addHandler(fh)

#########################################
# Global variables for ARCGis functions
#########################################
addr_loc = "E:/someplace/the address locator"
fgdb_path = r"E:/RoutingServices/StreetData/NorthAmerica.gdb"  # The address database
#item_md = md.Metadata(fgdb_path)
item_md = "NorthAmerica.gdb"

Preferred_Location_Type = 'ROUTING_LOCATION'
Output_Fields = 'ALL'
#addr_fields = arcpy.FieldInfo()

input_mappings = {"Address or Place": "ADDRESS",
				   "Address2":"<None>",
				   "Address3":"<None>",
                   "Neighborhood":"<None>",
                   "City":"CITY",
                   "State" : "REGION",
                   "ZIP" : "POSTAL",
                   "ZIP4": "POSTALEXT",
				   "Country":"<None>"
                  }

#for field in input_mappings:
#    addr_fields.addField(field, input_mappings[field], "VISIBLE", "NONE")


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
    AND GEO_ZIP5_CD = '88888'
 ;
"""
#     AND GEO_ZIP5_CD = '21204'
##    AND GEO_ZIP5_CD = '21236'
#     AND GEO_ZIP5_CD = '88888'
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

SqlListOfSomeStates = """
    SELECT TRIM(GEO_USPS_STATE_CD) as GEO_USPS_STATE_CD
      FROM CMS_VIEW_GEO_CDEV.V1_GEO_USPS_STATE_CD 
    WHERE NOT GEO_USPS_STATE_CD IN  '~'
    AND       GEO_USPS_STATE_CD IN (?)
    ORDER BY 1
"""

SqlListOfAllStates = """
    SELECT TRIM(GEO_USPS_STATE_CD) as GEO_USPS_STATE_CD
      FROM CMS_VIEW_GEO_CDEV.V1_GEO_USPS_STATE_CD 
    WHERE NOT GEO_USPS_STATE_CD IN  '~'
    ORDER BY 1
"""

###############################
# functions
###############################
def getDateTime():
    now = datetime.datetime.now()
    return now.strftime('%H:%M:%S.%f')


###############################################
# geocode addresses process
###############################################
def performGeocoding(ThreadNum, csvFile):

    try:

        rootLogger.info("Start function performGeocoding().")

        ####################################
        # Create file GDB (geodatabase)
        ####################################
        temp_gdb = os.path.join(data_dir, f"geocode_out_{ThreadNum}.gdb")
        temp_gdb = temp_gdb.replace("\\", "/")
        rootLogger.debug(f"temp_gdb: {temp_gdb}")

        #if arcpy.Exists(temp_gdb):
        #    arcpy.management.Delete(temp_gdb)
        #arcpy.management.CreateFileGDB(os.path.dirname(temp_gdb), os.path.basename(temp_gdb))

        feature_class_name = f"GEO_ADDRESS_FC_{ThreadNum}"
        out_feature_class = os.path.join(temp_gdb, feature_class_name)
        out_feature_class = out_feature_class.replace("\\","/")
        rootLogger.debug(f"out_feature_class: {out_feature_class}")        

        ####################################
        # Geocode Addresses
        ####################################
        rootLogger.info("Call function arcpy.GeocodeAddresses().")
        #result = arcpy.geocoding.GeocodeAddresses(csvFile, addr_loc, addr_fields, out_feature_class, 'STATIC', None, Preferred_Location_Type, None, Output_Fields)

        geocoded_tablecount = 10008
        #geocoded_tablecount = arcpy.management.GetCount(out_feature_class)[0] #debug for count
        rootLogger.info(f"geocoded_tablecount: {geocoded_tablecount}")

        ####################################################
        # Convert out_feature_class to input for bulk update
        ####################################################
        rootLogger.info(f"Convert out_feature_class for input into bulk update.")

        field_names = ['Score', 'Match_addr', 'Addr_type', 'X', 'Y', 'USER_FID']

        out_feature_cur = [['98', '1313 Mockingbird Lane', 'Addr_type', 56.78, 45.99, 808]]
        #out_feature_cur = da.SearchCursor(out_feature_class, field_names=field_names,
        #                              sql_clause=(None, 'ORDER BY USER_FID ASC'))

        # Create DataFrame from list 
        dfOutFeatureClass = pd.DataFrame(out_feature_cur)
        #print(dfOutFeatureClass)
        # Add column to dataFrame before SK column
        dfOutFeatureClass.insert (loc = 5, column='STREETMAP_VERSION', value=item_md.title())
        #print(dfOutFeatureClass)
        #dfOutFeatureClass.to_csv()
        #dfOutFeatureClass.values.tolist()

        ###########################################
        # Perform clean-up
        ###########################################
        #arcpy.management.Delete(temp_gdb)
        rootLogger.info(f"Geocoding clean-up complete.")

        ###########################################
        # return list or csv.
        # with List --> need to add database function 
        #     to use list as input. 
        ###########################################

        return csvFile

    #except arcpy.ExecuteError as arcEx:
    #   rootLogger.error("Arcpy processing error.")
    #   arcpy.GetMessages())
    #   raise

    except Exception as e:
        rootLogger.error("Error in geocoding process.")
        rootLogger.error(e)
        raise


###############################################
# Thread function to execute Asynchronously.
###############################################
def geoCodeThreadProcess(rows):

    try:
        ###########################################
        # Set initial thread RC value
        ###########################################
        RC = 0

        ###########################################
        # Start main thread processing
        ###########################################
        rootLogger.info(f"Thread started at "+getDateTime())

        ###########################################
        # Extract Thread num from ThreadName
        # Note: ThreadName = "Thread-N"
        ###########################################
        ThreadName = threading.current_thread().getName()
        arr = ThreadName.split("-")
        ThreadNum = arr[1]
        #rootLogger.debug(f"extracted thread num: |{ThreadNum}|")

        ###########################################
        # How many rows retrieved?
        ###########################################
        nofRows = len(rows)
        rootLogger.info(f"Thread {ThreadNum} - Retrieved {nofRows} rows.")

        ##################################################
        # Create input format for geocoding process
        # --> (Create delimited file from results-set)
        ##################################################
        rootLogger.info(f"Thread {ThreadNum} - Converting results-set to csv file")

        csvFile = os.path.join(data_dir,f"resultsSet_{ThreadNum}.csv")
        #pdCSVFile = os.path.join(data_dir,f"pandasSet_{ThreadNum}.csv")
        SQLFncts.createCSVFile(csvFile, sCursorColNames, rows, ",")

        ##############################################
        # Perform Geocoding
        ##############################################
        rootLogger.info(f"Thread {ThreadNum} - Calling geocoding module")

        performGeocoding(ThreadNum, csvFile)

        time.sleep(3)

        ####################################################################
        # Create data format for bulk update. 
        # NOTE: (May just be retreiving output from performGeocoding function.
        #       So, the below code may not be necessary.
        #
        # Format CSV file into correct order of fields needed for Update SQL 
        #     statement.
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
                iRetryCount += 1
                if iRetryCount > MAX_NOF_RETRIES:
                    bBulkUpdatedCompleted = True
                    # re-raise the error to be caught at end of this function
                    raise
                else:
                    rootLogger.info(f"Thread {ThreadNum} - Re-try Bulk Insert/Update.")
                    time.sleep(3)

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


def mainThreadDriver(sSqlStmt, sStateList):
    ###############################
    # variables
    ###############################
    global iTotNOFRows
    global sThreadRCMsgs
    
    stateRunRC = 0
    iStateNOFRows = 0
    bEndofChunks = False
    activeThreads = []
    sThreadRCMsgs = []

    ###############################
    # Process next set of states
    ###############################
    try:

        ###############################
        # Starting info
        ###############################
        rootLogger.info("Start function mainThreadDriver")

        StartNextStatesTime = datetime.datetime.now()

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
            #rootLogger.debug("Column List: "+SQLFncts.getCursorColumnsCSVStr(","))

            # if no-more-data --> break from for-loop
            iNOFRows = len(rows)
            if iNOFRows == 0:
                bEndofChunks = True
                break
            else:
                iStateNOFRows += iNOFRows 

            th = threading.Thread(target=geoCodeThreadProcess, args=(rows,))
            activeThreads.append(th)

            rootLogger.info(f"Thread {th.name} started at "+getDateTime())
            th.start()
            #rootLogger.info(f"Thread {th.name} id: "+str(th.native_id))
            rootLogger.info(f"Thread {th.name} id: "+str(th.ident))

        ###################################################
        # Monitor processing of threads.
        # 1) Continue to start threads until no-more-chunks
        # of data. 
        # 2) Start a new thread only after a thread has 
        # completed.
        ###################################################
        rootLogger.info("Monitor threads")

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
                        iStateNOFRows += iNOFRows 

                    ################################        
                    # start new thread
                    ################################
                    rootLogger.info("Start a new thread!")

                    th = threading.Thread(target=geoCodeThreadProcess, args=(rows,))

                    # add thread to list of threads to monitor
                    activeThreads.append(th)

                    rootLogger.info(f"Start new thread {th.name} started at "+getDateTime())
                    th.start()
                    #rootLogger.info(f"Thread {th.name} id: "+str(th.native_id))
                    rootLogger.info(f"Thread {th.name} id: "+str(th.ident))
        
                    
        ###################################################
        # wait for remaining active threads to complete
        ###################################################
        rootLogger.info("Waiting for threads to finish")

        # Wait for threads to complete
        for t in activeThreads:
            t.join()   

        ###################################################
        # Set job RC from thread RCs.
        # Note: set RC for each set of states processed.
        ###################################################        
        stateRunRC = 0
        rootLogger.debug("Thread RCs")

        for sRCMsg in sThreadRCMsgs:
            rootLogger.debug(sRCMsg)
            arrMsg = sRCMsg.split("=")
            threadRC = int(arrMsg[1].strip())
            if threadRC != 0:
                stateRunRC = threadRC

        if stateRunRC == 12:
            raise NextStateRunException("A thread for next state processing generated a RC 12.")
 

    except Exception as ex:
        stateRunRC = 12
        # re-raise the error to be caught by main function
        raise

    finally:
        ###################################################
        # Capture NextRun stats.
        ###################################################
        EndNextStatesTime = datetime.datetime.now()
        StatesElapsedTime = str(EndNextStatesTime - StartNextStatesTime)
        frmtNOFRows = "{:,}".format(iStateNOFRows)
        iTotNOFRows += iStateNOFRows

        ###################################################
        # Write stats to logfile.
        ###################################################
        rootLogger.info("Next set of state(s) processing completed.")
        rootLogger.info(f"State(s) processed: {sStateList}") 
        rootLogger.info("Elapsed processing time: "+ str(StatesElapsedTime)) 
        rootLogger.info("Total NOF rows processed: "+ frmtNOFRows) 
        rootLogger.info("Next set of state(s) RC = "+str(stateRunRC))
        rootLogger.info("*************************************")

        ###################################################
        # Write next state(s) run stats to array.
        ###################################################
        # Reformat list of states for report
        #     sStateList = "'AL','MD'" --> "AL MD" 
        sStList = str(sStateList.replace(","," ")).replace("'","")
        lst = list([sStList, "'"+str(StatesElapsedTime), frmtNOFRows, str(stateRunRC) ])
        arrJobStatMsgs.append(lst)


def getAppConfigValues():
    ###############################
    # Get App config filename
    ###############################
    try:

        rootLogger.info("Get App configuration values from config file.")

        configPathFilename = os.path.join(config_dir,"GeoCodingConfig.cfg")
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

        MAX_NOF_ACTIVE_THREADS = int(config.get('RunParms','maxThreads'))
        CHUNK_SIZE_NOF_ROWS = int(config.get('RunParms','chunkSize'))
        MAX_NOF_RETRIES = int(config.get('RunParms','maxRetries'))
        NOF_STATES_2_PROCESS_AT_ONCE = int(config.get('RunParms','NOFStatesAtOnce'))
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
    #    Iterates thru all states to be processed,
    #       passing a subset of states each time it calls 
    #       mainThreadDriver.
    # NOTE: NOF_STATES_2_PROCESS_AT_ONCE = subset of states.
    ########################################################
    jobRC = 0

    try:
        StartJobTime = datetime.datetime.now()

        rootLogger.info("\n##########################")
        rootLogger.info("Start function main")

        # Delete rows when inserting
        #SQLFncts.DeleteRows(SqlDelete, None)

        #################################################
        # get Configuration Values
        #################################################
        getAppConfigValues()

        #################################################
        # get list of states that need addresses geocoded
        #################################################
        if STATES_2_PROCESS == "ALL":
            sSQLStates2Process = SqlListOfAllStates
        else:
            arrStates = STATES_2_PROCESS.split(",")
            sStates2Process = "'" + "','".join(arrStates) + "'"
            rootLogger.debug(sStates2Process)
            # NOTE: For In-phrase, passing a comma-delim list is treated as muliple parms instead of 
            # one parm. This results in an error when only one parameter marker is in SQL.
            sSQLStates2Process = SqlListOfSomeStates.replace("?",sStates2Process)            


        rootLogger.debug(sSQLStates2Process)
        arrListOfStates = SQLFncts.getAllRows(sSQLStates2Process, None)

        maxNOFStates = len(arrListOfStates)
        rootLogger.info(f"NOF States to process: {maxNOFStates}")

        #################################################
        # 1) Extract a subset of states to process
        # 2) Format State In-Phrase
        # 3) Replace parm marker with State In-Phrase
        # 4) Call mainThreadDriver
        #################################################
        for i in range (0, maxNOFStates, NOF_STATES_2_PROCESS_AT_ONCE):
            stateList = arrListOfStates[i : i+NOF_STATES_2_PROCESS_AT_ONCE]

            l = [",".join(st) for st in stateList]
            sStateList = "'" + "','".join(l) + "'"
            rootLogger.info(f"Next Set of States to process: {sStateList} ")

            #########################################################
            # Replace param marker with list of states in SELECT. 
            # NOTE: In-phrase csv-list of values is treated as if 
            #       multiple parms instead of one. 
            #########################################################
            sSQL2Process = SqlStmtGeo1.replace("?",sStateList)
            #rootLogger.debug(sSQL2Process)
            mainThreadDriver(sSQL2Process, sStateList)


    except Exception as ex:
        jobRC = 12
        rootLogger.error(ex)
        rootLogger.error("Processing terminated because of errors!")

    finally:
        ###################################################
        # create total stats
        ################################################### 
        frmtNOFRows = "{:,}".format(iTotNOFRows)
        EndJobTime = datetime.datetime.now()
        ElapsedTime = str(EndJobTime - StartJobTime)

        lst = list(["Total Run", "'"+str(ElapsedTime), frmtNOFRows, str(jobRC) ])
        arrJobStatMsgs.append(lst)

        ###################################################
        # write log messages
        ################################################### 
        rootLogger.info("jobRC = "+str(jobRC))

        rootLogger.info("Elapsed processing time: "+ str(ElapsedTime)) 
        rootLogger.info(f"States processed: {STATES_2_PROCESS}") 
        rootLogger.info("Total NOF rows processed: "+ frmtNOFRows) 
        rootLogger.info("MAX_NOF_ACTIVE_THREADS: " + str(MAX_NOF_ACTIVE_THREADS))
        rootLogger.info("CHUNK_SIZE_NOF_ROWS: " + str(CHUNK_SIZE_NOF_ROWS))    

        ###################################################
        # create report from vsc file.
        ###################################################
        sHeader = list(["State(s) processed", "ElapsedTime", "NOF Rows Processed", "RC"])
        SQLFncts.createCSVFile(rptFile, sHeader, arrJobStatMsgs, ",")
        #rootLogger.debug(arrJobStatMsgs)
        
        sys.exit(jobRC)


if __name__ == "__main__":  # confirms that the code is under main function

    main()



