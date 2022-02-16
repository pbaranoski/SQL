# SQL test

#import datetime
#import logging
import sys
import os
import SQLTeraDataFncts

# Needed if we use Pandas to perform DBMS actions
import pandas

# Create log path+filename
csv_dir = os.path.join(os.getcwd(), "tempPandas")
csvFile = os.path.join(csv_dir,"insert.csv")
csvFastExportFile = os.path.join(csv_dir,"fastexport.csv")

bulkInsertCSVFile = os.path.join(csv_dir,"insert1.csv")

SqlInsert = """
        INSERT INTO CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS (
        ADR_LINE_1, ADR_LINE_2, ADR_LINE_3, ADR_LINE_4, ADR_LINE_5, ADR_LINE_6,
        ADR_FULL, 
        CITY_NAME, USPS_STATE_CD, POSTAL_CD, POSTAL_EXT,
        ADD_GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD,
        GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY,
        IDR_INSRT_TS, IDR_UPDT_TS)
    Values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
    """

SqlUpdate = """
    UPDATE CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS
    SET USPS_STATE_CD = 'MT'
    WHERE ADR_LINE_1 = '1313 Mockingbird Lane'
    AND USPS_STATE_CD = 'MD';
"""

SqlStmtGeo = """
    SELECT ADR_LINE_1, ADR_LINE_2, ADR_LINE_3, ADR_LINE_4, ADR_LINE_5, ADR_LINE_6,
        ADR_FULL, 
        CITY_NAME, USPS_STATE_CD, POSTAL_CD, POSTAL_EXT,
        ADD_GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD,
        GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY,
        IDR_INSRT_TS, IDR_UPDT_TS
    FROM CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS
    WHERE ADR_LINE_1 like '% Mockingbird Lane';
"""

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

SqlStmtGeo3 = "SELECT ADR_LINE_1, ADR_LINE_2, ADR_LINE_3, ADR_LINE_4, ADR_LINE_5, ADR_LINE_6, ADR_FULL, CITY_NAME, USPS_STATE_CD, POSTAL_CD, POSTAL_EXT, ADD_GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD, GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY, IDR_INSRT_TS, IDR_UPDT_TS FROM CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS;"

SqlStmtCols = """select databasename 
                    from dbc.columns
                    where tablename ='GEOCODE_ADDRESS'
                    ;
"""

SqlStmtCols2 = """select columnname 
                    from dbc.columns
                    where tablename ='GEOCODE_ADDRESS'
                    and databasename = 'CMS_WORK_COMM_CDEV';

"""

SqlStmt2 = """
        SELECT DISTINCT FieldName 
          FROM Tables3VX 
         WHERE DatabaseName = 'CMS_VDM_VIEW_MDCR_PRD' 
           AND TableName = 'V2_MDCR_CLM' ; 
        """

SqlStmt3 = """
    SELECT 
     BENE.BENE_SK
	,BENE.BENE_SSN_NUM
	,COALESCE(BENE.BENE_CAN_NUM, ' ' ) (CHAR(9)) AS BENE_CAN_NUM
	,COALESCE(BENE.BENE_BIC_CD, ' ') (CHAR(2))   AS BENE_BIC_CD
	,BENE.BENE_LAST_NAME (CHAR (40)) as BENE_LAST_NAME
	,BENE.BENE_1ST_NAME (CHAR (30)) as BENE_1ST_NAME

	,COALESCE(BENE.BENE_MIDL_NAME,' ') (CHAR(15)) as BENE_MIDL_NAME
    FROM CMS_VIEW_BENE_PRD.V1_BENE BENE

	INNER JOIN CMS_LOAD_COMM_PRD.TRICARE_FINDER FINDER
	ON FINDER.SSN = BENE.BENE_SSN_NUM

    """

def getAllRows(): 

    ############################################
    # Get all rows in results-set
    ############################################
    #rows = SQLTeraDataFncts.getAllRows(SqlStmt2, None)
    #rows = SQLTeraDataFncts.getAllRows(SqlStmtGeo, None)
    rows = SQLTeraDataFncts.getAllRows(SqlStmtCols, None)    

    #sColNames = SQLTeraDataFncts.sSelectColNames
    #SQLTeraDataFncts.logger.debug(sColNames)

    # Convert comma-del string into a List
    #lColNames = sColNames.split(",")

    #dfResults = pandas.DataFrame(rows, columns= lColNames)
    #print(dfResults)
    #dfResults.to_csv(csvFile, sep = ",", index=False, line_terminator = "\n")

    # Create delimited file from results-set
    #SQLTeraDataFncts.createCSVFile(csvFile, lColNames, rows, ",")

    ##################################################
    # Print results-set by 1) full row 2) flds in row
    ##################################################
    SQLTeraDataFncts.logger.info("NOF rows in results-set: "+str(len(rows)))
    for row in rows:
        SQLTeraDataFncts.logger.info("full record: "+str(row))
    #    SQLTeraDataFncts.logger.info("print fields in record:")
    #    for fld in row:
    #        SQLTeraDataFncts.logger.info(fld)


def getManyRows():

    ##################################################
    # Get results-set in subsets of records
    ##################################################
    cursor = SQLTeraDataFncts.getManyRowsCursor(SqlStmtGeo2, None)

    ##################################################
    # Print results-set by 1) full row 2) flds in row
    ##################################################
    SQLTeraDataFncts.logger.info("Fetching Many rows-->")
    rows = SQLTeraDataFncts.getManyRowsNext(cursor, 3)
    while len(rows) > 0:
        SQLTeraDataFncts.logger.info("NOF rows in results-set: "+str(len(rows)))
        for row in rows:
            SQLTeraDataFncts.logger.info("full record: "+str(row))
            #SQLTeraDataFncts.logger.info("print fields in record:")
            #for fld in row:
            #    SQLTeraDataFncts.logger.info(fld)
    
        rows = SQLTeraDataFncts.getManyRowsNext(cursor, 3)        


def insertRows(cnx):

    ##################################################
    # Insert row into Geo table.
    # !!!Change InsertRow to take a connection parameter
    ##################################################
    tupParms = ("1313 Mockingbird Lane","","","","","",
    "1313 Mockingbird Lane, Mockingbird Heights, NY 21158", 
    "Mockingbird Heights", "NY", "21158", "1234",  
    0, 0, 0, 0, 0,0, "2022-02-04 10:40:26", None )

    SQLTeraDataFncts.insertRow(cnx, SqlInsert, tupParms)


def updateRows(cnx):

    ##################################################
    # Update Row; pass-in tuple of parms
    ##################################################
    #SQLTeraDataFncts.UpdateRow(cnx, sqlUpdate, tupUpdateParms)
    SQLTeraDataFncts.UpdateRow(cnx, SqlUpdate, None)

def processRowsUsingPandas():

        pass
        ############################################
        # NOTE: Get SQLAlchemy connection for teradata
        # Pandas test using slqalchemy connection
        # 1) I can connect to Engine for Teradata
        # 2) I cannot do pandas.read_sql which uses ODBC connection.
        #    I believe this is failing because I need 64-bit ODBC teradata driver. 
        #    Only have 32-bit. Cannot find and download 64-bit. 
        ############################################
        #eCnx =  SQLTeraDataFncts.getConnectionEngine()
        #if eCnx is None:
        #    SQLTeraDataFncts.logger.error("Could NOT connect to Tera-Data via Engine")
        #else:
        #    SQLTeraDataFncts.logger.info("Connected to Tera-Data! Via Engine! Yea!") 

#        df = pandas.read_sql_query(SqlStmt3, cnx) 
#        df.to_csv(csvFile, sep = ",", index=False, line_terminator = "\n")
#        SQLTeraDataFncts.logger.info(df)

        ####df.to_sql('temp', cnx, "prod", if_exists='append', 
        ####            index=False, index_label=None, chunksize=50, dtype=None, method=None)
        #         ####trans = eCnx.begin() --> need commit with this.
        # Works if include engine or engine's connection
        ##resp = df.to_sql('familymember3',eng,"prod", if_exists='append', 
        ##            index=False, index_label=None, chunksize=50, dtype=None, method=None)
        #trans.commit


#################################################
def main():

    # Insert eye-catcher
    SQLTeraDataFncts.logger.info("\n#########################")

    try:

        ############################################
        # Get connection
        ############################################
        cnx = SQLTeraDataFncts.getConnection()
        if cnx is None:
            SQLTeraDataFncts.logger.error("Could NOT connect to Tera-Data")
        else:
            SQLTeraDataFncts.logger.info("Connected to Tera-Data! Yea!")    

        #SQLTeraDataFncts.closeConnection(cnx)

        ############################################
        # Explore functionality of import file
        ############################################
        getAllRows()

        #getManyRows(None)

        #insertRows(cnx)

        #updateRows(cnx)

        #********** The below function does not work.
        #SQLTeraDataFncts.getExportCSVFile(cnx, csvFastExportFile, SqlStmtGeo)

        #SQLTeraDataFncts.bulkInsertTDReadCSV(cnx, csvFile, SqlInsert)
        #SQLTeraDataFncts.bulkInsertCSVReader(cnx, bulkInsertCSVFile, SqlInsert)
        
        """
        lstParms = [
                    ["1401 Mockingbird Lane","","","","","",
                     "1401 Mockingbird Lane, Mockingbird Heights, NY 21158", 
                     "Mockingbird Heights", "NY", "21158", "1234",  
                     0, 0, 0, 0, 0,0, "2022-02-10 10:40:26", None ],
                    ["1402 Mockingbird Lane","","","","","",
                     "1402 Mockingbird Lane, Mockingbird Heights, NY 21158", 
                     "Mockingbird Heights", "NY", "21158", "1234",  
                     0, 0, 0, 0, 0,0, "2022-02-10 10:40:26", None ],
                    ["1403 Mockingbird Lane","","","","","",
                     "1403 Mockingbird Lane, Mockingbird Heights, NY 21158", 
                     "Mockingbird Heights", "NY", "21158", "1234",  
                     0, 0, 0, 0, 0,0, "2022-02-10 10:40:26", None ]                     
        ]
        SQLTeraDataFncts.bulkInsertTDFastLoad(cnx, SqlInsert, lstParms)
        """

        ##################################################
        # Commit/rollback should be controlled by main pgm
        ##################################################


        ##################################################
        # use python to execute bteq script.
        ##################################################
#excecute the .sql file with bteq via the python package subprocess
#wait until the process is finished and then continue with the script.


    except Exception as ex:
        SQLTeraDataFncts.logger.error("General Exception:", ex) 

    except SQLTeraDataFncts.teradatasql.Error as e:
        SQLTeraDataFncts.logger.error("Database error:", e) 

    finally: 
        SQLTeraDataFncts.logger.info("Processing complete.")


if __name__ == "__main__":
    main()


