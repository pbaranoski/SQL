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
csvFile = os.path.join(csv_dir,"teradata_pandas.csv")

### Remove this from function    
sqlStmt = """INSERT into familymember
        (FamilyMembername, FatherID, MotherID, GenderIND, DOB, DOBEndRange, DOBFormat,DOBQualifier,
        DOD, DODEndRange, DODFormat, DODQualifier,
        Birthplace, DeathPlace, InternmentID, FactsID, HospitalID,
        FamilyMemberAKAName, MultipleMarriagesIND, DefaultSpouseInd)
        Values(%s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s )
"""

SqlInsert = """
    INSERT INTO CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS (
        ADR_LINE_1, ADR_LINE_2, ADR_LINE_3, ADR_LINE_4, ADR_LINE_5, ADR_LINE_6, 
        ADR_FULL,                                                 
        CITY_NAME, USPS_STATE_CD, POSTAL_CD, POSTAL_EXT,                                                
        ADD_GEO_SK, GEO_ADR_GIS_MATCH_SCRE_NUM, GEO_ADR_GIS_MATCH_ADR, GEO_ADR_GIS_ADR_RULE_CD,                                     
        GEO_ADR_GIS_LON_QTY, GEO_ADR_GIS_LAT_QTY                                         
        IDR_INSRT_TS, IDR_UPDT_TS  )
    Values(%s, %s, %s, %s, %s, %s, 
           %s, 
           %s, %s, %s, %s, 
           %s, %s, %s, %s, 
           %s, %s, 
           %s, %s)
"""

SqlStmtTemp = """
    SELECT *
    FROM CMS_WORK_COMM_CDEV.GEOCODE_ADRRESS;
"""

SqlStmtTemp2 = """select columnname 
                    from dbc.columns
                    where tablename ='GEOCODE_ADRRESS'
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

SqlStmt = """SELECT familyMemberName, date_format(DOB,'%Y-%m-%d'), truncate((datediff(curdate(), DOB) / 365),0) as age
                from familymember where familymemberID < 10"""

SqlStmtAll = """SELECT * from familymember where familymemberID < 10"""

sqlUpdateStmt = """Update familymember
                      set FamilyMemberName = 'Mickey Mouse III'
                    where familyMemberID = %s;
"""
sqlUpdateStmt2 = """Update familymember
                      set FamilyMemberName = 'Mickey Mouse III'
                    where familyMemberID = 2760;
"""

iFamilyMemberID = str(2760)

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

        ##################################################
        # Update Row; pass-in tuple of parms
        ##################################################
        #sqlUpdateParmList = [iFamilyMemberID]
        #tupUpdateParms = (iFamilyMemberID,)
        #SQLTeraDataFncts.UpdateRow(cnx, sqlUpdateStmt, tupUpdateParms)
        #SQLTeraDataFncts.UpdateRow(cnx, sqlUpdateStmt2, None)

        ############################################
        # Get all rows in results-set
        ############################################
        #rows = SQLTeraDataFncts.getAllRows(SqlStmt3, None)
        #rows = SQLTeraDataFncts.getAllRows(SqlStmtTemp2, None)
        
        ##################################################
        # Print results-set by 1) full row 2) flds in row
        ##################################################
        #SQLTeraDataFncts.logger.info("NOF rows in results-set: "+str(len(rows)))
        #for row in rows:
        #    SQLTeraDataFncts.logger.info("full record: "+str(row))
        #    SQLTeraDataFncts.logger.info("print fields in record:")
        #    for fld in row:
        #        SQLTeraDataFncts.logger.info(fld)

        ##################################################
        # Get results-set in subsets of records
        ##################################################
        #cursor = SQLTeraDataFncts.getManyRowsCursor(SqlStmt)

        ##################################################
        # Print results-set by 1) full row 2) flds in row
        ##################################################
        #SQLTeraDataFncts.logger.info("Fetching Many rows-->")
        #rows = SQLTeraDataFncts.getManyRowsNext(cursor, 3)
        #while len(rows) > 0:
        #    SQLTeraDataFncts.logger.info("NOF rows in results-set: "+str(len(rows)))
        #    for row in rows:
        #        SQLTeraDataFncts.logger.info("full record: "+str(row))
        #        SQLTeraDataFncts.logger.info("print fields in record:")
        #        for fld in row:
        #            SQLTeraDataFncts.logger.info(fld)
        #
        #    rows = SQLTeraDataFncts.getManyRowsNext(cursor, 3)        

        ##################################################
        # Insert row into Geo table.
        # !!!Change InsertRow to take a connection parameter
        ##################################################
        tupParms = ("1313 Mockingbird Lane", "", "", "", "", "", 
                "1313 Mockingbird Lane, Mockingbird Heights, NY 21158",
                "Mockingbird Heights", "NY", "21158","1234", "",
                None, None, None, None,
                None, None,  
                "Current_Timestamp", None )
 
        SQLTeraDataFncts.insertRow(cnx, SqlInsert, tupParms)
                                             


        ##################################################
        # Commit/rollback should be controlled by main pgm
        ##################################################


        ##################################################
        # use python to execute bteq script.
        ##################################################
#save the pd.DataFrame as a .csv
#write a .sql file with the .logon, .import vartext from file, sql insert query and .logoff and save it locally
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


