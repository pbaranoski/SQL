import snowflake.connector

SqlStmt2 = """
        SELECT DISTINCT FieldName 
          FROM Tables3VX 
         WHERE DatabaseName = 'CMS_VDM_VIEW_MDCR_PRD' 
           AND TableName = 'V2_MDCR_CLM' ; 
        """

##########################################
# MySQL Connection string values
##########################################
snowflake_user="xxxx"
snowflake_password="xxxxxx" 
snowflake_account="cms-idrnp.privatelink"
snowflake_warehouse="BIA_NP_ETL_WKLD"
snowflake_database="IDRC_DEV"
#snowflake_database="SNOWFLAKE_SAMPLE_DATA"
snowflake_schema="CMS_DIM_GEO_DEV"
#snowflake_schema="INFORMATION_SCHEMA"
snowflake_host="https://cms-impl.okta.com/"
#snowflake_host="https://impl.idp.idm.cms.gov/"


###############################
# Extract configuration values
###############################
try: 

    ###################################################
    # Connect to MySQL
    ###################################################   
    # ConnectionString="account={YOUR_ACCOUNT_ID};host={YOUR_HOST};user={YOUR_USERNAME};password={YOUR_PASSWORD};db=SNOWFLAKE_SAMPLE_DATA;"
    #   
    cnx = snowflake.connector.Connect(user=snowflake_user, password=snowflake_password, 
        host=snowflake_host,
        account=snowflake_account,
        authenticator='externalbrowser',
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
        
    )


    print("Connected to Snowflake!")

except Exception as ex:
    print("Could not connect to Snowflake!")
    print(ex)



