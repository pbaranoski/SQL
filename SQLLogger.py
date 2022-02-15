import datetime
import logging
import sys
import os


# Set up log file
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

logger.info("Creating Metrics collection")

metrics = []

logger.info("Creating 1st geoCodingEventMetric object")












logger.info("End of program")
