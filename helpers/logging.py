import logging # Import Pythons Logging Library


########### Logging Details
# We have 3 Logs
# Access Log = Basically keeps track of time parser was activated and finished.
# Error Log = Keeps track of any issues arising during the running of the parser. 
# Progress Log = Keeps track of the script as it runs. But only exists for each independent session. I.e. its overwritten 

# Log Locations - i.e. where things get saved to
access_log_location = './logs/access.log' 
error_log_location = './logs/error.log' 
progress_log_location = './logs/progress.log' 

## Create Simple Logging Classes that will create & store script as a variable. 

class Log_Details:
    def __init__(
            self,
            script: str,
    ) -> None:
        self.script = script

    def get_detail_info(self):
        return {
            "script": self.script,
        }

##### Access Logger

## Create Access Logger
logger_access  = logging.getLogger("access")
logger_access.setLevel(logging.INFO)

## Setup Access  Log Handler -> In this case a file handler to dump error log to /logs/access.log
access_file_handler =  logging.FileHandler(access_log_location, mode='a')

## Setup Access  Log Formatter -> To ensure error logs have appropriate time stamps
access_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
access_file_handler.setFormatter(access_formatter)
logger_access.addHandler(access_file_handler)

## Create custom access log function that we can import anywhere in the code base
def log_access(e: Exception, message: str, detail: Log_Details):
    log = f"Script: {detail.script} - Message: {message}"
    logger_access.info(log)

##### Error Logger

## Create Error Logger
logger_error  = logging.getLogger("error")
logger_error.setLevel(logging.ERROR)

## Setup Error Log Handler -> In this case a file handler to dump error log to /logs/error.log
error_file_handler =  logging.FileHandler(error_log_location, mode='a')

## Setup Error Log Formatter -> To ensure error logs have appropriate time stamps
error_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
error_file_handler.setFormatter(error_formatter)
logger_error.addHandler(error_file_handler)

## Create custom error log function that we can import anywhere in the code base
def log_error(e: Exception, message: str, detail: Log_Details):
	log = f"Script: {detail.script} - Message: {message} - Details: {e}"
	logger_error.exception(log)

##### Progress Logger

## Create Progress Logger
logger_progress  = logging.getLogger("progress")
logger_progress.setLevel(logging.INFO)

## Setup Progress  Log Handler -> In this case a file handler to dump error log to /logs/progress.log
progress_file_handler =  logging.FileHandler(progress_log_location, mode='w')

## Setup Progress  Log Formatter -> To ensure error logs have appropriate time stamps
progress_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
progress_file_handler.setFormatter(progress_formatter)
logger_progress.addHandler(progress_file_handler)

## Create custom progress log function that we can import anywhere in the code base
def log_progress(e: Exception, message: str, detail: Log_Details):
    log = f"Script: {detail.script} - Message: {message}"
    logger_progress.info(log)


