'''
    XML Parser Script 
    
    Goal: 
    (1) Download Index hosted on the Giving Tuesday DataLake 
    (2) Parse each XML file and process the xml data according to a mapping of variables and paths
    (3) Store resulting data into Mongo DB (a non relational document store)

    Parser commands that can be passed from command line/terminal: 
    -i {Index Name} Insert command -  with index_name as latest_only_year_month_day or all_years_year_month_day 
    -f              Force  command - used when inserting which removes and insert forms versus just inserting    
    -l {Number}     Limit  command - Number of forms that will be inserted simultaneously default 1000
    -c {Number}     Continue command - Location from an index where you want to continue/begin inserting/processing 
    -u              Update command - Re downloads a specific index incase things have changed    
    --mongodb       Mongo 
    --qa            Specifies the environment QA            - Local Test Environment
    --prod          Specifies the environment PRODUCTION    - AWS Production Environment 
    
    Example: 
        
        nohup python3 ./xml_parser.py -i latest_only_2018-12-31 --prod --mongodb
    
    In the above example: 
        We use nohup to create an ouput file of anything that is printed to console. You dont need to use nohup. 
        You can also just run the script or even use linux screens. 

        Anyways, continuing along ....
        We are using python 3 to run the parser with 
            -i insert flag for index named: latest_only_2018-12-31 
            -into production mongodb  
    
    Final Note: You can pass more than 1 index_name via console. 
'''

import sys  # python library allows us to use sys values like from terminal
import re  # allows us to use regular expressions
import os  # allows us to use operating system functions
from datetime import datetime  # allows us to figure out what current date is etc
# allows us to parellelize the running of our code
from multiprocessing import Process, Pool
# Allows us to update an index i.e. process an index for latest filings
from helpers.index_downloader import fetch_filings_updated
# Given a index_name Downloads an index from aws and creates a list of filings
from helpers.index_downloader import fetch_filings_from_index_file
# Parser is what we use to parse xml
from helpers.parser.formparser import FormParser
# Allows us to read the mapping (list of variables) csv file
from helpers.helpers import csv_to_object
# Allows us to read the mapping (list of variables) for table values.
from helpers.helpers import csv_table_to_object
# helper function takes a list of dictionaries and extracts certain values and outputs as a list of lists
from helpers.helpers import partition_list
# Allow us to access Mongodb
from pymongo import MongoClient 
# Import all variables that are hardcoded
from settings.Settings import mongo_qa_details, mongo_production_details
# Import Custom Logging
from helpers.loggingutil import Log_Details, log_access, log_error, log_progress

# Store name of current script in Log_Details class object as script name. We do this so that error log will always tell us which script error comes from. 
Log_Details.script = os.path.split(sys.argv[0])[1]

# Sets base directory to where this script is located
BASE_DIR = os.path.abspath(__file__)

# store all arguments passed in from shell into a list starting with
ARGS = sys.argv[1:]

# Turn all args into string
initial_args = ' '. join([str(arg) for arg in sys.argv[1:]])

def init(index_name):
    '''
    This is our main function which takes a index_name from console along with other consle arguments
    It then processes filings for that index_name and inserts them into mongodb. 

    '''

    log_access('', 'Started Running The XML Parser with following arguments & flags: %s' % initial_args, Log_Details)

    # Step 1 - Check to see if -t in arguments that triggers a testing of database connections
    if '-t' in ARGS:  
        print ('Testing Connections')

        # Step 2. Test Connection to QA/Local Mongo database
        try: 
            print ('Testing Mongo QA/Local Connection')
            mongodb_client = MongoClient(mongo_qa_details)
            print (mongodb_client.server_info())

        except Exception as g:
            print ('Connection To Mongo QA/Local Failed', g)
            log_error(g, "Connection To Mongo QA/Local Failed", Log_Details) 

        # Step 3. Test Connection to Production Mongo Database
        try: 
            print ('Testing Mongo Production Connection')
            mongodb_client = MongoClient(mongo_production_details,tls=True,tlsAllowInvalidCertificates=True,connect=False)
            print (mongodb_client.server_info())
        except Exception as g:
            print ('Connection To Mongo Production Failed',g)
            log_error(g, 'Connection To Mongo in Production Failed', Log_Details) 

        # Step 4. Terminate Script as we are only running tests
        sys.exit("Finished Testing Connections")

    else: # This means we are actually running script vs testing anything

        # Step 1. Read Mapping.csv and create object
        CSV_OBJECT = csv_to_object()
        '''Read the mapping.csv from disk and convert it into 
        an object. Output is a dictionary of Paths/Variables'''

        # Step 2. Read Table mapping & Create CSV table object
        CSV_TABLE_OBJECT = csv_table_to_object()
        '''Read the mapping_table.csv from disk and convert it to 
        bject. Mapping_table is really a list of all tables in 
        forms and schedules. Output is a dictionary of Paths/Variables'''

        # Step 3a. Check to see if -u is in arguments as that triggers updating of document vs insertion
        if '-u' in ARGS:  

            '''
            Main Behavior

            # Gets an index 'i.e 2018-12-31.json'
            # (1) Removes current index called 2018-12-31.json
            # (2) Downloads latest version of index 2018-12-31.json
            #    (2a) downloads latest index for a index_name
            #    (2b) if path doesnt exist creates it and creates file
            # (3) Call function: fetch_filings_from_index_file(index_name)
            #    (3a) Downloads latest version of index 2018
            #    (3b) Creates a list of dictionaries of all filings for that index
            # (4) Stores Yesterday's Date as difference between today and 1 day
            # (5) Processes list of filings and searches for any new ones and creates a list of dictionaries
            '''

            # Step 3a1. Create Form Parser object and pass CSV Object & Table Object
            form_parser = FormParser(CSV_OBJECT, CSV_TABLE_OBJECT)

            # Step 3a2. Grab latest version of index by using fetch_filings method from index_downloader.py script
            filings_updated = fetch_filings_updated(index_name)
            # Uncomment line below (and comment line above) to run test with simple filing
            # filings_updated = [{u'OrganizationName': u'JAWONIO RESIDENTIAL OPPORTUNITIES III INC', u'ObjectId': u'201803129349301355', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803129349301355_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93493312013558', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'201078564'}]#, {u'OrganizationName': u'ROAD RUNNERS CLUB OF AMERICA 1174 PACE SETTERS RUNNING CLUB INC', u'ObjectId': u'201803269349300500', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803269349300500_public.xml', u'SubmittedOn': u'2018-12-19', u'DLN': u'93493326005008', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'391455942'}, {u'OrganizationName': u'UNITED HOMES FUND INC CO FLUSHING HOUSE', u'ObjectId': u'201803129349201105', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803129349201105_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93492312011058', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990EZ', u'EIN': u'112808943'}, {u'OrganizationName': u'HOUGHTON VOLUNTEER AMBULANCE SERVICE INC', u'ObjectId': u'201803119349201075', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803119349201075_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93492311010758', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990EZ', u'EIN': u'262980099'}, {u'OrganizationName': u'VALLEY MEMORIAL FOUNDATION', u'ObjectId': u'201803119349301280', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803119349301280_public.xml', u'SubmittedOn': u'2018-11-30', u'DLN': u'93493311012808', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201806', u'FormType': u'990', u'EIN': u'450392710'}, {u'OrganizationName': u'PLUMBERS AND STEAMFITTERS PROTECTIVE ASSOCIATION INC', u'ObjectId': u'201803119349302560', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803119349302560_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93493311025608', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'526038675'}]

            # Step 3a3. For each filing in the index, download, process index and store filing in mongo
            for index, xml_list in enumerate(partition_list(filings_updated, 1)):
                # creates a list of urls to download filings from filings_update (list of dictionaries)
                for xml_link in xml_list:
                    # for each url link in list do following 2 steps
                    # 1. Download Document
                    # 2. Process document saves it as a form object/class
                    form = form_parser.create(xml_link)
                    # 3. Saves form to mongo using update data method in mongo inteface found in interface2.py
                    form.update_data_mongo()
                    # 3a. Inserts data into Schedules
                    # 3b. Inserts data into main forms


        # Step 3b. Check to see if -i in argument list as this means we are clean inserting (ie. for first tiem) into mongo
        if '-i' in ARGS:

            # Step 3b1. check to see how many objects we will try to insert simultaneously
            limit = int((re.search("'-l', '([0-9]+)'",
                                   str(sys.argv)) or re.search("(1000)", "1000"))
                        .group(1))

        if '-c' in ARGS: # if -c is passed as an argument  is passed it means we are trying to start from specific location in index
            
            # Step 3b2. Check for the specific number from an index where we may want to continue
            # default will be to start at beginning i.e. 0
            continue_progress = int((re.search("'-c', '([0-9]+)'",str(sys.argv)) or re.search("(0)", "0")).group(1)) - 2 
            stop_progress = int((re.search("'-s', '([0-9]+)'",str(sys.argv)) or (len(fetch_filings_from_index_file(index_name)) - 1)))
            end_process = continue_progress + stop_progress

            # Step 3b3. Build a list of filings fetch all filings from index and slice using continue_progress
            filings = fetch_filings_from_index_file(index_name)[continue_progress:end_process]
    
        else:

            # Step 3b4. Initialize Progress Counter
            continue_progress = 0 

            # Step 3b5 # Fetch filings from index file 
            filings = fetch_filings_from_index_file(index_name)

            # Step 3b6. Creates a log for index_name we are currently processing this is deprecated as we use our own logging.py 
            # logging.basicConfig(
            #     filename=str.format('log-{0}.log', index_name),
            #     format='%(levelname)s: TIME: %(asctime)s MESSAGE: %(message)s',
            #     level=logging.INFO
            # )

            # Step 3b7. For each filing in the index, download, process index and store filing in mongodb 
            for index, xml_list in enumerate(partition_list(filings, limit)):

                # Step 3b7a1
                counter = 0  # set counter at 0 

                # Step 3b7a2 for each url link in list do following 2 steps
                for xml_link in xml_list:

                    # Step 3b5a1 Create Form Parser object and pass CSV Object & Table Object
                    form_parser = FormParser(CSV_OBJECT, CSV_TABLE_OBJECT)

                    # Step 3b7a2 Create Form by:
                    # 1. Download Document
                    # 2. Process document saves it as a form object/class
                    form = form_parser.create(xml_link)

                    # Step 3b7a3 If the form is None continue processing
                    if form is None:
                        continue

                    # Step 3b7a4 if --Mongodb has been passed from consol then store to mongo
                    if '--mongodb' in ARGS:

                        # Step 3b7a4a1 if -f is in arguments then use the insert data by force into mongo
                        if '-f' in ARGS:

                            # Step 3b7a4a2
                            # Finds the form, if it exists removes it and reinserts it
                            form.insert_data_force_to_mongo()

                        # Step 3b7a4b Else insert the data normally into mongo.
                        else:
                            #print ('Inserting document %s into Mongo' % xml_link)
                            form.insert_data_to_mongo()
                    
                    # 3b7b Increase counter
                    counter = counter + 1
                    
                    # 3b7c. Create a string of our progress
                        # Index tells us what position we are in the index multiply it by index and adding continue_progress tells us how many documents inserted
                        # Numerator = if index was 10k documents limit was 1 then it would be 10k*1 + whatever position we start from
                        # Denominator = Nnumber of filings + where we started from
                    progress = str.format(
                        "Completed {0} / {1}", ((limit * index) + continue_progress) + counter,
                        len(filings) + continue_progress
                    )

                    # 3b7d. Log Progress we keep this commented as we will already have details at document level (elsewhere in code)
                    #log_progress('', 'Parser Progress: %s Finished inserting: %s' % (progress, xml_link), Log_Details) # original logging -> #logging.info(progress) 

                    # 3b7e. Log our progress to the console
                    print (progress)

    log_access('', 'Finished Running XML Parser', Log_Details)

# Step 1. If your program module is in main (folder) then it will execute the following. If script is called form outside of main then the lines below wont execute
if __name__ == '__main__':

    # Step 1a - Check to see if -t in arguments that triggers a testing of database connections
    if '-t' in ARGS:  
        print ('Testing Connections')
        log_access('', 'Started Running The XML Parser with following arguments & flags: %s' % initial_args, Log_Details)

        # Step 1b. Test Connection to QA/Local Mongo database
        try: 
            print ('Testing Mongo QA/Local Connection')
            mongodb_client = MongoClient(mongo_qa_details)
            print (mongodb_client.server_info())
        except Exception as g:
            print ('Connection To Mongo QA/Local Failed', g)
            log_error(g, "Connection To Mongo QA/Local Failed", Log_Details) 

        # Step 1c. Test Connection to Production Mongo Database
        try: 
            print ('Testing Mongo Production Connection')
            mongodb_client = MongoClient(mongo_production_details,tls=True,tlsAllowInvalidCertificates=True,connect=False)
            print (mongodb_client.server_info())
        except Exception as g:
            print ('Connection To Mongo Production Failed',g)
            log_error(g, 'Connection To Mongo Production Failed', Log_Details) 

        # Step 1d. Terminate Script as we are only running tests
        sys.exit("Finished Testing Connections")

    else: # Step 1b check other args passed via console
            
        # Step 2a. Check to see if -i has been passed (i.e. inserting) as argument from console
        if '-i' in ARGS: 

            #### IMPORTANT we are assuming the following: 
                # We are assuming indices are those found on Giving Tuesday's Data Lake 
                # That indices are .json
                # That and that there are two variants either all_years or latest_only followed with their creation dates

            # Step 2a1. Check to see which index we are grabbing from data lake either "all_years + date or latest_only + date"
            indices = [re.search(
                "(?:'-i', ').(latest_only_[0-9]{4}-[0-9]{2}-[0-9]{2})'|(all_years_[0-9]{4}-[0-9]{2}-[0-9]{2})|(latest_only_[0-9]{4}-[0-9]{2}-[0-9]{2})", 
                str(sys.argv)).group(0)]

            # Step 2a2a.  If more than 1 index name has been requested to be inserted
            if len(indices) >= 2:  

                # Step 2a2a1. Instantiate a pool to process indices in parallel you can learn more about multi processing :     
                    # -> https://www.ellicium.com/python-multiprocessing-pool-process/ 
                    # -> https://sebastianraschka.com/Articles/2014_multiprocessing.html           
                POOL = Pool()

                # Step 2a2a2.
                # we are essentially passing the function init into Pool so
                # it can run in separate cores and then we are saying
                # if indexes are = [latest_only_2018-12-31.json,a ll_years_2017-12-31.json] pass 2017, and 2018 into init one for each pool i.e. run in parallel
                POOL.map(init, indices) 

            # Step 2a2b. If only 1 index name has been passed then we wont run it in parallel because the document insertion already happens as parallel batches.
            elif len(indices) == 1:
                # Then we just need to intiate the init function (i.e. main function) and pass the argument of index_name

                # Step 2a2b1 # Setup a list of processes that we want to run
                PROCESS = Process(target=init, args=(indices[0],))
                # Step 2a2b2 # Run processes
                PROCESS.start()
                # Step 2a2b3 # Exit the completed processes
                PROCESS.join()

        # Step 2b. If -u is passed i.e. updating vs inserting
        elif '-u' in ARGS:

            # Step 2b1 Check to see which index we are grabbing from data lake either "all_years + date or latest_only + date"
            indices = [re.search(
                "(?:'-i', ').(latest_only_[0-9]{4}-[0-9]{2}-[0-9]{2})'|(all_years_[0-9]{4}-[0-9]{2}-[0-9]{2})|(latest_only_[0-9]{4}-[0-9]{2}-[0-9]{2})", 
                str(sys.argv)).group(0)]

            # Step 2b2 Instantiate a pool to process indices in parallel you can learn more about multi processing :     
                    # -> https://www.ellicium.com/python-multiprocessing-pool-process/ 
                    # -> https://sebastianraschka.com/Articles/2014_multiprocessing.html            
            POOL = Pool()

            # Step 2b3 Pass init function and use core for each index
                # we are essentially passing the function init into Pool so
                # it can run in separate cores and then we are saying
                # if indexes are = [latest_only_2018-12-31.json,a ll_years_2017-12-31.json] pass 2017, and 2018 into init one for each pool i.e. run in parallel
            POOL.map(init, indices)
