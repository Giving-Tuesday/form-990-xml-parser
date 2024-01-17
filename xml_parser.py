'''
    XML Parser Script 
    
    Goal: 
    (1) Download all XML Filings hosted by Giving Tuesday
    (2) Parse each XML file and process the xml data according to a mapping of variables and paths
    (3) Store resulting data into Mongo DB (a non relational document store)

    Parser commands that can be passed from command line/terminal: 
    -i {Year}       Inserting command with year  
    -f              When processing removes and insert forms versus just inserting    
    -l {Number}     Number of forms that will be inserted simultaneously 1000
    -c {Number}     Location from an index where you want to continue inserting/processing 
    -u              Update forms      
    --mongodb       Mongo 
    --qa            Specifies the environment QA            - Local Test Environment
    --prod          Specifies the environment PRODUCTION    - AWS Production Environment
    
    Command line often run as follows: nohup python3 ./xml_parser.py -i 2018 --prod --mongodb
    In this example: we are using python 3 to run the parser with -i insert flag for 2018 into 
    
    Note: We can pass more than 1 year via console. 

'''

import sys  # python library allows us to use sys values like from terminal
import re  # allows us to use regular expressions
import logging  # python library that allows us to log things
import os  # allows us to use operating system functions
from datetime import datetime  # allows us to figure out what current date is etc
# allows us to parellelize the running of our code
from multiprocessing import Process, Pool
# Allows us to update an index i.e. process an index for latest filings
from helpers.index_downloader import fetch_filings_updated
# Given a year Downloads an index from aws and creates a list of filings
from helpers.index_downloader import fetch_filings_from_index_file
# Parser is what we use to parse xml
from helpers.parser.formparser import FormParser
# Allows us to read the mapping (list of variables) csv file
from helpers.helpers import csv_to_object
# Allows us to read the mapping (list of variables) for table values.
from helpers.helpers import csv_table_to_object
# helper function takes a list of dictionaries and extracts certain values and outputs as a list of lists
from helpers.helpers import partition_list
# helper function for comparing new xpath with the mapping
from helpers.helpers import check_new_xpaths
# Allow us to access Mongodb
from pymongo import MongoClient 

# Import all variables that are hardcoded
from settings.Settings import mongo_qa_details, mongo_production_details

# Sets base directory to where this script is located
BASE_DIR = os.path.abspath(__file__)

# store all arguments passed in from shell into a list starting with
ARGS = sys.argv[1:]


def init(year):
    '''
    This is our main function which takes a year from console along with other consle arguments
    It then processes filings for that year and inserts them into mongodb. 

    '''

    if '-t' in ARGS: 
        print ('Testing Connections')

        try: 
            print ('Testing Mongo QA/Local Connection')
            mongodb_client = MongoClient(mongo_qa_details)
            print (mongodb_client.server_info())
        except Exception as e:
            print ('Mongo QA/Local Failed', e)

        # try: 
        #     print ('Testing Mongo Production Connection')
        #     mongodb_client = MongoClient(mongo_production_details,tls=True,tlsAllowInvalidCertificates=True,connect=False)
        #     print (mongodb_client.server_info())
        # except Exception as e:
        #     print ('Mongo Production Failed',e)

        sys.exit("Finished Testing Connections")

    else: 

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
        if '-u' in ARGS:  # this triggers updating of forms

            # Step 3a1. Create Form Parser object and pass CSV Object & Table Object
            form_parser = FormParser(CSV_OBJECT, CSV_TABLE_OBJECT)

            # Step 3a2. Grab latest version of index by using fetch_filings method from index_downloader.py script
            filings_updated = fetch_filings_updated(year)
            # Uncomment line below (and comment line above) to run test with simple filing
            # filings_updated = [{u'OrganizationName': u'JAWONIO RESIDENTIAL OPPORTUNITIES III INC', u'ObjectId': u'201803129349301355', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803129349301355_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93493312013558', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'201078564'}]#, {u'OrganizationName': u'ROAD RUNNERS CLUB OF AMERICA 1174 PACE SETTERS RUNNING CLUB INC', u'ObjectId': u'201803269349300500', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803269349300500_public.xml', u'SubmittedOn': u'2018-12-19', u'DLN': u'93493326005008', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'391455942'}, {u'OrganizationName': u'UNITED HOMES FUND INC CO FLUSHING HOUSE', u'ObjectId': u'201803129349201105', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803129349201105_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93492312011058', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990EZ', u'EIN': u'112808943'}, {u'OrganizationName': u'HOUGHTON VOLUNTEER AMBULANCE SERVICE INC', u'ObjectId': u'201803119349201075', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803119349201075_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93492311010758', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990EZ', u'EIN': u'262980099'}, {u'OrganizationName': u'VALLEY MEMORIAL FOUNDATION', u'ObjectId': u'201803119349301280', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803119349301280_public.xml', u'SubmittedOn': u'2018-11-30', u'DLN': u'93493311012808', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201806', u'FormType': u'990', u'EIN': u'450392710'}, {u'OrganizationName': u'PLUMBERS AND STEAMFITTERS PROTECTIVE ASSOCIATION INC', u'ObjectId': u'201803119349302560', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803119349302560_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93493311025608', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'526038675'}]
            # Gets a yearly index 'say 2018 index'
            # (1) Removes  current index called 2018
            # (2) Downloads latest version of index 2018
            #    (2a) downloads latest index for a year
            #    (2b) if path doesnt exist creates it and creates file
            # (3) Call function: fetch_filings_from_index_file(year
            #    (3a) Downloads latest version of index 2018
            #    (3b) Creates a list of dictionaries of all filings for that index
            # (4) Stores Yesterday's Date as difference between today and 1 day
            # (5) Processes list of filings and searches for any new ones and creates a list of dictionaries

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
            # ! Notice We probably should have some updating that this document has been updated.

        # Step 3b. Check to see if -i in argument list as this means we are clean inserting into mongo
            # Sample Commmands that could be passed
                # -i 2018 -l 100 --mongodb --prod
                # -i 2018 --prod --mongodb
        if '-i' in ARGS:

            # Step 3b1. check to see how many objects we will try to insert simultaneously
            limit = int((re.search("'-l', '([0-9]+)'",
                                   str(sys.argv)) or re.search("(1000)", "1000"))
                        .group(1))

        if '-c' in ARGS:
            # Step 3b2. Check for the specific number from an index where we may want to continue
            # default will be to start at beginning i.e. 0
            continue_progress = int((re.search("'-c', '([0-9]+)'",str(sys.argv)) or re.search("(0)", "0")).group(1)) - 2 
            print ('initial step %s' % continue_progress)

            stop_progress = int((re.search("'-s', '([0-9]+)'",str(sys.argv)) or (len(fetch_filings_from_index_file(year)) - 1)))
            #print ('number of files to process  %s' % stop_progress)
            end_process = continue_progress + stop_progress

            # print ('end process %s' %end_process)

            # Step 3b3. Build a list of filings fetch all filings from index and slice using continue_progress
            filings = fetch_filings_from_index_file(year)[continue_progress:end_process]
    
        else:
            continue_progress = 0

            filings = fetch_filings_from_index_file(year)

            # Step 3b4. Creates a log for year we are currently processing
            logging.basicConfig(
                filename=str.format('log-{0}.log', year),
                format='%(levelname)s: TIME: %(asctime)s MESSAGE: %(message)s',
                level=logging.INFO
            )

            # Step 3b5. For each filing in the index, download, process index and store filing in mongodb 
            for index, xml_list in enumerate(partition_list(filings, limit)):

                # Step 3b5a for each url link in list do following 2 steps
                counter = 0
                for xml_link in xml_list:

                    # Step 3b5a1 Create Form Parser object and pass CSV Object & Table Object
                    form_parser = FormParser(CSV_OBJECT, CSV_TABLE_OBJECT)

                    # Step 3b5a2 Create Form by:
                    # 1. Download Document
                    # 2. Process document saves it as a form object/class
                    form = form_parser.create(xml_link)

                    # Step 3b5a3 If the form is None continue processing
                    if form is None:
                        continue

                    # Step 3b5a4 if --Mongodb has been passed from consol then store to mongo
                    if '--mongodb' in ARGS:

                        # Step 3b5a4a if -f is in arguments then use the insert data by force into mongo
                        if '-f' in ARGS:

                            print ("FORCING")

                            # Finds the form, if it exists removes it and reinserts it
                            form.insert_data_force_to_mongo()

                        # Step 3b5a4b Else insert the data normally into mongo.
                        else:
                            print ('Inserting document %s into Mongo' % xml_link)
                            form.insert_data_to_mongo()
                    counter = counter + 1
                    # 3b6b.For every document that we insert log
                    progress = str.format(
                        "Completed {0} / {1}", ((limit * index) + continue_progress) + counter,
                        len(filings) + continue_progress
                    )
                    # Index tells us what position we are in the index multiply it by index and adding continue_progress tells us how many documents inserted
                    # Numerator = if index was 10k documents limit was 1 then it would be 10k*1 + whatever position we start from
                    # Denominator = Nnumber of filings + where we started from
                    logging.info(progress)  
                    print (progress)



# Step 1. If your program module is in made then it will execute the following. If script is called form outside of main then the lines below wont execute
if __name__ == '__main__':

    if '-t' in ARGS: 
        print ('Testing Connections')

        try: 
            print ('Testing Mongo QA/Local Connection')
            mongodb_client = MongoClient(mongo_qa_details)
            print (mongodb_client.server_info())
        except Exception as e:
            print ('Mongo Failed', e)

        # try: 
        #     print ('Testing Mongo Production Connection')
        #     mongodb_client = MongoClient(mongo_production_details,tls=True,tlsAllowInvalidCertificates=True,connect=False)
        #     print (mongodb_client.server_info())
        # except Exception as e:
        #     print ('Mongo Production Failed',e)

        sys.exit("Finished Testing Connections")

    else:
            
        # Step 2a. Check to see if -i has been passed (i.e. inserting) as argument from console
        if '-i' in ARGS:

            # Step 3a. Check to see how many years we are trying to insert for example
            YEARS = sorted(list(set(re.search(
                "'-i', '(([0-9]{4})|([0-9]{4}-[0-9]{4}))'",
                str(sys.argv)).group(1).split('-'))))

            # Step 3b.  If more than 1 year has been requested to be inserted
            if len(YEARS) == 2:  # maybe this should be >= 2 vs 2 not sure.
                # Step 3b1. Instantiate a pool -> you can learn more about this ->https://www.ellicium.com/python-multiprocessing-pool-process/ and here https://sebastianraschka.com/Articles/2014_multiprocessing.html
                POOL = Pool()

                # Step 3b2.
                # we are essentially passing the function init into Pool so
                # it can run in separate cores and then we are saying
                # if years = [2018,2017] pass 2017, and 2018 into init one for each pool
                POOL.map(init, range(int(YEARS[0]), int(YEARS[1]) + 1))

            # Step 3c. If only 1 year has been passed.
            elif len(YEARS) == 1:
                # Then we just need to intiate the init (i.e. main function) and pass the argument of years
                # I.e. w will perform this serially

                # Step 3c1 # Setup a list of processes that we want to run
                PROCESS = Process(target=init, args=(int(YEARS[0]),))
                # Step 3c2 # Run processes
                PROCESS.start()
                # Step 3c3 # Exit the completed processes
                PROCESS.join()

        # Step 2b. If -u is passed i.e. updating vs inserting
        elif '-u' in ARGS:

            # Step 3a Set years to Years -> the range starting in 2011, until current year plus 1 because range function will stop 1 before
            #!  Not sure why we are using 2011 and not 2009 could be that we initially only have updated data for 2011 and beyond
            YEARS = range(2000, datetime.now().year + 1)

            # Step 3b Initiate Pool
            POOL = Pool()

            # Step 3c Pass init function and use core for each year
            POOL.map(init, YEARS)
