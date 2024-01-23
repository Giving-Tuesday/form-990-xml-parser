import os,sys # allows us to use operating system functions
import json # allows us to parse and store json
#import urllib2 # allows us to handle urls - requests etc
from urllib.request import urlopen
from datetime import datetime, timedelta # allows us to figure out what date we are on and calculate a difference in dates
import requests
from settings.Settings import indices_directory_name, gt_datalake_index_location
from .loggingutil import Log_Details, log_access, log_error, log_progress

Log_Details.script = os.path.split(sys.argv[0])[1] # Store name of current script in Log_Details class object as script name. We do this so that error log will always tell us which script error comes from. 
ROOT_DIR = os.path.join(os.path.dirname(__file__)) # Sets root to Helpers Directory
INDEXES_DIR = os.path.join(ROOT_DIR, indices_directory_name) # adds "indices" to the helpers directory path

# Overview: Variety of methods for downloading and processing index data from Giving Tuesday Datalake on AWS


def download_index(index_name):

    '''

    This method will download a json index containing links to filings hosted on the giving tuesday aws data lake. 
    This method takes a index_name i.e. latest_only_2018-12-31.json and download it from
    https://gt990datalake-rawdata.s3.amazonaws.com/Indices/990xmls/index_latest_only_efiledata_xmls_created_on_2018-12-31.json
   
    '''

    x,y,url_date =(index_name.split("_"))
    url_type = x+'_'+y

    # Step 1. Set url index based on index_name. 
    url_index = gt_datalake_index_location
    full_url = str.format(url_index, url_type, url_date)
    
    # Step 2. Check to see if indexpath exists
    if not os.path.exists(INDEXES_DIR):
        # Step 2a. If index path doesnt exist create it
        os.makedirs(INDEXES_DIR)
    
    # Step 3. new file path will be index directory/index_name.json
    new_file_path = os.path.join( INDEXES_DIR,str.format('{0}.json', index_name))
    
    # Step 4. Check to see if file exists
    if not os.path.isfile(new_file_path):

        # Step 4a. If file path doesnt exist print to console that you are downloading the index from amazon. 
        print (str.format('Downloading & saving index {0} from amazon.', index_name))
        log_progress('',str.format('Downloading & saving index {0} from amazon.', index_name),Log_Details)
        try:

            #Step 4b. Store the downloaded index as "response"  
            response = requests.get(full_url, stream=True,verify=False)

            # Step 4c. Create a new file with downloaded index. 
            with open(new_file_path, mode="wb") as file:
                for chunk in response.iter_content(chunk_size=10 * 1024):
                    file.write(chunk)
                # Close file that was just created/downloaded.
                file.close()

            # Step 4d Set file to 0 to avoid memory issues. 
            response = None

        except Exception as g:
            print(str.format( "Failed To Download Index: {0} Giving Tuesday AWS: {1} Error was: {2}", index_name, full_url, g ))
            log_error(g,str.format( "Failed To Download Index: {0} Giving Tuesday AWS: {1}", index_name, full_url),Log_Details)

    else:
        print (str.format('Index: {0} Already Exists Locally No Need to Download', index_name))
        log_progress('',str.format('Index: {0} Already Exists Locally No Need to Download', index_name),Log_Details)


def remove_index(index_name):

    '''

    This method removes an index from the local hard drive/storage. 
    This method takes a index_name as the indexes are store as such 2019-12-31.json

    '''

    # Step 1. Create a path given base index directory & index_name -> indexes/2018.json
    path = os.path.join(INDEXES_DIR, str(index_name)+'.json')

    # Step 2. Check if the path exists. If patth exists means file exists
    if os.path.exists(path):
        
        # Step 3. Remove the path -> i.e. remove the file. 
        try:
            os.remove(path)
            log_progress('',str.format( "Successfully Removed Index: {0}", index_name),Log_Details)
        except Exception as g:
            log_error(g, str.format( "Failed To Remove Index: {0} from Mongo", index_name),Log_Details)
 

def fetch_filings_from_index_file(index_name):

    '''

    This method downloads an index and then creates a list of filings from the index file. 
    This method takes index_name -> i.e. 2018

    '''

    # Step 1: Download Index Call the download_index method and pass index_name
    download_index(index_name)

    # Step 2. Create a file path for index_name we are processing. 
    file_path = str.format('{0}.json', index_name)

    try:

        # Step 3. open index file
        with open(os.path.join(INDEXES_DIR, file_path)) as file:

            #Step 3a. Load the file as json object
            index_obj = json.load(file)

            # Step 3b. Gather the Filing name is going to be -> Filings2018  -> This is no longer used because the old main key was removed
            #filling_name = list(index_obj.keys())[0]
            
            # Step 3c. generate an index list of dictionaries {u'OrganizationName': u'NEWPORT HARBOR BOOSTERS GIRLS LACROSSE PROGRAM', u'ObjectId': u'201820329349200102', u'URL': u'https://s3.amazonaws.com/irs-form-990/201820329349200102_public.xml', u'SubmittedOn': u'2018-03-01', u'DLN': u'93492032001028', u'LastUpdated': u'2018-03-14T23:04:38', u'TaxPeriod': u'201708', u'FormType': u'990EZ', u'EIN': u'455636537'}
            #index_list = index_obj.get(filling_name, []) ---> No Longer Used Already is a list of dictionaries
            index_list = index_obj

            # Step 3d. Close the file i.e. finish writing
            file.close()

            # Step 3e. # Set inde_obj to 0 again to avoid memory issues
            index_obj = None 

            # Step 3f. return the list of dictionaries
            return index_list 

    except Exception as g:
        log_error(g,str.format( "Failed To Fetch Filings From Index named: {0}", index_name),Log_Details)
        return None

def fetch_filings_updated(index_name):
    '''

    # NOTE!!!  This method is currently inoperable until last_updated field is properly fixed in index in datalake. 
    The Last_Updated field was in the original IRS Indices but now we make our own and dont have it. 

    This method takes a index_name i.e. "2018.json" and updates the index meaning it will try to download index incase it has changed

    '''

    # Step 1. Removes prior existing version of index 
    remove_index(index_name)

    # Step 2. Redownloads the index
    download_index(index_name)

    # Step 3. Downloads latest index and create a list of dictionaries of all filings for that index
    filings = fetch_filings_from_index_file(index_name)

    # Step 4.  Stores Yesterday's Date as difference between today and 1 day That is to say 1 day ago. 
    # Example if today is 2019-10-16 15:53:25.393400 yesterday will be 2019-10-15 15:53:25.393400
    yesterday = datetime.now() - timedelta(days=1)
 
    # Step 5.  Each object in list of dictionaries contains a last field u'LastUpdated': u'2018-03-14T23:04:38'
    #          We only want to select those where the lastupdate is greater than yesterday i.e this means its a new filing
    #          to do this we process each entry in dictionary and run a comparision i.e. is 2018-03-14 > 2019-10-16 if so add to list
    #          Then we will return the filtered list

    return filter(
        (lambda x: datetime.strptime(
            x['LastUpdated'][0:10],
            '%Y-%m-%d') > yesterday), filings
    )