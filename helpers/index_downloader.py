import os # allows us to use operating system functions
import json # allows us to parse and store json
#import urllib2 # allows us to handle urls - requests etc
from urllib.request import urlopen
from datetime import datetime, timedelta # allows us to figure out what date we are on and calculate a difference in dates

ROOT_DIR = os.path.join(os.path.dirname(__file__))
INDEXES_DIR = os.path.join(ROOT_DIR, 'indexes') # directory that contains indices 


def download_index(year):

    '''

    This method will download an index containing links to filings hosted on aws. 
    This method takes a year i.e. 2018 


    '''

    # Step 1. Set url index based on year. 
    url_index = 'https://s3.amazonaws.com/irs-form-990/index_{0}.json'

    # Step 2. Check to see if indexpath exists
    if not os.path.exists(INDEXES_DIR):
        # Step 2a. If index path doesnt exist create it
        os.makedirs(INDEXES_DIR)
    # Step 3. new file path will be index directory/year.json
    new_file_path = os.path.join(
        INDEXES_DIR,
        str.format('{0}.json', year)
    )
    # Step 3. Check to see if file exists
    if not os.path.isfile(new_file_path):

        # Step 3a. If file path doesnt exist print to console that you are downloading the index from amazon. 
        ## ! Could be a good thing to log
        print (str.format('Downloading index {0} from amazon.', year))

        #Step 3b. Store the downloaded index as "response"
        respose = urlopen(str.format(url_index, year))

        # Step 3c. Create a new file with downloaded index. Set file to 0 to avoid memory issues. 
        with open(new_file_path, 'w+') as file:
            file.write(respose.readline())
            file.close()
            respose = None

def remove_index(year):

    '''

    This method removes an index from the hard drive. 
    This method takes a year as the indexes are store as such 2019.json

    '''

    # Step 1. Create a path given base index directory & year -> indexes/2018.json
    path = os.path.join(INDEXES_DIR, str(year)+'.json')

    # Step 2. Check if the path exists. If patth exists means file exists
    if os.path.exists(path):
        # Step 3. Remove the path -> i.e. remove the file. 
        os.remove(path)


def fetch_filings_from_index_file(year):

    '''

    This method downloads an index and then creates a list of filings from the index file. 
    This method takes year -> i.e. 2018

    '''

    # Step 1: Download Index Call the download_index method and pass year
    #download_index(year)

    # Step 2. Create a file path for year we are processing. 
    file_path = str.format('{0}.json', year)

    # Step 3. open index file
    with open(os.path.join(INDEXES_DIR, file_path)) as file:

        #Step 3a. Load the file as json object
        index_obj = json.load(file)

        # Step 3b. Gather the Filing name is going to be -> Filings2018
        filling_name = list(index_obj.keys())[0]
        
        # Step 3c. generate an index list of dictionaries {u'OrganizationName': u'NEWPORT HARBOR BOOSTERS GIRLS LACROSSE PROGRAM', u'ObjectId': u'201820329349200102', u'URL': u'https://s3.amazonaws.com/irs-form-990/201820329349200102_public.xml', u'SubmittedOn': u'2018-03-01', u'DLN': u'93492032001028', u'LastUpdated': u'2018-03-14T23:04:38', u'TaxPeriod': u'201708', u'FormType': u'990EZ', u'EIN': u'455636537'}
        index_list = index_obj.get(filling_name, [])

        # Step 3d. Close the file i.e. finish writing
        file.close()

        # Step 3e. # Set inde_obj to 0 again to avoid memory issues
        index_obj = None 

        # Step 3f. return the list of dictionaries
        return index_list # return the index list


def fetch_filings_updated(year):
    '''

    This method takes a year i.e. "2018" and updates the index

    '''

    # Step 1. Removes prior existing version of index 
    remove_index(year)

    # Step 2. Redownloads the index
    download_index(year)

    # Step 3. Downloads latest index and create a list of dictionaries of all filings for that index
    filings = fetch_filings_from_index_file(year)

    # Step 4.  Stores Yesterday's Date as difference between today and 1 day That is to say 1 day ago. 
    # Example if today is 2019-10-16 15:53:25.393400 yesterday will be 2019-10-15 15:53:25.393400
    yasterday = datetime.now() - timedelta(days=1)
 
    # Step 5.  Each object in list of dictionaries contains a last field u'LastUpdated': u'2018-03-14T23:04:38'
    #          We only want to select those where the lastupdate is greater than yesterday i.e this means its a new filing
    #          to do this we process each entry in dictionary and run a comparision i.e. is 2018-03-14 > 2019-10-16 if so add to list
    #          Then we will return the filtered list

    return filter(
        (lambda x: datetime.strptime(
            x['LastUpdated'][0:10],
            '%Y-%m-%d') > yasterday), filings
    )