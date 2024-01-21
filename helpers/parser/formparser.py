#import urllib2 # this is a library that allows us to handle url requests
import os,sys
from urllib.request import urlopen
import re # this allows us to use regular expressions in python
from lxml import etree # this is an xml parsing library 
import logging # allows us to store logs
from helpers.factory.formfactory import FormFactory # library allows us to create forms
from helpers.loggingutil import Log_Details, log_error # Import Custom Logging

Log_Details.script = os.path.split(sys.argv[0])[1] # Store name of current script in Log_Details class object as script name. We do this so that error log will always tell us which script error comes from. 

URL_IRS = '{http://www.irs.gov/efile}'   # This is a tag present in all XML filings
REGEXP_SCHEDULE_TYPE = '(.*)-(PF|EZ|PC)' # We use this to undestand what part of form or schedule we are dealing with
REGEXP_TYPE = '(.*)-(.*)-(.*)'           # We use this to remove initial unesscessary data and grab variable name: F9-PC-09-PENPLACONTOT becomes -> PENPLACONTOT

class FormParser (object):

    '''

    Each form parser is a class object with 4 initiated variables/objects and various methods used to parse xml
    Xml is parsed as main form data, table data, and schedule data. 


    '''

    def __init__(self, csv_object, csv_table_object):
        self.csv_object = csv_object # Initiate with a csv_object variable within class/object allows us to pass/access/store variables mapping
        self.csv_table_object = csv_table_object # Initiate with a csv_table variable within/object allows us to pass/access/store table variables mapping
        self.object_parsed = {} # Initiate with a variable that allows us to store parsed results
        self.type = '' # Initiate with an empty variable that gets set as we parse document. I.e 990/EZ/PF etc

    def path_to_variable_name(self, path):
        
        '''

        Find the variable name in csv object, i.e. providing a path return variable name

        '''

        return self.csv_object.get(path)

    def find_table_value(self, path, object_parsed, root):

        '''

        Goal: Find the lists which represent a table in the form. 

        Receive a path, object_parsed dictionary that is initially empty, document
        '''
	

        # Step 1. Create a variable Say a path is Return/ReturnHeader/ReturnTs -> Path to cleaned will be Return/ReturnHeader/
        path2_cleaned = path[:path.rfind('/')]

        # Step 2. Create a variable Path to Original will then be {http://www.irs.gov/efile}ReturnHeader
        path2_original = path2_cleaned.replace(
            '/', '/' + URL_IRS).replace('Return/', '')


        # Step 3a. If path count is 3 or 4 ( only paths with more 2 '/' represent tables) 
        if path2_cleaned.count('/') == 3 or path2_cleaned.count('/') == 4:
            # if there are more than 3 paths then we need to find the path form the table object otherwise we skip it because its not a table
            # for example Return/ReturnHeader/PreparerFirmGrp/PreparerFirmName

            # Step 3b. Set variable_name list as result of calling tableobject lookup # variable name list then will be Return/ReturnData/IRS990/Form990PartVIISectionAGrp or NONE
            variable_name_list = self.csv_table_object.get(path, None) 

            # Step 3c1 if the value is not none: example: F9-PC-07-OFFICERS_PC_PART_VII_A
            if variable_name_list:


                # Step 3c2 if the variable is not in object_parsed dictionary
                if variable_name_list not in object_parsed.keys():

                    # Step 3c2a create an empty list for the variable
                    object_parsed[variable_name_list] = [] 

                    # Step 3c2b use the path to original to find all the elements
                    elememts = root.findall(path2_original)

                    # Step 3c2c for each element that is found 
                    for element in elememts:

                        # Step 3c2c1 create a dictionary
                        row = {}

                        # STep 3c2c2 # as long as the length of list associated with variable is less than list of elements
                        if len(object_parsed[variable_name_list]) < len(elememts):

                            # Step 3c2c2a search and store paths by passing element and path 2 cleaned
                            paths = self.find_all_path(element, path2_cleaned)

                                # path2_cleaned  Return/ReturnData/IRS990/Form990PartVIISectionAGrp
                                # paths = [('Return/ReturnData/IRS990/Form990PartVIISectionAGrp/PersonNm', 'JILL WARNER'), ('Return/ReturnData/IRS990/Form990PartVIISectionAGrp/TitleTxt', 'EXECUTIVE DIRECTOR/CEO')]

                            # Step 3c2c2b if the list of tuples is length 1 example: [('Return/ReturnData/IRS990ScheduleR/IdRelatedTaxExemptOrgGrp/DisregardedEntityName/BusinessNameLine1Txt', 'JAWONIO INC')]
                            if len(paths) == 1:

                                # Step 3c2c2b1 Set parent 
                                parent = element.getparent()
                                # example parent: <Element {http://www.irs.gov/efile}IdRelatedTaxExemptOrgGrp at 0x110ad8b90>
                                
                                # Step 3c2c2b2 Set path parent
                                path_parent = path2_cleaned[:path2_cleaned.rfind('/')]
                                # path_parent : Return/ReturnData/IRS990ScheduleR/IdRelatedTaxExemptOrgGrp
                                # Generate list of tuples passing an element and path output is list tuples (path/value)

                                # Step 3c2c2b2 Find all paths and set as paths
                                paths = self.find_all_path(parent, path_parent)
                                # example: ('Return/ReturnData/IRS990ScheduleR/IdRelatedTaxExemptOrgGrp/DisregardedEntityName/BusinessNameLine1Txt', 'JAWONIO INC')
                            
                            # Step 3c2c2c if paths contains more than 1 tuple 
                            for path, value in paths:

                                # Step 3c2c2c1 Set variable name by searching for the variable name from mapping i.e. csv_object
                                variable_name = self.path_to_variable_name(path)

                                # Step 3c2c2c2 # Assuming variable name exists if not pass !!!GOOD PLACE FOR ERROR HANDLING
                                if variable_name:  

                                    # Step 3c2c2c2a do a regular expression on '(.*)-(.*)-(.*)', passing a variable name
                                    pattern = re.search(REGEXP_TYPE, variable_name)
                                    # example variable name F9-PC-07-NAMEPEPERSON

                                    # Step 3c2c2c2b if a pattern exists
                                    if pattern:

                                        # Step 3c2c2c2b1 select everything after third dash -> NAMEPEPERSON from F9-PC-07-NAMEPEPERSON
                                        variable_name_cleaned = pattern.group(3)
                                        variable_name_cleaned = re.sub(r'[^\x00-\x7f]',r'', variable_name_cleaned)
                                        # Step 3c2c2c2b2 update the row dictionary
                                        row.update({variable_name_cleaned: value})
                            
                            # 3c2c2d Store data into variable_name_list and append
                            object_parsed[variable_name_list].append(row)

                                # 
                                # So a section with tables like the realted orgs table with variable SR-PC-02-RELATED_ORGS_PART_II
                                # will contain a dictionary with key - SR-PC-02-RELATED_ORGS_PART_II and values: 
                                #        [{'IICONTROORGRG': 'false', 'IIEININN': '131761660', 'IIDCENBBNLINE11': 'NA', 
                                #        'IIEXEMCODESECT': '501(C)(3)', 'IIPUBLCHARSTAT': '10', 'IIADDRADDRLINE1': 
                                #        '260 NORTH LITTLE TOR ROAD', 'IIADDRESSTATET': 'NY', 'IIADZIIPPCCOOD': '10956', 
                                #        'IIPRIMARACTIVI': 'SRVC PROVIDER', 'IINODEBNLINE11': 'JAWONIO INC', 
                                #        'IILEGADOMISTAT': 'NY', 'IIADDRESCITYIT': 'NEW CITY'}, {'IICONTROORGRG': 
                                #        'false', 'IIEININN': '133889526', 'IIDCENBBNLINE11': 'NA', 'IIEXEMCODESECT': 
                                #        '501(C)(3)', 'IIPUBLCHARSTAT': '10', 'IIADDRADDRLINE1': '260 NORTH LITTLE TOR ROAD', 
                                #        'IIADDRESSTATET': 'NY', 'IIADZIIPPCCOOD': '10956', 'IIPRIMARACTIVI': 'HOUSING', 
                                #        'IINODEBNLINE11': 'JAWONIO RESIDENTIAL OPPORTUNITIES INC', 'IILEGADOMISTAT': 'NY', 'IIADDRESCITYIT': 
                                #        'NEW CITY'}, {'IICONTROORGRG': 'false', 'IIEININN': '134109910', 'IIDCENBBNLINE11': 
                                #        'NA', 'IIEXEMCODESECT': '501(C)(3)', 'IIPUBLCHARSTAT': '10', 'IIADDRADDRLINE1': '
                                #        260 NORTH LITTLE TOR ROAD', 'IIADDRESSTATET': 'NY', 'IIADZIIPPCCOOD': '10956', 
                                #        'IIPRIMARACTIVI': 'HOUSING', 'IINODEBNLINE11': 'JAWONIO RESIDENTIAL OPPORTUNITIES 
                                #        II INC', 'IILEGADOMISTAT': 'NY', 'IIADDRESCITYIT': 'NEW CITY'}]
                                


    def find_all_path(self, elem, elem_path="", root=None):

        '''

        Given an xml element, tag called return which is used as path. Find Paths from XML Form -- 
        initially elem = document  and elem_path = "Return" -- return a list of tuples (path and text)

        '''
        
        # Step 0 create a list container where paths will be stored
        paths = [] 

        # Step 1. Create a find_path funciton that takes elemen and element_path ""
        def find_path(elem, elem_path=""):

            # Step 2b1. for each child of the main element that is to say document
            for child in elem:
                # Child.text the value inside a tag example: <ReturnTs>2018-11-08T09:39:29-06:00</ReturnTs> -> would be2018-11-08T09:39:29-06:00

                # Step 2b2 if child element has no chilren and it has text that is to say it's not empty (i.e. leaf vs branch)
                if not child.getchildren() and child.text:
                    # path for say -> <ReturnHeader><ReturnTs>2018-11-08T09:39:29-06:00</ReturnTs><Returnheader>
                    # will be Return/ReturnHeader/ReturnTs

                    # Step 2b2a. Store path 
                    path = "%s/%s" % (elem_path, child.tag.replace(URL_IRS, ''))

                    # Step 2b2b. Append a tuple of (path and child.text) -> ('Return/ReturnHeader/ReturnTs', '2018-11-08T09:39:29-06:00')
                    paths.append((path, child.text))

                # Step 2b3. if child element has children i.e. & its not empty  (branch vs leaf)
                else: 
                    # Step 2b3a Recursion call same function 
                    # We will have to get sub children element is now the branch aka child
                    # Element path now equals "element_path originally Return now Return/ReturnHeader each time additional path gets added"
                    find_path(child, "%s/%s" % (elem_path, child.tag.replace(URL_IRS, '')))

        # Step 2. Call the function passing the element and element path passed into function. 
        find_path(elem, elem_path)

        #Step 3. Once we have parsed everything we return a list of tuples called path contianing (path and text)
        return paths

    def find_schedules(self, object_parsed):

        '''

        Looks at parsed object and searches for schedules constructs a list of dictionaries, each dictionary represents a schedule with key:value pairs

        '''

        # Step 0 Setup a list container where we will store the schedules
        schedules = [] 

        # Step 1. Setup a dictionary container for meta data 
        meta = {} 

        # Step 2. for each key in object_parsed example: 'F9-PC-01-TOGRUBBII': '0'
        for var_key in object_parsed:

            # Step 2a. If the var_key is None continue
            if var_key is None: # could be a good error log area
                continue

            # Step 2b. Search the var_key using a regular expression that matches for '(.*)-(PF|EZ|PC)' and store first group
            # example SA-PC-02-IIUBTICTYMYE3 ---> will store SA which corresponds with schedule a
            type_schedule = re.search(REGEXP_SCHEDULE_TYPE, var_key, re.IGNORECASE).group(1)
            

            # Step 2c Using a regular expression extract the variable name -> IIUBTICTYMYE3
            variable_name = re.search("(.*)-(.*)-(.*)", var_key).group(3)
            variable_name = re.sub(r'[^\x00-\x7f]',r'', variable_name)
            # adding meta data to file  ## test with pf, ez, regular might need to add variations adding this so that we know what type of col
            
            # Following if statements all check for certain variables and add them to meta container
            # Step 2d. Grab & Store EIN
            if variable_name == 'FILEREIN':
                meta['FILEREIN'] = object_parsed[var_key]
            # Step 2e. Grab & Store Tax Period Beginning Date
            if variable_name == 'TAXPERBEGIN':
                meta['TAXPERBEGIN'] = object_parsed[var_key]
            # Step 2f. Grab & Store Tax Ending Date
            if variable_name == 'TAXPEREND':
                meta['TAXPEREND'] = object_parsed[var_key] 
            # Step 2g  Grab & Store Taxyear
            if variable_name == 'TAXYEAR':
                meta['TAXYEAR'] = object_parsed[var_key]
            # Step 2h. Grab & Store Filername
            if variable_name == 'FILERNAME1':
                meta['FILERNAME1'] = object_parsed[var_key]
            # Step 2i. Grab & Store Street Address
            if variable_name == 'FILERUS1':
                meta['FILERUS1'] = object_parsed[var_key]
            # Step 2j Grab and store city
            if variable_name == 'ADDRESCITYIT':
                meta['ADDRESCITYIT'] = object_parsed[var_key]
            # Step 2k1 Grab and store state
            if variable_name == 'ADDRESSTATET':
                meta['ADDRESSTATET'] = object_parsed[var_key]
            # Step 2k2 Grab and store zipcode
            if variable_name == 'ADZIIPPCCOOD':
                meta['ADZIIPPCCOOD'] = object_parsed[var_key]

            # Step 2l as long as the schedule is not of type F9 because this means its main form 
            if type_schedule != 'F9': 

                # Step 2l1 Appends an object into a list if the object is in the schedules container and has a key value pair where type = schedule type
                schedules_filtered = [obj for obj in schedules if obj['type'] == type_schedule]
                # example [{'type': 'SA', 'IIUBTICTYMYE3': '13'}] is going to trickle in and then {'type': 'SA', 'IIUBTICTYMYE2'} and it needs to be added in

                # Step 2l2 if the length is 0, 
                if len(schedules_filtered) == 0: 
                    # Step 2l2a append to schdules container a dictionary with variable name: value and type
                    schedules.append({
                        variable_name: object_parsed[var_key],
                        'type': type_schedule,
                        'FILEREIN':meta['FILEREIN'],
                        'FILERNAME1':meta['FILERNAME1'],
                        'TAXYEAR':meta['TAXYEAR'],
                        'TAXPERBEGIN':meta['TAXPERBEGIN'],
                        'TAXPEREND':meta['TAXPEREND']
                    })
                
                # Step 2l3 update schedules filtered
                else: #  variable position 0 which is a dictionary and add additional key value pair as it belongs in schedule
                    schedules_filtered[0].update({
                        variable_name: object_parsed[var_key],
                        'type': type_schedule,
                        'FILEREIN':meta['FILEREIN'],
                        'FILERNAME1':meta['FILERNAME1'],
                        'TAXYEAR':meta['TAXYEAR'],
                        'TAXPERBEGIN':meta['TAXPERBEGIN'],
                        'TAXPEREND':meta['TAXPEREND']
                    })

        # Step 3. Return schedules i.e. list of dictionaries = each dictionary is a schedule with schedule contents
        return schedules 

    def find_all_data(self, object_parsed):

        ''' 

        Called by create form. Takes object_parsed in handle_objects_parsed. Looks for all variables and values and stores them in dictionary representing entire filing

        '''

        # Step 0. Create dictionary container for data notice this is a dictionary not a list because it represents the entire form without schedules 
        all_data = {} 

        # Step 1. For each key in object_parsed example: 'F9-PC-01-TOGRUBBII': '0'
        for var_key in object_parsed:
            
            # Step 1a If the var_key is None continue
            if var_key is None:
                continue

            # Step 1b Search the var_key using a regular expression that matches for '(.*)-(PF|EZ|PC)' and store first group
            # example F9-PC-09-PENPLACONTOT ---> will store F9 which corresponds with schedule a form 990
            type_form = re.search(REGEXP_SCHEDULE_TYPE, var_key, re.IGNORECASE).group(1)

            # Step 1c Search the var_key using a regular expression extract the variable name -> TOFUEXPRSEER
            variable_name = re.search("(.*)-(.*)-(.*)", var_key, re.IGNORECASE).group(3)
            variable_name = re.sub(r'[^\x00-\x7f]',r'', variable_name)
            # Step 1d If its main form data (i.e. form 990, 990ez, 990pf) add data otherwise its a schedule and has been processed
            if type_form == 'F9': 

                # Step 1d1 Use F9-PC-09-PENPLACONTOT to look up value that will be stored for key i.e. variable: PENPLACONTOT
                all_data.update({variable_name: object_parsed[var_key]})

        # Step 2 return all_data container #! should we also set container back to empty 
        return all_data

    def handle_object_parsed(self, paths, object_parsed):

        ''' This function receives paths (a list of tuples containint (path/text)) and an empty dictionary'''

        #Step 0 For each key, value store in paths
        for path, value in paths:

            # Step 1. uses the paths from paths tuple and looks up corresponding variable in csv_object i.e mapping
            variable_name = self.path_to_variable_name(path)
            #variable_name = re.sub(r'[^\x00-\x7f]',r'', variable_name)
            # Step 2. first we are going to extract all tables
            # Look for a table value calling find_table_value function passing path, empty dictionary, xml document
            self.find_table_value(path, object_parsed, self.root)

            # Step 3a. if the variable is in the object_parsed i.e. in the dictionary
            if variable_name in object_parsed: 
                    
                # Step 3a1check the dictionary for variable value if its not a list then store dictionary[variable_name] = [list_value]
                if type(object_parsed[variable_name]) != list:

                    # Step 3a1a store object parsed[variable name] as a list with variable name 
                    object_parsed[variable_name] = [object_parsed[variable_name]]

                # Step 3a2 The variable is already a list so we are just appending value to list
                object_parsed[variable_name].append(value)

            # Step 3b. if the variable name isn't in the dictionary
            else:
                #Step 3b1 then append the variable name and value to dictionary
                object_parsed[variable_name] = value

    def create(self, xml_link):

        '''
        This method takes an xml_link i.e. location of a xml filing, downloads the filing, processes it and creates a form object which can then be inserted. 

        '''

        # Step 0 Initialize the form as None. Helps to clear any prior data & save space. 
        form = None 

        # Step 1a. Try to run following code
        try:
            # Step 2 Given a link download the filing and store as parser.root
            self.root = etree.XML(urlopen(xml_link).read())

            # Step 3. Find all paths from xml form. Passing (Document, stripping url from form so says 'Return' only, Document)
            # Once this step is done we will have a list of paths and variables. 
            paths = self.find_all_path(self.root, self.root.tag.replace(URL_IRS, ''), self.root)

            # Step 4. Passing Paths which is a list of tuples (path and text) & object_parsed which is an empty dictionary initated with class
            self.handle_object_parsed(paths, self.object_parsed)
            # -----currently here

            # Step 5. Find all schedules data in parsed object. Result will be a list of dictionaries with each dictionary representing a schedule & its contents
            schedules = self.find_schedules(self.object_parsed)

            # Step 6. Find all data related to main form 990/ez/pf
            all_data = self.find_all_data(self.object_parsed)

            # Step 7. To the main filing data that we got in step 6 add a link so that we can download the original xml if ever needed from aws. 
            all_data['XML_LINK'] = xml_link

            # Recap: at this point we have all the main form data in -> all_data and we have schedule data in schedules

            # Step 8. We are passing all the data (main form, and schedules) to create a form 
            form = FormFactory(all_data, schedules).create()

        # Step 1b. If code cant be run throw an exception and print it to console
        except Exception as g:

            # Step 1b1. Print Exception to console
            log_error(g, str.format( "Issue parsing the following xml file: {0}.", xml_link), Log_Details)

        # Step 2/9. Return the form that has been created back to whatever file called this method -> main_xml.py
        # Once the form is created and return, main can then store it or do other things with it.  
        return form
