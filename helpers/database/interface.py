from datetime import datetime           # python library allows us to get current date and time use this to access appropriate index
import logging                          # python library that allows us to log things 
from pymongo import MongoClient         # library that lets us use mongo with python 
from gridfs import GridFS               # library that allows us to store files larger than 16mb into mongo 
from bson import objectid               # way to handle bson objects for mongo
from helpers.helpers import get_config  # a method that gets database details depending on arguments passed from the terminal when running xml parser script
import pickle # file format for large python objects
import os #lets us use console

SIZE_MAX_MONGO = 16777216  # Measured in Bytes max size of mongo document


#mongodb_client_original = MongoClient(get_config('mongo'), connect=False) # get_config is a method that given arguments allows us to know which environment we are using i.e. production qa etc

try:
    mongodb_client = MongoClient(get_config('mongo'),connect=False)
except Exception as e:
    print ('Trying Different Connection', e)
    mongodb_client = MongoClient(get_config('mongo'),tls=True,tlsAllowInvalidCertificates=True,connect=False)

mongo_database = mongodb_client['irs_xml'] # name of mongo database
schedules_collection = mongo_database['schedules'] # mongo collection that holds schedules
schedules_collection_b = GridFS(mongo_database, 'schedulesb') # mongo collection that holds schedules larger than 16mb


class MongoInterface (object):

    '''
    Represent MongoDB Interface
    
    This is a class that later gets attached to documents that  are parsed and created and allows us to upload the documents into mongo. 
    '''

    def __init__(self, all_data, schedules, form_type):
        self.all_data = all_data   # this variable allows us to pass main form data into mongo
        self.schedules = schedules # this variable allows us to pass schedule data into mongo
        self.form_type = form_type # this variable will be used to help us identify which index to store data to

    def __remove_form(self, collection):

        '''

        This method removes a form from MongoDB, name of collection (990/990pf/990ez) is passed to this method. 

        '''

        # Step 1 is to use the collection name to find 1 document using find_one mongo api -> https://docs.mongodb.com/manual/reference/method/db.collection.findOne/
        # We use two criteria EIN & TAXYEAR as that ensures that we have singled out only 1 document. If we just used EIN we would get more than one
        nonprofit = collection.find_one({
            'FILEREIN': self.all_data.get('FILEREIN'),
            'TAXYEAR': self.all_data.get('TAXYEAR')
        })

        # If the result exists i.e. the document exists then we proceed to delte all related data
        if nonprofit:
            # find all the relevant schedules for this document and delete them. Remember schedules are in separate collection than main document. 
            #print ("Removing Schedules Associated with Document")
            schedules_collection.delete_many({
                '_id': {'$in': nonprofit.get('schedules', [])}
            })
            #print (str.format("Deleting record for EIN: {0} TaxYear: {1} ", self.all_data.get("FILEREIN"),self.all_data.get("TAXYEAR")))
            logging.info(collection.delete_one({'_id': nonprofit.get('_id', 0)}))

        else:
            #print (str.format("Record not found for EIN: {0} TaxYear: {1} ", self.all_data.get("FILEREIN"),self.all_data.get("TAXYEAR")))
            logging.info(str.format("Record not found for EIN: {0} TaxYear: {1} ", self.all_data.get("FILEREIN"),self.all_data.get("TAXYEAR")))

    def __form_not_exists(self, collection):

        '''

        This methods checks to see if a form exist in mongo once we have provided a colleciton to search. 

        '''

        # Step 1 is to use the collection name to find 1 document using find_one mongo api -> https://docs.mongodb.com/manual/reference/method/db.collection.findOne/
        # We use two criteria EIN & TAXYEAR as that ensures that we have singled out only 1 document. If we just used EIN we would get more than one
        nonprofit = collection.find_one({
            'FILEREIN': self.all_data['FILEREIN'],
            'TAXYEAR': self.all_data['TAXYEAR']
        })
        # Step 2 if the search results in nothing then we return None
        return nonprofit is None

    def insert_data_to_mongo(self):

        '''

        This method will insert form & schedule data into mongo after making sure it doesnt already exist

        '''

        # Step 1. by accessing the form type. We can specify which collection to access. 
        # Example if Form is 990pf mongodb_client['irs_xml'] -> collection = mongodb_client['irs_xml']['990PF'] 
        collection = mongo_database[self.form_type]
        collectionb = GridFS(mongo_database, (self.form_type+'b')) # way to access collection by form type for documents larger than 16mb

        # Step 2. Log whats going on i.e ein we are processing 
        logging.info(str.format("PROCESSING EIN: {0}",self.all_data.get("FILEREIN")))
        #print (str.format("PROCESSING EIN: {0}",self.all_data.get("FILEREIN")))

        # Step 3. Check to make sure the form doesn't already exist in Mongo if it does we pass because we are not interested in reinserting information
        if self.__form_not_exists(collection): # if it doesn't exist i.e. if return is a NONE:

            # Step 3a. If the form contains schedule data (remember schedules is one of the 2 data dictionaries store with each form )
            if self.schedules:

                # Step 3a1
                # Then we need to insert each schedule's data in data collection 
                # This is mongo docs for insert_many-> https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/
                try:
                    schedules_ids = schedules_collection.insert_many(self.schedules).inserted_ids
                    # Step 3a2 # Once we have inserted them we get as a result -> { "acknowledged" : true, "insertedIds" : [ 10, 11, 12 ] }
                    # We will store schedules_ids -> as list [ 10, 11, 12 ] these are the unique identifiers then for what we just inserted

                    # Step 3a3 Now that we have inserted the schedule data into mongo -> we just insert the id's for each schedule into the main form 
                    # remember all_data is the dictionary with the main form -> so now it contains a field called ['schedules'] with identifiers for each schedule that goes with that form 
                    self.all_data['schedules'] = schedules_ids
                    logging.info(str.format( "SUCCESSFULLY INSERTED SCHEDULE DATA FOR EIN: {0} into mongo", self.all_data.get("FILEREIN")))
                    #print (str.format( "SUCCESSFULLY INSERTED SCHEDULE DATA FOR EIN: {0} into mongo", self.all_data.get("FILEREIN")))

                except:
                    # This happens if BSON is too large then we need to use gridfs to insert into mongo database
                    try:
                        schedules_inserted = []
                        schedz = self.schedules
                        # Processing each schedule and inserting it as a separate object into main data form
                        for sched in schedz:
                            # for each schedule in list of schedules store schedule and save object
                            filenm= self.all_data.get("FILEREIN")+"_"+self.all_data.get('TAXYEAR')+"_"+ sched.get("type")+".pickle"
                            with open(filenm, 'wb') as handle:
                                # Saves schedule as pickle file locally
                                pickle.dump(sched, handle, protocol=pickle.HIGHEST_PROTOCOL)
                            with open(filenm) as z:
                                # Save pickle filed schedule to mongo
                                x = schedules_collection_b.put(z, content_type='pickle', type= sched.get("type"), filename=filenm, year=self.all_data.get('TAXYEAR'), state=self.all_data.get('FILERUSSTATE'), FILEREIN=self.all_data.get("FILEREIN"), filing_type =self.form_type )
                            # Converts object's objectid into string to append to list
                                nam = str(x)
                            # Append object id to list
                                schedules_inserted.append(nam)
                        # add list of schedules inserted into schedules
                        self.all_data['schedules'] = schedules_inserted
                        logging.info(str.format( "SUCCESSFULLY INSERTED SCHEDULE DATA FOR EIN: {0} into mongo gridfs list of ids{1}", self.all_data.get("FILEREIN"),self.all_data.get("schedules")))
                        #print (str.format( "SUCCESSFULLY INSERTED SCHEDULE DATA FOR EIN: {0} into mongo gridfs list of ids{1}", self.all_data.get("FILEREIN"),self.all_data.get("schedules")))
                    except Exception as g: 
                        # If this fails then we log it
                        logging.info(str.format( "FAILED TO INSERT SCHEDULE DATA FOR EIN: {0} into mongo gridfs. Error was: {1}", self.all_data.get("FILEREIN"), g ))
                        #print (str.format( "FAILED TO INSERT SCHEDULE DATA FOR EIN: {0} into mongogridfs. Error was: {1}", self.all_data.get("FILEREIN"), g ))

            # we take the main form type in our example 990PF -> insert the main form data by calling the mongo insert_one method docs ->  https://docs.mongodb.com/manual/reference/method/db.collection.insertOne/
            try:

                # Step 4. Insert Document
                collection.insert_one(self.all_data)
                
                # Step 5. We log that insertion works
                logging.info(str.format( "SUCCESSFULLY INSERTED MAIN FORM DATA FOR EIN: {0} into mongo",self.all_data.get("FILEREIN")))
                #print (str.format( "SUCCESSFULLY INSERTED MAIN FORM DATA FOR EIN: {0} into mongo",self.all_data.get("FILEREIN")))

            except:
                # Step 6. If insertion failed we make note of it
                try:
                    # Setup name of pickle file
                    filenm2= self.all_data.get("FILEREIN")+"_"+self.all_data.get('TAXYEAR')+"_"+"main_form"+".pickle"
                    with open(filenm2, 'wb') as second_handle:
                        # Save as pickle file
                        pickle.dump(self.all_data, second_handle, protocol=pickle.HIGHEST_PROTOCOL)
                    with open(filenm2) as zz:
                        # Try to insert pickle file using gridfs
                        collectionb.put(zz, content_type='pickle', filename=filenm2, year=self.all_data.get('TAXYEAR'), state=self.all_data.get('FILERUSSTATE'), FILEREIN=self.all_data.get("FILEREIN"), filing_type=self.form_type)
                    # Step 5. We log that insertion works
                    logging.info(str.format( "SUCCESSFULLY INSERTED MAIN FORM DATA FOR EIN: {0} into mongo gridfs",self.all_data.get("FILEREIN")))
                    #print (str.format( "SUCCESSFULLY INSERTED MAIN FORM DATA FOR EIN: {0} into mongo gridfs",self.all_data.get("FILEREIN")))
                except Exception as h:
                    # If this fails we log it. 
                    logging.info(str.format("FAILED TO INSERT MAIN FORM DATA FOR EIN: {0} into mongo gridfs. Error was: {1}", self.all_data.get("FILEREIN"), h))
                    #print (str.format("FAILED TO INSERT MAIN FORM DATA FOR EIN: {0} into mongo gridfs. Error was: {1}", self.all_data.get("FILEREIN"), h ))
            
            # Step 6. Removing Large Pickle Files That were created and inserted using GridFs    
            #try:
            #    os.system("rm *.pickle") # removes all pickle files
            #except:
            #    pass
        else:
            logging.info(str.format( "FORM FOR EIN: {0} ALREADY EXISTS IN MONGO!", self.all_data.get("FILEREIN") ) )
            #print (str.format( "FORM FOR EIN: {0} ALREADY EXISTS IN MONGO!", self.all_data.get("FILEREIN")))


    def insert_data_force_to_mongo(self):
        
        '''

        This method tries to forceably re-insert data into mongo by first removing a form from a collection and then re-inserting it

        '''

        #Step 1. Establish type of form we are trying to insert -> need to identify form type to determine which collection to search/insert.
        # Example if Form is 990pf mongodb_client['irs_xml'] -> collection = mongodb_client['irs_xml']['990PF'] 
        collection = mongo_database[self.form_type]

        # Step 2. Call the remove form method passing the collection information 
        self.__remove_form(collection)

        # Step 3. Call the insert data into mongo method. 
        self.insert_data_to_mongo()

        ##! Opportunity for some error/log handling

    def update_data_mongo(self):

        '''

        This method allows us to update existing mongo data when a new version of an existing document appears

        '''

        #Step 1. Establish type of form we are trying to insert -> need to identify form type to determine which collection to search/insert.
        # Example if Form is 990pf mongodb_client['irs_xml'] -> collection = mongodb_client['irs_xml']['990PF'] 
        collection = mongo_database[self.form_type]

        # Step 2. Find a document 
        # We use two criteria EIN & TAXYEAR as that ensures that we have singled out only 1 document. If we just used EIN we would get more than one
        nonprofit_from_db = collection.find_one({
            'FILEREIN': self.all_data.get('FILEREIN'),
            'TAXYEAR': self.all_data.get('TAXYEAR')
        })

        # Step 3. if we found a document match 
        if nonprofit_from_db is not None:

            # Step 3a. Update Schedule Data
            # Call method _Update_Schedules passing the nonprofit_from_db data this is necessary so that we know 
            self.__update_schedules(nonprofit_from_db)
            logging.info(str.format("Updaing Schedule Data for EIN: {0} Tax Year: {1}", self.all_data.get("FILEREIN"),self.all_data.get("TAXYEAR") ))
            # Step 3b. Update Main Form Data
            # Remember in our example collection = mongodb_client['irs_xml']['990PF'] 
            collection.update_one(
                {'_id': nonprofit_from_db.get('_id')}, # passing id so we can identify the proper document
                {'$set': self.all_data}) # Passing all the document data
             logging.info(str.format("Updaing Main Form Data for EIN: {0} Tax Year: {1}", self.all_data.get("FILEREIN"),self.all_data.get("TAXYEAR") ))

        else: 
            # This means we didn't find an existing document so actually the document needs to be inserted not updated. 
            # Step 4. Call the insert_data_into_mongo method
            self.insert_data_to_mongo()

    def __update_schedules(self, nonprofit_from_db):
        

        '''

        This method allows us to update existing mongo schedule data when a new version of an existing document with schedules appear
        This method takes a list of documents i.e. dictionary like objects representing form data.  

        '''

        # step 1. Get a list of all the schedule document ids from each document that has been passed to method. 
        for schedule_id in nonprofit_from_db.get('schedules', []):
        # Step 2. For each scheduleid that has been found for each form. 
            # remember schedules_collection = mongo_database['schedules']  

            # Step 2a. Connect to mongo schedules collection and find the relevant document given the id using find_one mongo api -> find_one mongo api -> https://docs.mongodb.com/manual/reference/method/db.collection.findOne/
            schedule_from_db = schedules_collection.find_one(
                {"_id": schedule_id})

            # Step 2b1. 
            # create a list: of schedules by setting schedule equal to all the schedule data in the form self.schedules -> is dictionary with schedule data
              # but only if schedule.get('type') == schedule_from_db.get('type') in other words we only want to update if its same type of schedule. 
              # we dont want to override schedule a data with schedule b data. 
            schedule_filter = [
                schedule
                for schedule in self.schedules
                if schedule.get('type') == schedule_from_db.get('type')
            ]

            # Step 2b2. # If the schedule_filter has schedule information 
            if schedule_filter:
                # Connect to mongo mongo_database['schedules'] 
                # Step 2b2a Update document using update_one mongo api https://docs.mongodb.com/manual/reference/method/db.collection.updateOne/
                schedules_collection.update_one(
                    {'_id': schedule_id},
                    {'$set': schedule_filter[0]}) 
                    # we pass schedule filter [0] because there should only be one match for the document given ein/tax year etc. 
                    # Schedule_filter 0 at this point is the underlying schedule data
                break
       
