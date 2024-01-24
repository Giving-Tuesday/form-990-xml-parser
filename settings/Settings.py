import os
### Index information --- Details regarding local index location and datalake location (i.e. where to download from )
indices_directory_name = 'indices'
gt_datalake_index_location = 'https://gt990datalake-rawdata.s3.amazonaws.com/Indices/990xmls/index_{0}_efiledata_xmls_created_on_{1}.json'

''' Mongo Recommendations
   
   We recommend setting up a database called: irs_xml 
   with the following collections: 
   	  - 990 		- For all 990 xml forms smaller than 16mb
   	  - 990b 		- To store large pickle files of form 990s that surpass 16mb mongo doc limit
   	  - 990EZ 		- For all 990ez xml forms smaller than 16mb
   	  - 990EZb		- To store large pickle files of form 990ez that surpass 16mb mongo doc limit
   	  - 990PF 		- For all 990pf xml forms smaller than 16mb
   	  - 990PFb 		- To store large pickle files of form 990pf that surpass 16mb mongo doc limit
   	  - schedules 	- For all schedules forms smaller than 16mb
   	  - schedulesb  - To store large pickle files of schedules that surpass 16mb mongo doc limit
'''

### Mongo Details ----- Details to local, prod mongo along with basic database details
mongo_qa_details = 'mongodb://localhost:27017/'# Local host server details
mongo_production_details = 'mongodb://localhost:27017/' # Production server details
mongo_max_document_size = 16777216 			   # Measured in Bytes max size of mongo document DO NOT TOUCH THIS !!
mongo_database_name = 'irs_xml' 			   # Main Mongo DB Name where we store our collection(s) of documments
schedules_reg_collection_name = 'schedules'   # Name of Schedules Collection for documents < 16mb in size
schedules_large_collection_name = 'schedulesb' # Name of Schedules Collection for documents > 16mb in size

### Mapping & Concordance Deatils --- These two files refer to the concordance file created by the Nonprofit Data Collaborative
#   one file - mapping- contains main variables for all form 990,990ez,990pf, and schedules
#   the other file - mapping_table- is for table elements from form 
mapping_main_file = os.path.join('helpers', 'concordance_files', 'mapping.csv')
mapping_table_file = os.path.join('helpers', 'concordance_files', 'mapping_table.csv')