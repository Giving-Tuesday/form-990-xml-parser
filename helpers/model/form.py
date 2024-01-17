
from helpers.database.interface import MongoInterface 
# we are importing two interfaces one for each places we will store data to ie. mongo  
# each interface contains methods to access databases 


class Form990PF (MongoInterface):

	'''

	This class represents a form 990pf document with schedules

	Each variable that is passed to this class "Mongo interface"
	gives the class/object access to methods necessary to insert data into mongo

	In other words: Say you have a form that needs to be inserted into Mongo or removed etc. 
	The intefaces allow you to do that. 

	'''

	def __init__(self, all_data, schedules):
		super(Form990PF, self).__init__(all_data, schedules, "990PF") 
		self.form_type = "990PF"
        # Creates a form 990PF
        # w variables all data & schedules --> allows us to bind the data that will be passed from parser
        # setting the form type variable as 990pf

class Form990 (MongoInterface):


	'''

	This class represents a form 990 document with schedules

	Each variable that is passed to this class "Mongo interface"
	gives the class/object access to methods necessary to insert data into mongo

	In other words: Say you have a form that needs to be inserted into Mongo or removed etc. 
	The intefaces allow you to do that. 

	'''

	def __init__(self, all_data, schedules):
		super(Form990, self).__init__(all_data, schedules, "990")
		self.form_type = "990"
        # Creates a form 990
        # w variables all data & schedules --> allows us to bind the data that will be passed from parser
        # setting the form type variable as 990


class Form990EZ (MongoInterface):

	'''

	This class represents a form 990ez document with schedules

	Each variable that is passed to this class "Mongo interface "
	gives the class/object access to methods necessary to insert data into mongo

	In other words: Say you have a form that needs to be inserted into Mongo or removed etc. 
	The intefaces allow you to do that. 

	'''

	def __init__(self, all_data, schedules):
		super(Form990EZ, self).__init__(all_data, schedules, "990EZ")
		self.form_type = "990EZ"
        # Creates a form 990EZ
        # w variables all data & schedules --> allows us to bind the data that will be passed from parser
        # setting the form type variable as 990ez
