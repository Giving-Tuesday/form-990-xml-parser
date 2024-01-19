
from helpers.model.form import Form990, Form990PF, Form990EZ  # this is form.py has 3 classes one for each form with 1 interface for  mongo


class FormFactory (object):

    '''

    Class that is initiated with 2 variables (1) all data -> main form 990/ez/pf data and (2) schedule data 


    '''

    def __init__(self, all_data, schedules):
        self.all_data = all_data # this allows us to pass main form data thats been parsed in formparser.py
        self.schedules = schedules # this allows us to pass schedule data thats been parsed in formparser.py

    def create(self):

        '''

        This method will create a "Form" based on type of return :990/990ez/990pf. 
        This matters because different form types get inserted in different locations in Mongo

        '''
        
        # Step 1a. Check to see if the return type is 990
        if (self.all_data.get('RETURNTYPE') == '990'):
            return Form990(self.all_data, self.schedules)
                # Step 2. If the form is a 990 form use form.py to instantiate appropriate class
                # Step 3. Return a Form 990 Object, object with data, formtype, and interface

        # Step 1b. Check to see if its a form 990pf 
        elif (self.all_data.get('RETURNTYPE') == '990PF'):
            return Form990PF(self.all_data, self.schedules)
                # Step 2. If the form is a 990 form use form.py to instantiate appropriate class
                # Step 3. Return form 990pf Object, object with data, formtype, and interface


        # Step 1c. Check to see if its a form 990ez
        elif (self.all_data.get('RETURNTYPE') == '990EZ'):
            return Form990EZ(self.all_data, self.schedules)
                # Step 2. If the form is a 990 form use form.py to instantiate appropriate class
                # Step 3. Return form 990EZ Object, object with data, formtype, and interface
            
