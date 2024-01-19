
import sys  # python library allows us to use sys values like from terminal
import os  # python library allows us to use operating system commands like file system
import csv  # allows us to open/close/write to CSV Files
import re  # python library for regular expressions
from settings.Settings import mongo_qa_details, mongo_production_details

def csv_to_object():
    '''

    This method reads mapping.csv file from disk and converts it to a dictionary
    Output is a dictionary of Paths/Variables
    Sample Output: {'Return/ReturnData/IRS990PF/StatementsRegardingActy4720Grp/SubjToTaxRmnrtnExPrchtPymtInd': 'F9-PF-07-SUBREPARP'}

    '''
    # Step 1. Initialize a csv object i.e. dictionary
    csv_object = {}

    # Step 2. Open the mapping csv file in read only mode -  Path will be helpers/files/mapping.csv
    file = open(os.path.join('helpers', 'files', 'mapping.csv'), 'r')

    # Step 3. Create a list of rows (for each row in csv) row will be a dictionary
    rows = csv.reader(file.readlines())

    # Step 4. For each row in list of rows
    for row in rows:
        # a row will look like this ['F9-PF-07-SUBREPARP', 'Subject to the section 4960 tax on remuneration or excess parachute payments?', 'IRS990PF-PART-07B-LINE-08', 'Return/ReturnData/IRS990PF/StatementsRegardingActy4720Grp/SubjToTaxRmnrtnExPrchtPymtInd', '2018v3.1',
        # Step 4a. Append to csv object key : 'Return/ReturnData/IRS990PF/StatementsRegardingActy4720Grp/SubjToTaxRmnrtnExPrchtPymtInd'   value: 'F9-PF-07-SUBREPARP'
        csv_object[row[3]] = row[0]

    # Step 5. Close the file since we are done processing all rows
    file.close()

    # Step 6 return the csv object
    return csv_object


def csv_table_to_object():
    '''

    This method reads mapping_table.csv file from disk and converts it to a dictionary
    Output is a dictionary of Paths/Variables
    Sample Output is {'Return/ReturnData/IRS990ScheduleI/Form990ScheduleIPartII/NoGrantMoreThan5000': 'SI-PC-02-GRANTS_SI_PART_II'}

    '''

    # Step 1. Initialize csv_object
    csv_object = {}

    # Step 2. Open Mapping_Table.csv in read only mode -  Path will be helpers/files/mapping.csv
    file = open(os.path.join('helpers', 'files', 'mapping_table.csv'), 'r')

    # Step 3. Create a list of dictionaries represents rows of csv file
    rows = csv.reader(file.readlines())

    # Step 4. For each row in the list of rows.
    for row in rows:
        # Step 4a. Store Row[1] as key and Row[0] as value
        # Sample row will look like this -> ['SI-PC-02-GRANTS_SI_PART_II', 'Return/ReturnData/IRS990ScheduleI/Form990ScheduleIPartII/NoGrantMoreThan5000']
        csv_object[row[1]] = row[0]

    # Step 5. Close file as we are done processing rows
    file.close()

    # Step 6. Return the CSV Object
    return csv_object


def get_location_form_part(original_part_line, form_type):
    '''
        Get the location of mapping form part.

        :param original_part_line: Part line without format. E.g. Part VI Section A Line 4
        :param form_type: Type of form. E.g. F990-PC

        :return: E.g. F990-PC-PART-00-SECTION-A-LINE-4
    '''
    # Value that will return
    converted_part_line = ''
    # Dictionary that contain roman numbers for translating to ordinary number
    roman_number = {
        'I': '01', 'II': '02', 'III': '03', 'IV': '04',
        'V': '05', 'VI': '06', 'VII': '07', 'VIII': '08', 'IX': '09',
        'X': '10', 'XI': '11', 'XII': '12', 'XIII': '13', 'XIV': '14',
        'XV': '15', 'XVI': '16'
    }
    # @TODO add more patterns
    # Patterns of form part line
    patterns = [
        (1, 'Part (I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV|XVI) Line (.*)'),
        (2, 'Part (I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV|XVI) Section (.*) Line (.*)'),
        (3, 'Part (I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV|XVI) Column (.*)'),
        (4, 'col (.*)'),
        (5, 'Part (.*)-(.*) Column \((.*)\)'),
    ]

    for id, pattern in patterns:
        match = re.match(pattern, original_part_line, re.IGNORECASE)
        if match:
            # List of values that matched
            groups = match.groups()
            if id == 1:
                converted_part_line = '{0}-PART-{1}-LINE-{2}'.format(
                    form_type, roman_number[groups[0]], groups[1]
                )
            if id == 2:
                converted_part_line = '{0}-PART-{1}-SECTION-{2}-LINE-{3}'.format(
                    form_type, roman_number[groups[0]], groups[1], groups[2]
                )
            if id == 3:
                converted_part_line = '{0}-PART-{1}-COLUMN-{2}'.format(
                    form_type, roman_number[groups[0]], groups[1]
                )
            if id == 4:
                converted_part_line = 'COL-{0}'.format(
                    groups[0]
                )
            if id == 5:
                converted_part_line = '{0}-PART-{1}-{2}-COLUMN-{3}'.format(
                    form_type, roman_number[groups[0]], groups[1], groups[2]
                )
    return converted_part_line.upper()


def get_form_type(source):
    '''
        Get the form type from form source.

        :param source: Source of a form. E.g. IRS990
        :return: E.g. F990-PC
    '''

    return {
        'IRS990': 'F990-PC',
        'IRS990PF': 'F990-PF',
        'IRS990EZ': 'F990-EZ',
        'IRS990ScheduleA': 'SCHED-A',
        'IRS990ScheduleB': 'SCHED-B',
        'IRS990ScheduleC': 'SCHED-C',
        'IRS990ScheduleD': 'SCHED-D',
        'IRS990ScheduleE': 'SCHED-E',
        'IRS990ScheduleF': 'SCHED-F',
        'IRS990ScheduleG': 'SCHED-G',
        'IRS990ScheduleH': 'SCHED-H',
        'IRS990ScheduleI': 'SCHED-I',
        'IRS990ScheduleJ': 'SCHED-J',
        'IRS990ScheduleK': 'SCHED-K',
        'IRS990ScheduleL': 'SCHED-L',
        'IRS990ScheduleM': 'SCHED-M',
        'IRS990ScheduleN': 'SCHED-N',
        'IRS990ScheduleO': 'SCHED-O',
        'IRS990ScheduleP': 'SCHED-P',
        'IRS990ScheduleQ': 'SCHED-Q',
        'IRS990ScheduleR': 'SCHED-R',
        'AccountingFeesSchedule': 'SCHED-PF',
        'AllOtherProgramRelatedInvestmentsSchedule': 'SCHED-PF',
        'AmortizationSchedule': 'SCHED-PF',
        'DepreciationSchedule': 'SCHED-PF',
        'GainLossFromSaleOtherAssetsSchedule': 'SCHED-PF',
        'InvestmentsCorpBondsSchedule': 'SCHED-PF',
        'InvestmentsCorpStockSchedule': 'SCHED-PF',
        'InvestmentsGovtObligationsSchedule': 'SCHED-PF',
        'InvestmentsLandSchedule2': 'SCHED-PF',
        'InvestmentsOtherSchedule2': 'SCHED-PF',
        'LandEtcSchedule2': 'SCHED-PF',
        'LegalFeesSchedule': 'SCHED-PF',
        'OtherAssetsSchedule': 'SCHED-PF',
        'OtherDecreasesSchedule': 'SCHED-PF',
        'OtherExpensesSchedule': 'SCHED-PF',
        'OtherIncomeSchedule2': 'SCHED-PF',
        'OtherIncreasesSchedule': 'SCHED-PF',
        'OtherLiabilitiesSchedule': 'SCHED-PF',
        'TaxesSchedule': 'SCHED-PF',
        'ReductionExplanationStatement': 'SCHED-PF',
        'OtherProfessionalFeesSchedule': 'SCHED-PF'
    }.get(source, '')  # Type of form


def check_new_xpaths():
    '''
        Check is there are new xpaths.
        It will generete a report `called new_xpaths_report.csv` that 
        contain all new xpath that not exist in `mapping.csv`.
    '''
    try:
        # Report structure
        reports = [("VARIABLE_NAME", "DESCRIPTION",
                    "LOCATION", "XPATH", "EXIST?")]
        # File path string of xpath_all.csv
        xpath_all_path = os.path.join('helpers', 'files', 'xpath_all.csv')
        # File path string of mapping.csv
        mapping_path = os.path.join('helpers', 'files', 'mapping.csv')
        # File path string of report
        xpath_report_path = os.path.join(
            'helpers', 'files', "new_xpaths_report.csv")

        # Mapping file object
        mapping_file = open(mapping_path, 'r')
        # Xpath All file object
        xpath_file = open(xpath_all_path, 'r')
        # Report file object
        xpath_report_file = open(xpath_report_path, 'w')
        # Csv object used for creating a csv in disk
        xpath_report_csv = csv.writer(xpath_report_file)
        # Contain all rows of mapping.csv
        mapping_lines = mapping_file.readlines()

        # Contain all xpath from mapping
        mapping_xpaths = map(lambda item: item[3],
                             csv.reader(mapping_lines))
        # Contain all location part line from mapping
        mapping_parts = map(lambda item: item[2],
                            csv.reader(mapping_lines))
        # Contain all variable name from mapping
        mapping_variable_names = map(lambda item: item[0],
                                     csv.reader(mapping_lines))
        # Contain all new xpath
        new_xpaths = map(lambda item: (item[1], item[2], item[5], item[4]),
                         csv.reader(xpath_file.readlines()))

        # Ignore the first and remove duplicate rows.
        for src, xpath, part, desc in list(set(new_xpaths[1:])):

            if xpath not in mapping_xpaths:  # Checking if the new xpath exist into the mapping
                # get the form type
                form_type = get_form_type(src)
                # get location form part the form
                location_form_part = get_location_form_part(part, form_type)

                # Checking if location of new xpath exist into the location list of mapping
                if location_form_part in mapping_parts:
                    # Note: In this case probably the xpath exist but with other structure.

                    # Find the variable name from mapping
                    variable_name = mapping_variable_names[mapping_parts.index(
                        location_form_part)]
                    # Add the new xpath to report
                    reports.append(
                        [variable_name, desc, location_form_part, xpath, "YES"])
                else:
                    # Note: In this case probably the xpath not exist into the mapping.

                    # Add the new xpath to report
                    reports.append(
                        ["N/A", desc, location_form_part, xpath, "NO"])

        # Save reports in disk
        for row in reports:
            xpath_report_csv.writerow(row)

        # Release file resources
        mapping_file.close()
        xpath_file.close()
        xpath_report_file.close()

    except IOError as e:
        print(e)


def get_config(database):
    '''

    This method gets a database name -> Mongo 
    Returns configuration variables depending on whether qa or prod was passed in console command

    '''

    # Step 1. If qa was passed as an argument via console set ENV to "qa" otherwise default as "prod"
    ENV = 'prod'
    if '--qa' in sys.argv[1:]:
        ENV = 'qa'
    elif '--backup' in sys.argv[1:]:
        ENV = 'backup'

    # Step 2. Dicionary that contains  dictionaries per storage ie. mongo with prod/qa details

    ## Originally these were in config mongo

    CONFIG = {
        'mongo': {
            'prod': mongo_production_details,
            'qa': mongo_qa_details
        }
    }

    # Step 3. Access the Config Dictionary searching for database dictionary and then env variable dictionary
    return CONFIG.get(database).get(ENV, 'prod')


def partition_list(general_list, limit):
    '''

    This method takes a general_list of dictionaries and a limit Outputs a list of list. In each list will be a link to aws xml filing
        Example Input: [{u'OrganizationName': u'JAWONIO RESIDENTIAL OPPORTUNITIES III INC', u'ObjectId': u'201803129349301355', u'URL': u'https://s3.amazonaws.com/irs-form-990/201803129349301355_public.xml', u'SubmittedOn': u'2018-12-03', u'DLN': u'93493312013558', u'LastUpdated': u'2019-02-21T16:25:33', u'TaxPeriod': u'201712', u'FormType': u'990', u'EIN': u'201078564'}]
        Example Output: [[u'https://s3.amazonaws.com/irs-form-990/201803129349301355_public.xml']]

    '''

    # Step 0. Setup some initial place holders
    result = []  # final output that will be returned
    to_change = []
    limit_change = limit

    # Step 1. For each dictionary in general list grab the url and add it to the to change list do this until none are left
    for index, element in enumerate(general_list):
        # To clarify index = position in list  element  =dictionary with content & element['URL'] = url of xml
        to_change.append(element['URL'])
        if index+1 == limit_change:
            result.append(to_change)
            limit_change += limit
            to_change = []
    # Step 2. Check to see if length of to_change is greater than 0 if it is append it to results
    if len(to_change) > 0:
        result.append(to_change)

    # Step 3. Return list of lists (of links to xml files)
    return result
