import sys
import helpers
import regex_tables as rt
import soft_law_citations as sl
import intl_court_citations as ic
import treaty_citations as tc
import foreign_court_citations as fc
import extract_court_refs as cr
from tqdm import tqdm
import getopt
# import logging
from time import gmtime, strftime
import csv


def get_references(REGEX_FOLDER, DATA_FOLDER):
    # Initialize logger
    # start_time = strftime("%d-%m-%Y", gmtime())
    # logging.basicConfig(filename='/tmp/get_references_%s.log' % start_time, level=logging.DEBUG,
    #                     format='[%(asctime)s %(filename)s:%(lineno)s - %(funcName)20s()] %(message)s')
    #                     # format='%(asctime)s %(levelname)s %(name)s %(message)s')
    # logger = logging.getLogger(__name__)

    # create regex tables just once
    softlaw_names, soft_law_regex_df = rt.createSoftLawRegexDf(folder_path=REGEX_FOLDER, file_name='softlaw_regex_20161003.csv')
    intl_court_names, intl_court_regex_df = rt.createIntlCourtsRegexDf(folder_path=REGEX_FOLDER, file_name='intl_courts_regex_20161003.csv')
    treaty_names, treaties_regex_df = rt.createTreatiesRegexDf(folder_path=REGEX_FOLDER, file_name='treaties_regex_20161003.csv')
    fc_country_names, fc_court_names, fc_regex_df = rt.createForeignCourtsDf(folder_path=REGEX_FOLDER, file_name='foreign_courts_regex_20161007.csv')
    country_df = rt.createCountryDf(folder_path=REGEX_FOLDER, file_name='country_ids_20161210.csv')

    # connect to mysql server and create tables
    helpers.createTables(DATABASE_NAME, PASSWORD, drop_table=True)
    ENGINE = helpers.connectDb(DATABASE_NAME, PASSWORD)
    ID_VAR = 1
    error_log = []

    for country in COUNTRY_LIST:
        print country
        countryFiles = helpers.getCountryFiles(DATA_FOLDER, country)
        for year, folder in countryFiles.items():
            # go through dictionary of files and insert into mysql table
            for file in tqdm(folder):
                try:
                    if country == 'USA':
                        country = 'United States'
                    if country == 'UK':
                        country = 'United Kingdom'
                        for function in cr.countryRefFunctions[country]:
                            case = function(file)
                            if case is not None:
                                caseInfo = case
                                # break
                    else:
                        print country
                        caseInfo = cr.countryRefFunctions[country](file)
                    if caseInfo is not None:
                        cr.insertCaseRefData(case_info=caseInfo,
                                             country_name=country,
                                             country_df=country_df,
                                             year=year,
                                             id=ID_VAR,
                                             source_file=file,
                                             mysql_table=CASE_TABLE_NAME,
                                             connection_info=ENGINE)
                    # Fix for pdfs because esm only accepting strings, not unicode
                    fileText = helpers.getFileText(file, html=True, pdf_utf8=False)
                    if fileText is not None:
                        sl.insertSoftLawData(country_name=country, year=year,
                                             file=file, fileText=fileText,
                                             regex_df=soft_law_regex_df,
                                             softlaw_names=softlaw_names,
                                             id_num=ID_VAR,
                                             mysql_table=CITATION_TABLE_NAME,
                                             connection_info=ENGINE,
                                             country_df=country_df)
                        ic.insertIntlCourtData(country_name=country,
                                               year=year,
                                               file=file,
                                               fileText=fileText,
                                               regex_df=intl_court_regex_df,
                                               intl_court_names=intl_court_names,
                                               id_num=ID_VAR,
                                               mysql_table=CITATION_TABLE_NAME,
                                               connection_info=ENGINE,
                                               country_df=country_df)
                        tc.insertTreatyData(country_name=country,
                                            year=year,
                                            file=file,
                                            fileText=fileText,
                                            regex_df=treaties_regex_df,
                                            treaty_names=treaty_names,
                                            id_num=ID_VAR,
                                            mysql_table=CITATION_TABLE_NAME,
                                            connection_info=ENGINE,
                                            country_df=country_df)
                        fc.insertForeignCourtsData(country_name=country,
                                                   year=year,
                                                   file=file,
                                                   fileText=fileText,
                                                   regex_df=fc_regex_df,
                                                   country_names=fc_country_names,
                                                   court_names=fc_court_names,
                                                   id_num=ID_VAR,
                                                   mysql_table=CITATION_TABLE_NAME,
                                                   connection_info=ENGINE,
                                                   country_df=country_df)
                    ID_VAR += 1
                except Exception, e:
                    print file
                    error_log.append('%s, %s, %s' % (country, file, e))
                    # logger.error('%s, %s, %s' % (country, file, e))
                    ID_VAR += 1
                    raise
    if len(error_log) > 0:
        start_time = strftime("%d-%m-%Y", gmtime())
        with open('/tmp/error_log_%s.csv' % (start_time), 'wb') as csvfile:
            wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            wr.writerow(error_log)


if __name__ == "__main__":
    try:
        options, arguments = getopt.getopt(sys.argv[1:], "r:d:t:a:p:c:n:",
                                           ["regex_folder=", "database_name=",
                                            "citation_table_name=",
                                            "case_table_name",
                                            "mysql_password=",
                                            "cglp_data_folder=", "country="])
    except getopt.GetoptError as error:
        sys.exit("Getopt Error: " + str(error))

    # set up default values
    COUNTRY_LIST = ['Australia', 'Austria', 'Botswana', 'Belgium', 'Canada',
                    'Chile', 'Colombia', 'France', 'Germany', 'India',
                    'Ireland', 'Lesotho', 'Malawi', 'Malaysia', 'New Zealand',
                    'Nigeria', 'Papua New Guinea', 'Peru',
                    'Philippines', 'South Africa', 'Spain', 'Switzerland',
                    'Uganda', 'UK', 'USA', 'Zimbabwe']
    INCOMPLETE_COUNTRIES = ['Latvia']
    CITATION_TABLE_NAME = 'citations'
    CASE_TABLE_NAME = 'case_info'
    DATABASE_NAME = 'cglp'
    PASSWORD = 'cglp'

    for o, a in options:
        if o in ["-r", "--regex_folder"]:
            REGEX_FOLDER = a
        elif o in ["-d", "--database_name"]:
            DATABASE_NAME = a
        elif o in ["-t", "--citation_table_name"]:
            CITATION_TABLE_NAME = a
        elif o in ["-a", "--case_table_name"]:
            CASE_TABLE_NAME = a
        elif o in ["-p", "--mysql_password"]:
            PASSWORD = a
        elif o in ["-c", "--cglp_data_folder"]:
            DATA_FOLDER = a
        elif o in ["-n", "--country_name"]:
            COUNTRY_LIST = [a]
    get_references(REGEX_FOLDER, DATA_FOLDER)
