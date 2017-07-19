import sys
import helpers
import regex_tables as rt
import soft_law_citations as sl
import intl_court_citations as ic
import treaty_citations as tc
import foreign_court_citations as fc
import extract_court_refs as cr
import getopt
# import logging
from time import gmtime, strftime
import csv
import multiprocessing as mp
import uuid


def insertData(file_info,country):
    file = file_info[0]
    ID_VAR = file_info[1]
    year = file_info[2]
    if country == 'United Kingdom':
                    for function in cr.countryRefFunctions[country]:
                        case = function(file)
                        if case is not None:
                            caseInfo = case
                            # break
    else:
        caseInfo = cr.countryRefFunctions[country](file)
    if caseInfo is not None:
        case_res = cr.insertCaseRefData(case_info=caseInfo,
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
        softLaw = sl.insertSoftLawData(country_name=country, year=year,
                                       file=file, fileText=fileText,
                                       regex_df=soft_law_regex_df,
                                       softlaw_names=softlaw_names,
                                       id_num=ID_VAR,
                                       mysql_table=CITATION_TABLE_NAME,
                                       connection_info=ENGINE,
                                       country_df=country_df)
        intlCourt = ic.insertIntlCourtData(country_name=country,
                                           year=year,
                                           file=file,
                                           fileText=fileText,
                                           regex_df=intl_court_regex_df,
                                           intl_court_names=intl_court_names,
                                           id_num=ID_VAR,
                                           mysql_table=CITATION_TABLE_NAME,
                                           connection_info=ENGINE,
                                           country_df=country_df)
        treaty = tc.insertTreatyData(country_name=country,
                                     year=year,
                                     file=file,
                                     fileText=fileText,
                                     regex_df=treaties_regex_df,
                                     treaty_names=treaty_names,
                                     id_num=ID_VAR,
                                     mysql_table=CITATION_TABLE_NAME,
                                     connection_info=ENGINE,
                                     country_df=country_df)
        foreignCourt = fc.insertForeignCourtsData(country_name=country,
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
        return case_res, softLaw, intlCourt, treaty, foreignCourt


COUNTRY_LIST = ['Australia', 'Austria', 'Botswana', 'Belgium', 'Canada',
                'Chile', 'Colombia', 'France', 'Germany', 'India',
                'Ireland', 'Lesotho', 'Malawi', 'Malaysia', 'New Zealand',
                'Nigeria', 'Papua New Guinea', 'Peru',
                'Philippines', 'South Africa', 'Spain', 'Switzerland',
                'Uganda', 'UK', 'USA', 'Zimbabwe']
INCOMPLETE_COUNTRIES = ['Latvia']
CITATION_TABLE_NAME = 'citations_test'
CASE_TABLE_NAME = 'case_info_test'
DATABASE_NAME = 'cglp'
PASSWORD = 'cglp'
REGEX_FOLDER = '/home/ec2-user/regex_tables/'
DATA_FOLDER = '/home/ec2-user/CGLP_Data'
softlaw_names, soft_law_regex_df = rt.createSoftLawRegexDf(folder_path=REGEX_FOLDER, file_name='softlaw_regex_20161003.csv')
intl_court_names, intl_court_regex_df = rt.createIntlCourtsRegexDf(folder_path=REGEX_FOLDER, file_name='intl_courts_regex_20161003.csv')
treaty_names, treaties_regex_df = rt.createTreatiesRegexDf(folder_path=REGEX_FOLDER, file_name='treaties_regex_20161003.csv')
fc_country_names, fc_court_names, fc_regex_df = rt.createForeignCourtsDf(folder_path=REGEX_FOLDER, file_name='foreign_courts_regex_20161007.csv')
country_df = rt.createCountryDf(folder_path=REGEX_FOLDER, file_name='country_ids_20161210.csv')
# connect to mysql server and create tables
helpers.createTables(DATABASE_NAME, PASSWORD, drop_table=True)
ENGINE = helpers.connectDb(DATABASE_NAME, PASSWORD)
BATCH_START = 1
error_log = []
pool = mp.Pool()
run_start_time = strftime("%d-%m-%Y %H:%M:%S", gmtime())
for country in COUNTRY_LIST:
    print country
    countryFiles = helpers.getCountryFiles(DATA_FOLDER, country)
    if country == 'USA':
        country = 'United States'
    if country == 'UK':
        country = 'United Kingdom'
    for year, folder in countryFiles.items():
        try:
            print year
            # data = list(zip(folder, list(range(BATCH_START, BATCH_START + len(folder)+1)), [year] * len(folder)))
            results = pool.map(insertData, list(zip(folder, [str(uuid.uuid4()) for i in range(len(folder))], [year] * len(folder), [country] * len(folder))))
            BATCH_START += len(folder)
            for result in results:
                if result is not None:
                    if result[0] is not None:
                        result[0].to_sql(name=CASE_TABLE_NAME, con=ENGINE,
                                         index=False, if_exists='append')
                    if result[1] is not None:
                        result[1].to_sql(name=CITATION_TABLE_NAME, con=ENGINE,
                                         index=False, if_exists='append')
                    if result[2] is not None:
                        result[2].to_sql(name=CITATION_TABLE_NAME, con=ENGINE,
                                         index=False, if_exists='append')
                    if result[3] is not None:
                        result[3].to_sql(name=CITATION_TABLE_NAME, con=ENGINE,
                                         index=False, if_exists='append')
                    if result[4] is not None:
                        result[4].to_sql(name=CITATION_TABLE_NAME, con=ENGINE,
                                         index=False, if_exists='append')
        except Exception, e:
            print e
            error_log.append('%s, %s, %s' % (country, file, e))
            pass
if len(error_log) > 0:
    start_time = strftime("%d-%m-%Y", gmtime())
    with open('/home/ec2-user/error_log_%s.csv' % (start_time), 'wb') as csvfile:
        wr = csv.writer(csvfile)
        wr.writerow(error_log)
run_end_time = strftime("%d-%m-%Y %H:%M:%S", gmtime())
print run_start_time, run_end_time
