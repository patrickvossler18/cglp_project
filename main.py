import helpers
import regex_tables as rt
import citation_data as cd
import extract_court_refs as cr
from time import gmtime, strftime
import csv
# import multiprocessing as mp
import pathos.multiprocessing as mp
import uuid
import getopt
import sys


def getReferences(REGEX_FOLDER, DATA_FOLDER, pool):

    def insertData(file_info):
        file = file_info[0]
        ID_VAR = file_info[1]
        year = file_info[2]
        country = file_info[3]
        try:
            if country == 'United Kingdom':
                            for function in cr.countryRefFunctions[country]:
                                case = function(file)
                                if case is not None:
                                    caseInfo = case
                                    # break
            else:
                caseInfo = cr.countryRefFunctions[country](file)
            if caseInfo is not None:
                case_res = cr.getCaseRefData(case_info=caseInfo,
                                             country_name=country,
                                             country_df=country_df,
                                             year=year,
                                             id=ID_VAR,
                                             source_file=file)
            # Fix for pdfs because esm only accepting strings, not unicode
            fileText = helpers.getFileText(file, html=True, pdf_utf8=False)
            if fileText is not None:
                softLaw = cd.getCitationData(country_name=country, year=year,
                                             file=file, text=fileText,
                                             regex_df=soft_law_regex_df,
                                             ref_terms=softlaw_names,
                                             id_string=ID_VAR,
                                             country_df=country_df)
                intlCourt = cd.getCitationData(country_name=country,
                                               year=year,
                                               file=file,
                                               text=fileText,
                                               regex_df=intl_court_regex_df,
                                               ref_terms=intl_court_names,
                                               id_string=ID_VAR,
                                               country_df=country_df)
                treaty = cd.getCitationData(country_name=country,
                                            year=year,
                                            file=file,
                                            text=fileText,
                                            regex_df=treaties_regex_df,
                                            ref_terms=treaty_names,
                                            id_string=ID_VAR,
                                            country_df=country_df)
                foreignCourt = cd.getForeignCourtsData(country_name=country,
                                                          year=year,
                                                          file=file,
                                                          text=fileText,
                                                          regex_df=fc_regex_df,
                                                          country_names=fc_country_names,
                                                          court_names=fc_court_names,
                                                          id_string=ID_VAR,
                                                          country_df=country_df)
                return case_res, softLaw, intlCourt, treaty, foreignCourt
        except Exception as e:
            raise Exception(repr(e), file_info[0])

    softlaw_names, soft_law_regex_df = rt.createSoftLawRegexDf(folder_path=REGEX_FOLDER, file_name='softlaw_regex_20161003.csv')
    intl_court_names, intl_court_regex_df = rt.createIntlCourtsRegexDf(folder_path=REGEX_FOLDER, file_name='intl_courts_regex_20161003.csv')
    treaty_names, treaties_regex_df = rt.createTreatiesRegexDf(folder_path=REGEX_FOLDER, file_name='treaties_regex_20161003.csv')
    fc_country_names, fc_court_names, fc_regex_df = rt.createForeignCourtsDf(folder_path=REGEX_FOLDER, file_name='foreign_courts_regex_20161007.csv')
    country_df = rt.createCountryDf(folder_path=REGEX_FOLDER, file_name='country_ids_20161210.csv')

    # connect to mysql server and create tables
    helpers.createTables(DATABASE_NAME, PASSWORD, drop_table=True)
    ENGINE = helpers.connectDb(DATABASE_NAME, PASSWORD)
    error_log = []
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
                results = pool.map(insertData, list(zip(folder, [str(uuid.uuid4()) for i in range(len(folder))], [year] * len(folder), [country] * len(folder))))
                for result in results:
                    if isinstance(result, Exception):
                        print "Error: %s" % result
                    else:
                        if result is not None:
                            if result[0] is not None:
                                result[0].to_sql(name=CASE_TABLE_NAME,
                                                 con=ENGINE,
                                                 index=False,
                                                 if_exists='append')
                            if result[1] is not None:
                                result[1].to_sql(name=CITATION_TABLE_NAME,
                                                 con=ENGINE,
                                                 index=False,
                                                 if_exists='append')
                            if result[2] is not None:
                                result[2].to_sql(name=CITATION_TABLE_NAME,
                                                 con=ENGINE,
                                                 index=False,
                                                 if_exists='append')
                            if result[3] is not None:
                                result[3].to_sql(name=CITATION_TABLE_NAME,
                                                 con=ENGINE,
                                                 index=False,
                                                 if_exists='append')
                            if result[4] is not None:
                                result[4].to_sql(name=CITATION_TABLE_NAME,
                                                 con=ENGINE,
                                                 index=False,
                                                 if_exists='append')
            except Exception, e:
                print e
                error_log.append('%s, %s' % (country, e))
                break

    if len(error_log) > 0:
        start_time = strftime("%d-%m-%Y", gmtime())
        with open('/home/ec2-user/error_log_%s.csv' % (start_time), 'wb') as csvfile:
            wr = csv.writer(csvfile)
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
    run_start_time = strftime("%d-%m-%Y %H:%M:%S", gmtime())
    pool = mp.Pool()
    getReferences(REGEX_FOLDER, DATA_FOLDER, pool)
    run_end_time = strftime("%d-%m-%Y %H:%M:%S", gmtime())
    print run_start_time, run_end_time
