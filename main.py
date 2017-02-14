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


def get_references(REGEX_FOLDER, DATA_FOLDER):
    # create regex tables just once
    softlaw_names, soft_law_regex_df = rt.createSoftLawRegexDf(folder_path=REGEX_FOLDER, file_name='softlaw_regex_20161003.csv')
    intl_court_names, intl_court_regex_df = rt.createIntlCourtsRegexDf(folder_path=REGEX_FOLDER, file_name='intl_courts_regex_20161003.csv')
    treaty_names, treaties_regex_df = rt.createTreatiesRegexDf(folder_path=REGEX_FOLDER, file_name='treaties_regex_20161003.csv')
    fc_country_names, fc_court_names, fc_regex_df = rt.createForeignCourtsDf(folder_path=REGEX_FOLDER, file_name='foreign_courts_regex_20161007.csv')
    country_df = rt.createCountryDf(folder_path=REGEX_FOLDER, file_name='country_ids_20161210.csv')

    # connect to mysql server
    ENGINE = helpers.connectDb(DATABASE_NAME, PASSWORD)
    ID_VAR = 1

    for country in COUNTRY_LIST:
        print country
        countryFiles = helpers.getCountryFiles(DATA_FOLDER, country)
        for year, folder in countryFiles.items():
            # go through dictionary of files and insert into mysql table
            for file in tqdm(folder):
                try:
                    caseInfo = cr.countryRefFunctions[country](file)
                    cr.insertCaseRefData(case_info=caseInfo,
                                         country_name=country,
                                         country_df=country_df,
                                         year=year,
                                         id=ID_VAR,
                                         mysql_table=CASE_TABLE_NAME,
                                         connection_info=ENGINE)
                    fileText = helpers.getFileText(file, html=False)
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
                    print e
                    pass


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
    COUNTRY_LIST = ['Australia', 'Austria', 'Botswana', 'Canada', 'Chile',
                    'Colombia', 'France', 'Germany', 'Ireland',
                    'Latvia', 'Lesotho', 'Malawi', 'Malaysia', 'New Zealand',
                    'Nigeria' 'Papua New Guinea', 'Peru',
                    'Philippines', 'South Africa', 'Switzerland', 'Uganda',
                    'UK', 'USA', 'Zimbabwe']
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
            country = a
    get_references(REGEX_FOLDER, DATA_FOLDER)
