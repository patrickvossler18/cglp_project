import sys
import helpers
import regex_tables
import soft_law_citations
import intl_court_citations
import treaty_citations
import foreign_court_citations
from tqdm import tqdm

def main(args=None):
    COUNTRY_LIST = ['Austria','Australia','Botswana','Canada','Chile','Colombia','France','Germany' ,'Ireland']
    #create regex tables just once
    soft_law_regex_table,soft_law_regex_df = regex_tables.createSoftLawRegexDf(folder_path="/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/",file_name='softlaw_regex_20161003.csv')
    intl_court_regex_table,intl_court_regex_df = regex_tables.createIntlCourtsRegexDf(folder_path="/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/",file_name= 'intl_courts_regex_20161003.csv')
    treaties_regex_table,treaties_regex_df = regex_tables.createTreatiesRegexDf(folder_path="/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/",file_name= 'treaties_regex_20161003.csv')
    fc_regex_table,fc_regex_df = regex_tables.createForeignCourtsDf(folder_path="/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/",file_name='foreign_courts_regex_20161007.csv')

    #connect to mysql server
    TABLE_NAME = 'citations'
    DATABASE_NAME ='cglp'
    PASSWORD = 'Measha4589$'
    ENGINE = helpers.connectDb(DATABASE_NAME,PASSWORD)

    for country in COUNTRY_LIST:
        print country
        countryFiles = helpers.getCountryFiles("/Users/patrick/Dropbox/Fall 2016/SPEC/CGLP Data",country)
        for year, folder in countryFiles.items():
            #go through dictionary of files and insert into mysql table
            for file in tqdm(folder):
                soft_law_citations.insertSoftLawData(country,year,file,soft_law_regex_df,soft_law_regex_table,TABLE_NAME,ENGINE)
                intl_court_citations.insertIntlCourtData(country,year,file,intl_court_regex_df,intl_court_regex_table,TABLE_NAME,ENGINE)
                treaty_citations.insertTreatyData(country,year,file,treaties_regex_df,treaties_regex_table,TABLE_NAME,ENGINE)
                foreign_court_citations.insertForeignCourtsData(country,year,file,fc_regex_df,fc_regex_table,TABLE_NAME,ENGINE)

if __name__ == "__main__":
    main()