import PyPDF2
import re
import operator
import os
from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup
import lxml.html as lh
import codecs
import csv
import pandas as pd
from tqdm import tqdm
import itertools
import esm
from sqlalchemy import create_engine
import helpers
import regex_tables


def findallTreatyMatches(text, treaty_names):
    '''
    For +/- 10 words use r"(?i)((?:\S+\s+){0,10})\b" + re.escape(search_term)+ r"\b\s*((?:\S+\s+){0,10})"
    '''
    results = []
    matches = treaty_names.query(text)
    for match in matches:
        treaty_name = match[1]
        treaty_string = re.compile(r"(?i)((?:\S+\s+){0,10})\b" + re.escape(treaty_name)+ r"\b\s*((?:\S+\s+){0,10})",re.IGNORECASE)
        # use start of match
        match_location = match[0][0]
        buffer_area = 500 + len(treaty_name)
        lower_bound = (match_location) - buffer_area
        upper_bound = (match_location) + buffer_area
        if (match_location - buffer_area) < 0:
            lower_bound = 0
        if (match_location + buffer_area) > len(text):
            upper_bound = len(text)
        search_area = text[lower_bound:upper_bound]
        context = treaty_string.search(search_area)
        if context:
            context_string = context.group()
            match = [treaty_name, context_string]
            results.append(match)
    results = [list(x) for x in set(tuple(x) for x in results)]
    return results


def getTreatyData(text, regex_df, treaty_names, file, country_df,
                  country_name=None, year=None):
    '''
    Inputs:
        text: raw string or parsed html
        country_name: optional for now but will be used if excluding self-references
        year: optional
        regex_df: citation info for each country in dataframe form
        treaty_names: list of lists of search terms and court names
    '''
    regex_treaty_results = findallTreatyMatches(text, treaty_names)
    if regex_treaty_results:
        regex_dict = dict((row[0], row[1:]) for row in regex_treaty_results)
        dataset = pd.Series(regex_dict)
        dataset = pd.DataFrame(dataset, columns=['matches'])
        merged_results = pd.merge(dataset, regex_df, right_index=True, left_index=True).reset_index()
        merged_results = pd.concat([pd.DataFrame(dict(zip(merged_results.columns, merged_results.ix[i]))) for i in range(len(merged_results))])
        merged_results = merged_results.rename(columns={'matches': 'context'})
        merged_results['year'] = year
        merged_results['source_file_name'] = os.path.basename(file)
        ###UNTESTED####
        #Use country id dataframe to assign a source country id based upon the country name from the file structure
        merged_results['source_country_id'] = country_df.loc[country_name][0]
        merged_results = merged_results.rename_axis(None)
        merged_results.drop(['index'], inplace=True, axis=1)
        return merged_results
    else:
        return pd.DataFrame()


def insertTreatyData(country_name, year, file, fileText, regex_df,
                     treaty_names, mysql_table, connection_info, country_df):
    '''
    Inputs:
        country_name: name of the source country
        file_dict: a dictionary with the file path to each decision in
        format year:list of file paths
        regex_df: regex dataframe used to merge in metadata information
        for matches
        treaty_names: regex table in list form used to find matches
        mysql_table: mysql table name to insert matches into
        connection_info: mysql table connection info
    Output:
        None, inserts matches into mysql table
    '''
    try:
        treatyData = getTreatyData(text=fileText, country_name=country_name,
                                   year=year, regex_df=regex_df,
                                   treaty_names=treaty_names,
                                   file=file, country_df=country_df)
        if not treatyData.empty:
            treatyData.to_sql(name=mysql_table, con=connection_info,
                              index=False, if_exists='append')
    except Exception, error:
        print error
        raise

# #create regex tables
# treaty_names,regex_df = createTreatiesRegexDf(folder_path="/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/",file_name= 'treaties_regex_20161003.csv')
# #connect to mysql server
# table_name = 'citations'
# password = ''
# engine = helpers.connectDb(table_name,password)
# #create dictionary of file paths
# countryFiles = helpers.getCountryFiles("/Users/patrick/Dropbox/Fall 2016/SPEC/CGLP Data","Australia")
# #go through dictionary of files and insert into mysql table
# insertTreatyData("Australia",countryFiles,regex_df,treaty_names,table_name,engine)
