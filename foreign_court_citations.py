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
from sqlalchemy import create_engine
import helpers
import regex_tables

def findallForeignCourtMatches(findAllInstances,text,search_terms,term_idx=0,court_idx=2):
    '''
    What to do about self references? Keep or skip?
    Goes through each country in regex table and returns the index of the matches as a tuple
    If there are matches, get +/- 10 characters (or words?) around the result and check if other term present
    For +/- 10 words use r"(?i)((?:\S+\s+){0,10})\b" + re.escape(search_term)+ r"\b\s*((?:\S+\s+){0,10})"
    '''
    matched_results = []
    results = []
    #Go through search terms and find the indicies of all matches for each term
    for row in search_terms:
        search_term = row[term_idx]
        matches = list(helpers.findAllInstances(text,search_term))
        if matches:
            matched_results.append([row,matches])
    # If we have matched results, get the 10 words around the country name and see if the court name is there
    if len(matched_results) > 0:
        for row in matched_results:
            country_name = row[0][0]
            country_string = re.compile(r"(?i)((?:\S+\s+){0,10})\b" + re.escape(country_name)+ r"\b\s*((?:\S+\s+){0,10})",re.IGNORECASE)
            court_names = row[0][1]
            match_locations = row[1]
            for court_name in court_names:
                court_string = re.compile( r'\b%s\b' % court_name,re.IGNORECASE)
                for loc in match_locations:
                    buffer_area = 500+len(country_name)
                    lower_bound = loc - buffer_area
                    if (loc- buffer_area) < 0:
                        lower_bound =  0
                    upper_bound = loc + buffer_area
                    search_area = text[lower_bound:upper_bound]
                    context = country_string.search(search_area)
                    if context:
                        context_string = context.group()
                        find_court = court_string.search(context_string)
                        if find_court:
                            match = [country_name,court_name,context_string]
                            results.append(match)
    return results

def getForeignCourtsData(text,regex_df,regex_list,country_name=None,year=None):
    '''
    Inputs:
        text: raw string or parsed html
        country_name: optional for now but will be used if excluding self-references
        year: optional
        regex_df: citation info for each country in dataframe form
        regex_list: list of lists of search terms and court names
    '''
    #Store matches in list
    regex_ct_results = findallForeignCourtMatches(helpers.findAllInstances,text,regex_list,0,2)
    if regex_ct_results:
        #If there are matches, reshape the results into a dataframe, merge with regex_df and clean up for inserting into table
        def extract_key(v):
            return [v[0],v[1]]
        data = sorted(regex_ct_results, key=extract_key)
        regex_data = [ [k,[x[2] for x in g]] for k, g in itertools.groupby(data, extract_key)]
        regex_dict = dict((tuple(row[0]),row[1]) for row in regex_data)
        dataset = pd.Series(regex_dict)
        dataset = pd.DataFrame(dataset, columns = ['matches'])
        merged_results = pd.merge(dataset,regex_df,right_index=True,left_index=True).reset_index()
        merged_results = pd.concat([pd.DataFrame(dict(zip(merged_results.columns,merged_results.ix[i]))) for i in range(len(merged_results))])
        merged_results = merged_results.rename(columns={'matches': 'context'})
        merged_results.drop(['level_1','level_0'],inplace=True,axis=1)
        merged_results['year'] = year
        # merged_results = merged_results.rename_axis(None)
        return merged_results
    else:
        return pd.DataFrame()

def insertForeignCourtsData(country_name,year,file,regex_df,regex_table,mysql_table,connection_info):
    '''
    Inputs:
        country_name: name of the source country
        file: a string of the file path to the decision
        regex_df: foreign courts regex dataframe used to merge in metadata information for matches 
        regex_table: foreign courts regex table in list form used to find matches
        mysql_table: mysql table name to insert matches into
        connection_info: mysql table connection info
    Output:
        None, inserts matches into mysql table
    '''     

    fileText = helpers.getFileText(file,html=False)
    try:
        foreignCourts = getForeignCourtsData(text=fileText,country_name=country_name,year=year,regex_df=regex_df,regex_list=regex_table)
        if not foreignCourts.empty:
            foreignCourts.to_sql(name=mysql_table,con=connection_info,flavor='mysql',index=False,if_exists='append')
    except Exception, e:
        print e
        raise


# #create foreign courts regex tables
# regex_table,regex_df = createForeignCourtsDf(folder_path="/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/",file_name='foreign_courts_regex_20161007.csv')
# #connect to mysql server
# table_name = 'citations'
# password = 'Measha4589$'
# engine = helpers.connectDb(table_name,password)
# #create dictionary of file paths
# countryFiles = helpers.getCountryFiles("/Users/patrick/Dropbox/Fall 2016/SPEC/CGLP Data","Australia")
# #go through dictionary of files and insert into mysql table
# insertForeignCourtsData("Australia",countryFiles,regex_df,regex_table,table_name,engine)


