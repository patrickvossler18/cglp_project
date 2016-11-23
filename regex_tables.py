import PyPDF2
import re
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

def createSoftLawRegexDf(folder_path=None,file_name=None):
    #Get regex table
    filename = 'softlaw_regex_20161003.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+filename))
    for row in reader:
        regex_table.append(row)
    regex_test = dict((row[0],row[1:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df,columns = ['info'])
    regex_df[['citation_type', 'softlaw_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info',inplace=True,axis=1)
    return regex_table,regex_df

def createForeignCourtsDf(folder_path=None,file_name=None):
    #Get regex table
    filename = 'foreign_courts_regex_20161007.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+filename))
    for row in reader:
        regex_table.append(row)
    def extract_key(v):
        return v[0]
    data = sorted(regex_table, key=extract_key)
    regex_result = [ [k,[x[2] for x in g]] for k, g in itertools.groupby(data, extract_key)]
    regex_test = dict((tuple([row[0],row[2]]),row[3:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df,columns = ['info'])
    regex_df[['citation_type', 'country_id', 'court_code','court_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info',inplace=True,axis=1)
    return regex_result,regex_df

def createIntlCourtsRegexDf(folder_path=None,file_name=None):
    #Get regex table
    filename = 'intl_courts_regex_20161003.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+filename))
    for row in reader:
        regex_table.append(row)
    regex_test = dict((row[0],row[1:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df,columns = ['info'])
    regex_df[['citation_type', 'intl_ct_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info',inplace=True,axis=1)
    return regex_table,regex_df

def createTreatiesRegexDf(folder_path=None,file_name=None):
    #Get regex table
    filename = 'treaties_regex_20161003.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+filename))
    for row in reader:
        regex_table.append(row)
    regex_test = dict((row[0],row[1:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df,columns = ['info'])
    regex_df[['citation_type', 'treaty_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info',inplace=True,axis=1)
    return regex_table,regex_df