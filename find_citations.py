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
import MySQLdb as mysql
import MySQLdb.cursors

db = mysql.connect(host='127.0.0.1', port=3306, user='root', db='cglp')
c = db.cursor(MySQLdb.cursors.DictCursor)

def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
	#Skip headers
	next(utf8_data)
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def findAllInstances(text,term):
    """
    >>> text = "Allowed Hello Hollow"
    >>> tuple(findAllInstances('ll', text))
    (1, 10, 16)
    """
    index = 0 - len(term)
    text = text.lower()
    term = term.lower()
    try:
        while True:
            index = text.index(term, index + len(term))
            yield index
    except ValueError:
        pass

def findall_each(findAllInstances,text,search_terms,term_idx=0,court_idx=2):
	'''
	IGNORE CASE
	Goes through each country in regex table and returns the index of the matches as a tuple
	If there are matches, get +/- 10 characters (or words?) around the result and check if other term present
	For +/- 10 words use r"(?i)((?:\S+\s+){0,10})\b" + re.escape(search_term)+ r"\b\s*((?:\S+\s+){0,10})"
	'''
	matched_results = []
	results = []
	for row in search_terms:
		search_term = row[term_idx]
		court_name = row[court_idx]
		terms = [search_term,court_name]
		matched_results.append([terms,(list(findAllInstances(text,search_term)))])
	for row in tqdm(matched_results):
		if len(row[1]) > 0:
			country_name = row[0][0]
			court_name = row[0][1]
			matches = row[1]
			country_string = re.compile(r"(?i)((?:\S+\s+){0,10})\b" + re.escape(country_name)+ r"\b\s*((?:\S+\s+){0,10})",re.IGNORECASE)
			court_string = re.compile( r'\b%s\b' % court_name,re.IGNORECASE)
			for i,match in enumerate(matches):
				#Get context around country name
				buffer_area = 100+len(country_name)
				lower_bound = match - buffer_area
				if (match- buffer_area) < 0:
					lower_bound =  0
				upper_bound = match + buffer_area
				search_area = text[lower_bound:upper_bound]
				context = country_string.search(search_area)
				if context:
					context_string = context.group()
					find_court = court_string.search(context_string)
					if find_court:
						# matches[i] = [match,context_string]
						matches[i] = context_string
					else:
						matches[i] = None 
				else:
					matches[i] = None
		row[1] = [x for x in row[1] if x is not None]
		if len(row[1]) > 0:
			results.append(row)
	return results

def getFileText(file_path):
	file_content = open(file_path).read()
	# soup = BeautifulSoup(file_content, "html.parser")
	html_text = lh.fromstring(file_content).text_content()
	return html_text

def readCountryFiles(folder_path,country_name):
	full_path=  folder_path + '/' +country_name +'/'
	sub_folders = os.listdir(full_path)
	regex = re.compile("([A-Za-z])\w.*")
	year_folders = {}
	for folder in sub_folders:
		if folder != ".DS_Store":
			path = full_path+folder
			if regex.findall(folder):
				year = folder[-4:]
			else:
				year = folder	
			year_folders[year] = [join(path,file) for file in listdir(path) if isfile(join(path, file))] 
	country_text = {}
	for year,folder in year_folders.items():
		cases = []
		for file in folder:
			cases.append(getFileText(file))
		country_text[(country_name,year)] = cases	
	return country_text


'''
For each country:
	-Read all files in folder into list (dict?)
	-For each file in list/dict:
		-get court references
		-find citations
'''

### FOREIGN COURTS ###
def getForeignCourtsData(text,country_name,year,regex_df,regex_list):
	#Find Matches
	regex_ct_results = findall_each(findAllInstances,text,regex_table,0,2)
	test = dict((tuple(row[0]),row[1]) for row in regex_ct_results)
	dataset = pd.Series(test)
	dataset = pd.DataFrame(dataset, columns = ['matches'])
	merged_results = pd.merge(dataset,regex_df,right_index=True,left_index=True).reset_index()
	return merged_results

def createRegexDf(folder_path=None,file_name=None):
	folder_path= "/Users/patrick/Dropbox/Fall 2016/SPEC/Regex tables/"
	#Get regex table
	filename = 'foreign_courts_regex_20161007.csv'
	regex_table = []
	reader = unicode_csv_reader(open(folder_path+filename))
	for row in reader:
		regex_table.append(row)
	regex_test = dict((tuple([row[0],row[2]]),row[3:]) for row in regex_table)
	regex_df = pd.Series(regex_test)
	regex_df = pd.DataFrame(regex_df,columns = ['info'])
	regex_df[['citation_type', 'country_id', 'court_code','court_id']] = regex_df['info'].apply(pd.Series)
	regex_df.drop('info',inplace=True,axis=1)
	return regex_table,regex_df


regex_table,regex_df = createRegexDf()

countryFiles = readCountryFiles("/Users/patrick/Dropbox/Fall 2016/SPEC/CGLP Data","Australia")



for key, value in countryFiles.items():
	citations = []
	for text in tqdm(value):
		m = getForeignCourtsData(text=text,country_name=key[0],year=key[1],regex_df=regex_df,regex_list=regex_table)
		citations.append(m)
	countryFiles[key] = [value,citations]