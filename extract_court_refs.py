import PyPDF2
import re
import operator
import os
from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup
import lxml.html as lh
import codecs
import translations


'''
To add:
Filename column
caseid
'''

countryFiles = readCountryFiles("/Users/patrick/Dropbox/Fall 2016/SPEC/CGLP Data","Colombia")



def getFileText(file_path,html=False):
	'''
	input: string of file path
	output: either raw string or parsed html text content
	'''
	file_content = open(file_path).read()
	if html:
		html_text = lh.fromstring(file_content).text_content()
		return html_text
	else:
		return file_content

def getCountryFiles(folder_path,country_name):
	'''
	input: path to folder with text files and name of the country
	output: dictionary with year as key and values are the file paths for the documents in that year
	Notes: updated to walk through the entire path to get files in subfolders as well. Doesn't check that files are html though.
	'''
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
			# year_folders[year] = [join(path,file) for file in listdir(path) if isfile(join(path, file))]
			# year_folders[year] = [val for sublist in [[os.path.join(i[0], j) for j in i[2]] for i in os.walk(path)] for val in sublist]
			year_folders[year] = [val for sublist in [[os.path.join(i[0], j) for j in i[2]] for i in os.walk(path)] for val in sublist if '.DS_Store' not in val]  
	return year_folders

def processCountryFiles(country_name_file_dict,extraction_function):		
	country_data = {}
	for year,folder in year_folders.items():
		cases = []
		for file in folder:
			# fileText = getFileText(file,html=False)
			court_info = extraction_function(file)
			#insert info into mysql table
		country_data[(country_name,year)] = cases	
	return country_data


def extractAustriaCourtReferences(file_path):
	try:
		file_content = open(file_path).read()
		soup = BeautifulSoup(file_content, "html.parser")
		html_text = soup.get_text()
		CaseId = soup.find("p.ErlText.AlignJustify")[2].get_text()
		DecisionDate = soup.find("p.ErlText.AlignJustify")[1].get_text()
		ParticipantsName = None
		return CaseId,DecisionDate,ParticipantsName
	except Exception, e:
		print "Error parsing html file: " + file_path
		print e

def extractAustraliaCourtReferences(file_path):
	try:
		file_content = open(file_path).read()
		soup = BeautifulSoup(file_content, "html.parser")
		html_text = soup.get_text()
		elements = soup.find('h2').get_text().split(';')
		caseAndDate = elements[len(elements)-1]
		index = len(caseAndDate)-1
		while(caseAndDate[index] != '('):
		    index = index - 1
		endIndex = index
		while(caseAndDate[endIndex] != ')'):
		    endIndex += 1
		CaseId = caseAndDate[0:index-1].replace('\r\n', '').strip(' ')
		DecisionDate = caseAndDate[index+1:endIndex].replace('\r\n', '').strip(' ')
		ParticipantsName = elements[0]
		return CaseId,DecisionDate,ParticipantsName
	except Exception, e:
		print "Error parsing html file: " + file_path
		print e		


def extractBotswanaCourtReference(file_path):
	try:
		file_content = open(file_path).read()
		soup = BeautifulSoup(file_content, "html.parser")
		html_text = soup.get_text()
		textToBeExtracted = soup.find("title").get_text()
		pattern = re.compile("\\((.*?)\\)")
		matched_array = pattern.findall(textToBeExtracted)
		if len(matched_array) > 0:
			firstIndex = textToBeExtracted.find(matched_array[len(matched_array)-2])
			if firstIndex == -1:
				ParticipantName = textToBeExtracted
			else:
				ParticipantName = textToBeExtracted[0:firstIndex-1].strip()
	    	DecisionDate = matched_array[len(matched_array) - 1]
	    	CaseId = matched_array[len(matched_array) - 2]
		else:
			ParticipantName = textToBeExtracted
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print "Error parsing html file: " + file_path
		print e

def extractCanadaCourtReference(file_path):
	try:
		file_content = getFileText(file_path,html=False)
		# file_content = open(file_path).read()
		soup = BeautifulSoup(file_content, "html.parser")
		html_text = soup.get_text()
		metadata = soup.find("div", { "class" : "metadata" })
		tablerows = metadata.findAll('tr')
		court_data = []
		for row in tablerows:
			info = row.find('td', {"class" : "metadata"}).get_text().strip()
			court_data.append(info)
		ParticipantName = court_data[0]
		DecisionDate = court_data[2]
		CaseId = court_data[3]
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print "Error parsing html file: " + file_path
		print e

def extractChileCourtReferences(file_path):
	try:
		file_content = getFileText(file_path,html=False)
		# file_content = codecs.open(file_path, "r", "utf-8").read()
		# # file_content = open(file_path).read().decode('string-escape').decode('utf-8')
		caseidString = re.compile("\<b\>role?s?\s.*\<\/b\>", re.IGNORECASE)
		caseid_matches = caseidString.findall(file_content)
		if len(caseid_matches) < 1:
			caseidString = re.compile("rol.*\d\<\/p\>", re.IGNORECASE)
			caseid_matches = caseidString.findall(file_content)
		if len(caseid_matches) < 1:
			caseidString = re.compile("\<b\>role?s?\s.*[\s\S]*?\<\/b\>", re.IGNORECASE)
			caseid_matches = caseidString.findall(file_content)
		if len(caseid_matches) > 0:
			CaseId = lh.fromstring(caseid_matches[0]).text_content().encode('utf-8')
			CaseId = CaseId.replace(".","")
			CaseId = CaseId.replace(',',"")
			CaseId = CaseId.replace('\n',' ')
			CaseId = CaseId.strip(' ')
			# CaseId = CaseId.replace('Rol N\xc3\x82\xc2\xb0','')			
		else:
			CaseId = None
		dateString = re.compile("\\bSantiago\,\s.*?[\s\S]*?\.")
		spanishDate = dateString.findall(file_content)
		if len(spanishDate) > 0:
			spanishDate = lh.fromstring(spanishDate[0]).text_content()
			spanishDate = spanishDate.replace('\n',' ')
			year = ''
			if "de dos mil" in spanishDate:
				startIndex = spanishDate.index('de dos mil')
				yearString = spanishDate[startIndex:]
				yearString = yearString.replace('.','')
				if yearString in translations.spanishtoEngDosMil.keys():
					year = translations.spanishtoEngDosMil.get(yearString)
				spanishDate = spanishDate.replace(yearString,'')	
			if "de mil" in spanishDate:
				startIndex = spanishDate.index('de mil')
				yearString = spanishDate[startIndex:]
				yearString = yearString.replace('.','')
				if yearString in translations.spanishtoEngDeMil.keys():
					year = translations.spanishtoEngDeMil.get(yearString)
				spanishDate = spanishDate.replace(yearString,'')
			month = ''
			for key,value in translations.spanishtoEngMonths.items():
				if key.decode('utf-8') in spanishDate:
					month = value
					monthString = key
					spanishDate = spanishDate.replace(monthString,'')
					break
			spanishDate = spanishDate.replace("de", "")
			spanishDate = spanishDate.replace(".", "")
			spanishDate = spanishDate.replace(" ","")
			day = ''
			for key,value in translations.spanishtoEngNum.items():
				if key.decode('utf-8') in spanishDate:
					day = value
					break
			DecisionDate = day+" "+month+" "+year
		else:
			DecisionDate = None
		ParticipantName = None
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractColombiaCourtReferences(file_path):
	'''
	Need to handle edge cases for case id and date
	'''
	try:
		# caseidString = re.compile("\\bSentencia\\b.*\-\d*\<span", re.IGNORECASE)
		file_content = getFileText(file_path,html=False)
		# dateString = re.compile("\\bD\.?C\.?,.*\d{4}")
		# dateString = re.compile("\\bD\.\s?C\.?\,.*[\s\S]*?.*\d{4}[,|:|;|\\?|!|)|(]")
		html_text = lh.fromstring(file_content)
		spans = html_text.findall('.//span[@lang]')
		spanishDate = ''
		CaseId = ''
		for span in spans:
			text = i.text_content()
			caseidString = re.compile("\>?\\bSentencia\\b[\s\S]*?.*\d{1}", re.IGNORECASE)
			# caseidString = re.compile("\>?\\bSentencia\\b\sC\-\d+\/\d{2}", re.IGNORECASE)
			caseMatch = caseidString.findall(text)
			if len(caseMatch) > 0:
				CaseId = caseMatch[0]
				break
			dateString = re.compile("\\bD\.\s?C\.?\,.*[\s\S]*?.*\d{4}[,|:|;|\\?|!|)|(]")
			dateMatch = dateString.findall(text)
			if len(dateMatch) > 0:
				spanishDate = dateMatch[0]
				break
			dateString = re.compile("\\bBogot\\xe1\\b[\s\S]*?\d{4}")
			dateMatch = dateString.findall(text)
			if len(dateMatch) > 0:
				spanishDate = dateMatch[0]
				break
		if len(CaseId) == 0:
			# caseidString = re.compile("\>?\\bSentencia\\b[\s\S]*?\d{3}\/\d{2}", re.IGNORECASE)
			caseidString = re.compile("\>?\\bSentencia\\b[\s\S]*?.*\d{1}", re.IGNORECASE)
			# caseidString = re.compile("\\bSentencia\\b[\s\S]*?.*\d{1}",re.IGNORECASE)
			caseid_matches = caseidString.findall(file_content)
			if len(caseid_matches) > 0:
				CaseId = lh.fromstring(caseid_matches[0]).text_content().encode('utf-8')
				CaseId = CaseId.replace(">","")
				CaseId = CaseId.replace("\r",'')
				CaseId = CaseId.replace("\n", " ")
		if len(spanishDate) == 0:
			dateString = re.compile("\\bD\.\s?C\.?\,.*[\s\S]*?.*\d{4}[,|:|;|\\?|!|)|(]")
			dateMatch = dateString.findall(file_content)
			if len(dateMatch) > 0:
				spanishDate = lh.fromstring(dateMatch[0]).text_content()
			dateString = re.compile("\\bBogot.*\\b[\s\S]*?\d{4}")
			dateMatch = dateString.findall(file_content)
			if len(dateMatch) > 0:
				spanishDate = lh.fromstring(dateMatch[0]).text_content()		
		dayPattern = re.compile("\\b\\d{1,2}\\b")
		dayMatcher = dayPattern.findall(spanishDate)
		day = ''
		if len(dayMatcher) > 0:
			day = dayMatcher[0]
		else:
			for key, value in translations.spanishtoEngNum.items():
				if key.decode('utf-8') in spanishDate:
					day = value
					dayString = key
					spanishDate = spanishDate.replace(dayString,'')
		spanishDate = spanishDate.replace(",","")
		spanishDate = spanishDate.replace(")","")
		spanishDate = spanishDate.replace("(","")
		year = spanishDate[len(spanishDate)-4:len(spanishDate)]
		month = ''
		for key,value in translations.spanishtoEngMonths.items():
			if key.decode('utf-8') in spanishDate:
				month = value
				monthString = key
				spanishDate = spanishDate.replace(monthString,'')
				break
		DecisionDate = day+' '+month+' '+year
		ParticipantName = None
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractFranceCourtReferences(file_path):
	try:
		file_content = getFileText(file_path,html=False)
		html_text = lh.fromstring(file_content)
		extractedText = html_text.find('.//title').text_content().strip()
		data = extractedText.split('du')
		num = 'n°'
		splitIndex = data[0].index(num.decode('utf-8'))+ len(num)
		decisionNumber = data[0][splitIndex:]
		decisionNumber = re.sub('[a-zA-z]', '', decisionNumber).strip()
		frenchDate = data[1].strip()
		dayPattern = re.compile("\\b\\d{1,2}\\b")
		dayMatcher = dayPattern.findall(frenchDate)
		day = ''
		if len(dayMatcher) > 0:
			day = dayMatcher[0]
			frenchDate = frenchDate.replace(day,'')
		year = ''
		year = frenchDate[len(frenchDate)-4:len(frenchDate)]	
		for key, value in translations.frenchtoEngMonth.items():
				if key.decode('utf-8') in frenchDate:
					month = value
					break
		DecisionDate = day+' '+month+' '+year
		CaseId = decisionNumber
		ParticipantName = None
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractGermanyCourtReferences(file_path):
	try:
		file_content = getFileText(file_path,html=False)
		html_text = lh.fromstring(file_content)
		extractedText = html_text.find(".//p[@class='zitierung']")
		if extractedText:
			extractedText = extractedText.text_content().strip()
			if "Citation" in extractedText:
				data = extractedText.split(',')
				if " du " in extractedText:
					info = data[1].split(" du ")
					CaseId = info[0].replace(' ','')
					day,month,year= info[1].strip().split('.')
					for key, value in translations.numberToMonth.items():
						if key.decode('utf-8') in month:
							month = value
							break	
				if " of " in extractedText:
					info = data[1].split(" of ")
					CaseId = info[0].replace(' ','')
					month,day,year= info[1].strip().split('/')
					for key, value in translations.numberToMonth.items():
						if key.decode('utf-8') in month:
							month = value
							break
			else:
				data = extractedText.split(' ')
				CaseId = data[2]+data[3]+data[4]
				splitDate = data[6].split('.')
				splitDate[2] = splitDate[2].replace(",", "")
				day = splitDate[0]
				month = splitDate[1]
				year = splitDate[2]
				for key, value in translations.numberToMonth.items():
						if key.decode('utf-8') in month:
							month = value
							break
			DecisionDate = day+' '+month+' '+year
		else:
			CaseId = None
			DecisionDate = None
		ParticipantName = None
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
			print e
			raise

def extractIrelandCourtReferences(file_path):
	try:
		file_content = getFileText(file_path,html=False)
		html_text = unicode(lh.fromstring(file_content).text_content())
		ParticipantName = ''
		CaseId = ''
		DecisionDate = ''
		match_idx = html_text.find('Judgment Title:')
		if match_idx != -1:
			match_string = html_text[match_idx:match_idx+1000]
			match_split = match_string.split('\n')
			for match in match_split:
				if 'Judgment Title:' in match:
					ParticipantName = match.replace('Judgment Title: ','').strip()
				if 'Supreme Court Record Number:' in match:
					CaseId = match.replace('Supreme Court Record Number: ','').strip()
				if 'Date of Delivery: ' in match:
					dateString = match.replace('Date of Delivery: ','').strip()
					date_split = dateString.split('/')
					month = date_split[0]
					day = date_split [1]
					year = date_split[2]
					for key, value in translations.numberToMonth.items():
						if key.decode('utf-8') in month:
							month = value
							break
					DecisionDate = day+' '+month+' '+year
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise






