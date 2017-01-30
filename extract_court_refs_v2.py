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
from dateutil.parser import parse
import translations
import helpers

'''
To add:
Filename column
caseid
'''

countryRefFunctions = {
	'Austria': extractAustriaCourtReferences,
	'Australia':extractAustraliaCourtReferences,
	'Botswana': extractBotswanaCourtReference,
	'Canada': extractCanadaCourtReference,
	'Chile': extractChileCourtReferences,
	'Colombia': extractColombiaCourtReferences,
	'France': extractFranceCourtReferences,
	'Germany' : extractGermanyCourtReferences,
	'Ireland': extractIrelandCourtReferences
}



def processCountryFiles(country_name_file_dict,extraction_function):		
	country_data = {}
	for year,folder in year_folders.items():
		cases = []
		for file in folder:
			# fileText = helpers.getFileText(file,html=False)
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
		print e
		raise

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
		print e
		raise		


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
		print e
		raise

def extractCanadaCourtReference(file_path):
	try:
		file_content = helpers.getFileText(file_path,html=False)
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
		print e
		raise

def extractChileCourtReferences(file_path):
	try:
		file_content = helpers.getFileText(file_path,html=False)
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
		file_content = helpers.getFileText(file_path,html=False)
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
		file_content = helpers.getFileText(file_path,html=False)
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
		file_content = helpers.getFileText(file_path,html=False)
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
		file_content = helpers.getFileText(file_path,html=False)
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

def extractLatviaCourtReferences(file_path):
	'''
	use datefinder for getting date of decision
	case Id in format No.YYYY-##-####
	participants name: reviewed the case "Case Title here"
	'''
	try:
		file_content = helpers.getFileText(file_path,html=False)
		html_text = unicode(lh.fromstring(file_content).text_content())
		pass
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractLesothoCourtReferences(file_path):
	'''
	TO DO: method for getting case id does not work for cases with multiple parens
	ex. (C OF A (CIV/APN/308/01))
	'''
	try:
		file_content = helpers.getFileText(file_path,html=False)
		html_text = lh.fromstring(file_content)
		title_text = html_text.find(".//h1[@class='title']")
		ParticipantName = ''
		CaseId = ''
		DecisionDate = ''
		if title_text is not None:	
			title_text = title_text.text_content().strip()
			split_text = title_text.upper().split(" V ")
			if len(split_text) > 1:
				ParticipantName = split_text[0].strip()
				extractedelements = split_text[len(split_text)-1].split('(')
				ParticipantName += ' v '+extractedelements[0].strip()
				case_match = re.search(r'\((.*?)\)',split_text[1])
				if case_match is not None:
					CaseId = case_match.group(1)
				# CaseId = extractedelements[1].replace(")",'')
		date_text = html_text.find(".//span[@class='date-display-single']")
		if date_text is not None:
			date_text = date_text.text_content()
			date = parse(date_text)
			day = '%02d' % date.day
			month = '%02d' % date.month
			year = str(date.year)
			DecisionDate = day + ' ' +month+ ' '+year
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractMalawiCourtReferences(file_path):
	ParticipantName = ''
	CaseId = ''
	DecisionDate = ''
	try:
		file_content = helpers.getFileText(file_path,html=False)
		if file_content:
			html_text = lh.fromstring(file_content)
			title_text = html_text.find(".//h1[@class='title']")
			if title_text is not None:
				ParticipantName = title_text.text_content().strip()
			caseidPattern = re.compile("\\[\\d{4}\\]\\s\\bMWSC\\s\\d+", re.IGNORECASE)
			caseidString = caseidPattern.findall(html_text.text_content())
			if len(caseidString) > 0:
				CaseId = caseidString[0]
			decisionDateNode = html_text.find('.//div[@class="field-item odd"]/span')
			# if decisionDateNode.text_content():
			if decisionDateNode is not None:
				DecisionDate = decisionDateNode.text_content().split(",")[1].strip()
	except Exception, e:
		print e
		raise
	return CaseId,DecisionDate,ParticipantName

def extractNewZealandCourtReferences(file_path):
	ParticipantName = ''
	CaseId = ''
	DecisionDate = ''
	try:
		file_content = helpers.getFileText(file_path,html=False)
		if file_content:
			html_text = lh.fromstring(file_content)
			title_text = html_text.find(".//h2[@class='make-database']")
			if title_text is not None:
				full_string = title_text.text_content().strip()
				participantsEnd_idx = full_string.find('[')
				if participantsEnd_idx > -1:
					ParticipantName = full_string[0:participantsEnd_idx-1].strip()
				dateStart_idx = full_string.find('(')
				dateEnd_idx = full_string.find(')')
				if dateStart_idx > -1 and dateEnd_idx > -1:
					DecisionDate = full_string[dateStart_idx+1:dateEnd_idx]
				caseIdPatternString = re.compile("SC [0-9]+|CRI [0-9]+")
				caseIdMatcher = caseIdPatternString.findall(full_string)
				if len(caseIdMatcher) > 0:
					CaseId = caseIdMatcher[0]
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractPapaNewGuineaCourtReferences(file_path):
	ParticipantName = ''
	CaseId = ''
	DecisionDate = ''
	try:
		file_content = helpers.getFileText(file_path,html=False)
		if file_content:
			html_text = lh.fromstring(file_content)
			title_text = html_text.find(".//h2[@class='make-database']")
			if title_text is not None:
				full_string = title_text.text_content().strip()
			participantsEnd_idx = full_string.find('[')
			if participantsEnd_idx > -1:
				ParticipantName = full_string[0:participantsEnd_idx-1].strip()
			dateStart_idx = full_string.find('(')
			dateEnd_idx = full_string.find(')')
			if dateStart_idx > -1 and dateEnd_idx > -1:
				DecisionDate = full_string[dateStart_idx+1:dateEnd_idx]
			#Older cases use PGSC but newer cases use SC### need check for if there is SC only and if SC only use that case id
			oldCaseIdPatternString = re.compile("PGSC [0-9]+|CRI [0-9]+")
			newCaseIdPatternString = re.compile("SC[0-9]+")
			oldCaseIdMatcher = oldCaseIdPatternString.findall(full_string)
			newCaseIdMatcher = newCaseIdPatternString.findall(full_string)
			if len(oldCaseIdMatcher) > 0 and len(newCaseIdMatcher) == 0:
				CaseId = oldCaseIdMatcher[0]
			if len(oldCaseIdMatcher) > 0 and len(newCaseIdMatcher) > 0:
				CaseId = newCaseIdMatcher[0]
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise

def extractPeruCourtReferences(file_path):
	ParticipantName = ''
	CaseId = ''
	DecisionDate = ''
	try:
		file_content = helpers.getFileText(file_path,html=False)
		if file_content:
			html_text = lh.fromstring(file_content)
			title_text = html_text.find(".//title")
			if title_text is not None:
				CaseId = title_text.text_content()
			all_ps = html_text.findall(".//p")
			extractedText = ''
			for para in all_ps:
				if "Lima" in para.text_content() or "En Arequipa" in para.text_content():
					extractedText = para.text_content().lower()
				participantString = para.text_content().find('Caso')
				if participantString == -1:
					participantString = para.text_content().find('CASO')
				if participantString != -1:
					ParticipantName = para.text_content().strip()
			if len(ParticipantName) > 0:
				ParticipantName = ParticipantName.upper().replace('CASO','').replace(':','')
			if len(extractedText) > 4:
				spanishDate = extractedText.strip()
				splitDate = spanishDate.split(',')[1]
				year = ''
				if "de dos mil" in splitDate:
					startIndex = splitDate.index('de dos mil')
					yearString = splitDate[startIndex:]
					yearString = yearString.replace('.','')
					if yearString in translations.spanishtoEngDosMil.keys():
						year = translations.spanishtoEngDosMil.get(yearString)
					splitDate = splitDate.replace(yearString,'')	
				if "de mil" in splitDate:
					startIndex = splitDate.index('de mil')
					yearString = splitDate[startIndex:]
					yearString = yearString.replace('.','')
					if yearString in translations.spanishtoEngDeMil.keys():
						year = translations.spanishtoEngDeMil.get(yearString)
					splitDate = splitDate.replace(yearString,'')
				month = ''
				for key,value in translations.spanishtoEngMonths.items():
					if key.decode('utf-8') in splitDate:
						month = value
						monthString = key
						splitDate = splitDate.replace(monthString,'')
						break
				splitDate = splitDate.replace("de", "")
				splitDate = splitDate.replace(".", "")
				splitDate = splitDate.replace(" ","")
				day = ''
				for key,value in translations.spanishtoEngNum.items():
					if key.decode('utf-8') in splitDate:
						day = value
						break
				DecisionDate = day+" "+month+" "+year
			else:
				DecisionDate = None
		return CaseId,DecisionDate,ParticipantName
	except Exception, e:
		print e
		raise






for year,folder in files.items():
		for file in folder:
			extractPeruCourtReferences(file)

