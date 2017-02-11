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
    'Australia': extractAustraliaCourtReferences,
    'Botswana': extractBotswanaCourtReference,
    'Canada': extractCanadaCourtReference,
    'Chile': extractChileCourtReferences,
    'Colombia': extractColombiaCourtReferences,
    'France': extractFranceCourtReferences,
    'Germany': extractGermanyCourtReferences,
    'Ireland': extractIrelandCourtReferences
}


def processCountryFiles(country_name_file_dict, extraction_function):
    country_data = {}
    for year, folder in year_folders.items():
        cases = []
        for file in folder:
            # fileText = helpers.getFileText(file,html=False)
            court_info = extraction_function(file)
            #insert info into mysql table
        country_data[(country_name, year)] = cases
    return country_data


def extractAustriaCourtReferences(file_path):
    try:
        file_content = open(file_path).read()
        soup = BeautifulSoup(file_content, "html.parser")
        CaseId = soup.find("p.ErlText.AlignJustify")[2].get_text()
        DecisionDate = soup.find("p.ErlText.AlignJustify")[1].get_text()
        ParticipantsName = None
        return CaseId, DecisionDate, ParticipantsName
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
        return CaseId, DecisionDate, ParticipantsName
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
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractCanadaCourtReference(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
        # file_content = open(file_path).read()
        soup = BeautifulSoup(file_content, "html.parser")
        html_text = soup.get_text()
        metadata = soup.find("div", {"class": "metadata"})
        tablerows = metadata.findAll('tr')
        court_data = []
        for row in tablerows:
            info = row.find('td', {"class" : "metadata"}).get_text().strip()
            court_data.append(info)
        ParticipantName = court_data[0]
        DecisionDate = court_data[2]
        CaseId = court_data[3]
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractChileCourtReferences(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
            CaseId = CaseId.replace(".", "")
            CaseId = CaseId.replace(',', "")
            CaseId = CaseId.replace('\n', ' ')
            CaseId = CaseId.strip(' ')
            # CaseId = CaseId.replace('Rol N\xc3\x82\xc2\xb0','')
        else:
            CaseId = None
        dateString = re.compile("\\bSantiago\,\s.*?[\s\S]*?\.")
        spanishDate = dateString.findall(file_content)
        if len(spanishDate) > 0:
            spanishDate = lh.fromstring(spanishDate[0]).text_content()
            spanishDate = spanishDate.replace('\n', ' ')
            year = ''
            if "de dos mil" in spanishDate:
                startIndex = spanishDate.index('de dos mil')
                yearString = spanishDate[startIndex:]
                yearString = yearString.replace('.', '')
                if yearString in translations.spanishtoEngDosMil.keys():
                    year = translations.spanishtoEngDosMil.get(yearString)
                spanishDate = spanishDate.replace(yearString, '')
            if "de mil" in spanishDate:
                startIndex = spanishDate.index('de mil')
                yearString = spanishDate[startIndex:]
                yearString = yearString.replace('.', '')
                if yearString in translations.spanishtoEngDeMil.keys():
                    year = translations.spanishtoEngDeMil.get(yearString)
                spanishDate = spanishDate.replace(yearString, '')
            month = ''
            for key, value in translations.spanishtoEngMonths.items():
                if key.decode('utf-8') in spanishDate:
                    month = value
                    monthString = key
                    spanishDate = spanishDate.replace(monthString, '')
                    break
            spanishDate = spanishDate.replace("de", "")
            spanishDate = spanishDate.replace(".", "")
            spanishDate = spanishDate.replace(" ", "")
            day = ''
            for key, value in translations.spanishtoEngNum.items():
                if key.decode('utf-8') in spanishDate:
                    day = value
                    break
            DecisionDate = day+" "+month+" "+year
        else:
            DecisionDate = None
        ParticipantName = None
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractColombiaCourtReferences(file_path):
    '''
    Need to handle edge cases for case id and date
    '''
    try:
        # caseidString = re.compile("\\bSentencia\\b.*\-\d*\<span", re.IGNORECASE)
        file_content = helpers.getFileText(file_path, html=False)
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
                CaseId = CaseId.replace(">", "")
                CaseId = CaseId.replace("\r", '')
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
                    spanishDate = spanishDate.replace(dayString, '')
        spanishDate = spanishDate.replace(",", "")
        spanishDate = spanishDate.replace(")", "")
        spanishDate = spanishDate.replace("(", "")
        year = spanishDate[len(spanishDate)-4:len(spanishDate)]
        month = ''
        for key, value in translations.spanishtoEngMonths.items():
            if key.decode('utf-8') in spanishDate:
                month = value
                monthString = key
                spanishDate = spanishDate.replace(monthString, '')
                break
        DecisionDate = day+' '+month+' '+year
        ParticipantName = None
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractFranceCourtReferences(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
        html_text = lh.fromstring(file_content)
        extractedText = html_text.find('.//title').text_content().strip()
        data = extractedText.split('du')
        num = 'n°'
        splitIndex = data[0].index(num.decode('utf-8')) + len(num)
        decisionNumber = data[0][splitIndex:]
        decisionNumber = re.sub('[a-zA-z]', '', decisionNumber).strip()
        frenchDate = data[1].strip()
        dayPattern = re.compile("\\b\\d{1,2}\\b")
        dayMatcher = dayPattern.findall(frenchDate)
        day = ''
        if len(dayMatcher) > 0:
            day = dayMatcher[0]
            frenchDate = frenchDate.replace(day, '')
        year = ''
        year = frenchDate[len(frenchDate)-4:len(frenchDate)]
        for key, value in translations.frenchtoEngMonth.items():
                if key.decode('utf-8') in frenchDate:
                    month = value
                    break
        DecisionDate = day+' '+month+' '+year
        CaseId = decisionNumber
        ParticipantName = None
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractGermanyCourtReferences(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
        html_text = lh.fromstring(file_content)
        extractedText = html_text.find(".//p[@class='zitierung']")
        if extractedText:
            extractedText = extractedText.text_content().strip()
            if "Citation" in extractedText:
                data = extractedText.split(',')
                if " du " in extractedText:
                    info = data[1].split(" du ")
                    CaseId = info[0].replace(' ', '')
                    day, month, year = info[1].strip().split('.')
                    for key, value in translations.numberToMonth.items():
                        if key.decode('utf-8') in month:
                            month = value
                            break
                if " of " in extractedText:
                    info = data[1].split(" of ")
                    CaseId = info[0].replace(' ', '')
                    month, day, year = info[1].strip().split('/')
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
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractIrelandCourtReferences(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
                    ParticipantName = match.replace('Judgment Title: ', '').strip()
                if 'Supreme Court Record Number:' in match:
                    CaseId = match.replace('Supreme Court Record Number: ', '').strip()
                if 'Date of Delivery: ' in match:
                    dateString = match.replace('Date of Delivery: ', '').strip()
                    date_split = dateString.split('/')
                    month = date_split[0]
                    day = date_split[1]
                    year = date_split[2]
                    for key, value in translations.numberToMonth.items():
                        if key.decode('utf-8') in month:
                            month = value
                            break
                    DecisionDate = day+' '+month+' '+year
        return CaseId, DecisionDate, ParticipantName
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
        file_content = helpers.getFileText(file_path, html=False)
        html_text = unicode(lh.fromstring(file_content).text_content())
        pass
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractLesothoCourtReferences(file_path):
    '''
    TO DO: method for getting case id does not work for cases with multiple parens
    ex. (C OF A (CIV/APN/308/01))
    '''
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
            DecisionDate = day + ' ' + month + ' '+year
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractMalawiCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
    return CaseId, DecisionDate, ParticipantName


def extractNewZealandCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractPapaNewGuineaCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
            # Older cases use PGSC but newer cases use SC###
            # need check for if there is SC only and if SC only use that case id
            oldCaseIdPatternString = re.compile("PGSC [0-9]+|CRI [0-9]+")
            newCaseIdPatternString = re.compile("SC[0-9]+")
            oldCaseIdMatcher = oldCaseIdPatternString.findall(full_string)
            newCaseIdMatcher = newCaseIdPatternString.findall(full_string)
            if len(oldCaseIdMatcher) > 0 and len(newCaseIdMatcher) == 0:
                CaseId = oldCaseIdMatcher[0]
            if len(oldCaseIdMatcher) > 0 and len(newCaseIdMatcher) > 0:
                CaseId = newCaseIdMatcher[0]
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractPeruCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
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
                ParticipantName = ParticipantName.upper().replace('CASO', '').replace(':','')
            if len(extractedText) > 4:
                spanishDate = extractedText.strip()
                splitDate = spanishDate.split(',')[1]
                year = ''
                if "de dos mil" in splitDate:
                    startIndex = splitDate.index('de dos mil')
                    yearString = splitDate[startIndex:]
                    yearString = yearString.replace('.', '')
                    if yearString in translations.spanishtoEngDosMil.keys():
                        year = translations.spanishtoEngDosMil.get(yearString)
                    splitDate = splitDate.replace(yearString, '')
                if "de mil" in splitDate:
                    startIndex = splitDate.index('de mil')
                    yearString = splitDate[startIndex:]
                    yearString = yearString.replace('.', '')
                    if yearString in translations.spanishtoEngDeMil.keys():
                        year = translations.spanishtoEngDeMil.get(yearString)
                    splitDate = splitDate.replace(yearString, '')
                month = ''
                for key, value in translations.spanishtoEngMonths.items():
                    if key.decode('utf-8') in splitDate:
                        month = value
                        monthString = key
                        splitDate = splitDate.replace(monthString, '')
                        break
                splitDate = splitDate.replace("de", "")
                splitDate = splitDate.replace(".", "")
                splitDate = splitDate.replace(" ", "")
                day = ''
                for key, value in translations.spanishtoEngNum.items():
                    if key.decode('utf-8') in splitDate:
                        day = value
                        break
                DecisionDate = day+" "+month+" "+year
            else:
                DecisionDate = None
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractPhilippinesCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content)
        caseAndDate = html_text.find(".//p[@class='BOOK']")
        participant = html_text.find(".//p[@class='CASETITLE']")
        if caseAndDate is not None:
            caseAndDateText = caseAndDate.text_content()
            splitString = caseAndDateText.split(" ")
            CaseId = splitString[0]+splitString[1]+splitString[2]
            day = splitString[4][0:len(splitString[4])-1]
            month = splitString[3]
            year = splitString[5]
            DecisionDate = day + "-" + month + "-" + year
        if participant is not None:
            ParticipantName = participant.text_content()
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractSpainCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content)
        title_text = html_text.find(".//title")
        if title_text is not None:
            extractedText = title_text.text_content()
            extractedElements = extractedText.split('SENTENCIA ')
            CaseId = extractedElements[len(extractedElements) - 1]
        extractedDecisionDate = html_text.find("/html/body/div[@id='wrapper']/section[@id='main']/div[1]/div[3]/fieldset[1]/table/tr[5]//td[2]/text()")
        if extractedDecisionDate is not None:
            DecisionDate = extractedDecisionDate.text_content()
            DecisionDate = re.sub('\s+', '', DecisionDate)
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractSwitzerlandCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content)
        title_text = html_text.find(".//title")
        if title_text is not None:
            CaseId = title_text.text_content()
        div_text = html_text.find(".//div[@class='paraatf")
        if div_text is not None:
            dateString = div_text.text_content()
            # need to check french and german
            year = ''
            yearString = re.compile("[0-9]{4}")
            if yearString.search(dateString) is not None:
                year = yearString.search(dateString).group()
            month = ''
            dayString = re.compile("(de|du|de la) (0[1-9]|[0-9]+)")
            if dayString.search(dateString) is not None:
                day = dayString.search(dateString).group()
            for key, value in translations.frenchtoEngMonth.items():
                if key.decode('utf-8') in dateString:
                    month = value
                    break
            DecisionDate = day+" "+month+" "+year
            # if not French, check german
            if len(day) == 0 | len(month) == 0 | len(year) == 0:
                '''
                if month, day, or year are empty strings,
                check if in german instead
                '''
                if len(year) == 0:
                    yearStringGer = re.compile("[0-9]{4}")
                    if yearStringGer.search(dateString) is not None:
                        year = yearStringGer.search(dateString).group()
                if len(day) == 0:
                    dayStringGer = re.compile('(vom) (0[1-9]|[0-9]+)')
                    if dayStringGer.search(dateString) is not None:
                        day = dayStringGer.search(dateString).group()
                if len(month == 0):
                    for key, value in translations.swissToEnglishMonth.items():
                        if key.decode('utf-8') in dateString:
                            month = value
                            break
                DecisionDate = day+" "+month+" "+year
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractUgandaCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content)
        div_text = html_text.findall(".//div[@class='field-item']")
        if len(div_text) > 0:
            CaseId = div_text[1]
            DecisionDate = div_text[2]
        title_text = html_text.find(".//title")
        if title_text is not None:
            participantString = title_text.text_content()
        if " v " in participantString.lower():
            splitString = participantString.split(" v ")
            ParticipantName = splitString[0]
        if "vs." in participantString.lower():
            splitString = participantString.split("vs.")
            ParticipantName = splitString[0]
        elif "and" in participantString.lower():
            splitString = participantString.split("and")
            ParticipantName = splitString[0]
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractUKSupremeCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    if 'Supreme Court' in file_path:
        try:
            file_content = helpers.getFileText(file_path, html=False)
            if file_content:
                html_text = lh.fromstring(file_content)
            data = html_text.find(".//SMALL")
            if data is not None:
                text = data.text_content()
            else:
                data = html_text.find(".//small")
                if data is not None:
                    text = data.text_content()
            splitString = text.split(">>")[3].split('URL')[0]
            details1 = splitString.split("[")
            ParticipantName = details1[0].strip()
            details2 = details1[1].split('(')
            CaseId = "["+details2[0]
            decisionDate = details2[1].strip().replace(')', '')
            date1 = decisionDate.split(" ")
            date2 = date1[0].split("th")[0] + " " + date1[1][0:len(date1[1])] + " " + date1[2][0:len(date1[2])]
            DecisionDate = date2
            return CaseId, DecisionDate, ParticipantName
        except Exception, e:
            print e
            raise


def extractUKHouseofLordsCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    if 'House Of Lords' in file_path:
        try:
            file_content = helpers.getFileText(file_path, html=False)
            if file_content:
                html_text = lh.fromstring(file_content)
            monthsinEnglish = ["january", "february", "march",
                               "april", "may", "june",
                               "july", "august", "september",
                               "october", "november", "december", "september"]
            caseIdPattern = re.compile('UKHL')
            decisionDatePattern = re.compile("^.*?(" + '|'.join(monthsinEnglish) + ").*$")
            participant = html_text.find(".//title")
            if participant is not None:
                participantText = participant.text_content()
                participantSplit = participantText.split('-')
                ParticipantName = participantSplit[1]
            all_ps = html_text.findall(".//p")
            if all_ps is not None:
                for p_element in all_ps:
                    extractedCaseId = p_element.text_content()
                    if caseIdPattern.search(extractedCaseId) is not None:
                        CaseId = extractedCaseId.strip()
                        break
                for p_element in all_ps:
                    extractedDecisionDate = p_element.text_content()
                    if decisionDatePattern.search(extractedDecisionDate.lower()) is not None:
                        dateString = extractedDecisionDate
                        dateSplit = dateString.split(" ")
                        if len(dateSplit) == 4:
                            if "ON" in extractedDecisionDate:
                                decDate = extractedDecisionDate.split("ON")
                                DecisionDate = decDate[1].lower().strip()
                                break
                            else:
                                DecisionDate = " ".join(dateSplit[1:])
                                break
            if DecisionDate == '':
                all_center = html_text.findall(".//center")
                if all_center is not None:
                    for element in all_center:
                        extractedString = element.text_content()
                        if decisionDatePattern.search(extractedString.lower()) is not None:
                            dateString = extractedString
                            dateSplit = dateString.split(" ")
                            if len(dateSplit) == 4:
                                if "ON" in extractedString:
                                    decDate = extractedString.split("ON")
                                    DecisionDate = decDate[1].lower().strip()
                                    break
            return CaseId, DecisionDate, ParticipantName
        except Exception, e:
            print e
            raise


def extractUKPrivyCouncilCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    if 'Privy Council' in file_path:
        try:
            file_content = helpers.getFileText(file_path, html=False)
            if file_content:
                html_text = lh.fromstring(file_content)
            title = html_text.find(".//title")
            if title is not None:
                title_text = title.text_content()
                textSplit = title_text.split("[")
                ParticipantName = textSplit[0]
                caseSplit = textSplit[1].split("(")
                CaseId = "[".caseSplit[0]
                DecisionDate = caseSplit[1]
            return CaseId, DecisionDate, ParticipantName
        except Exception, e:
            print e
            raise


def extractUnitedStatesCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    decisionString = None
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content.decode('utf-8', 'ignore'))
        extractedText = html_text.find(".//center")
        details1 = html_text.findall(".//center")
        caseIdString = ''
        if len(details1) > 0:
            for center in details1:
                if "No." in center.text_content():
                    caseIdString = center.text_content()
                    break
            for center in details1:
                if "[" in center.text_content():
                    decisionString = center.text_content()
                    break
        if len(caseIdString) > 0:
            caseSearchString = re.compile('No\.\s\d+\-\d+|No\.\s\d{2,3}|Nos\.\s\d+.+\s\d{2,3}')
            if caseSearchString.search(caseIdString) is not None:
                caseString = caseSearchString.search(caseIdString).group().replace('No.', '').replace('Nos.', '').strip()
                CaseId = caseString
            decidedDateIdx = caseIdString.find('Decided')
            if decidedDateIdx != -1:
                DecisionDate = caseIdString[decidedDateIdx+8:decidedDateIdx+24].replace('.', '').strip()
        if len(caseIdString) == 0 and decisionString is not None:
            DecisionDate = decisionString.replace('[', '').replace(']', '')
        if extractedText is not None:
            participantString = extractedText.text_content().strip()
            participantSplit = participantString.split('\n')
            for split in participantSplit:
                if "v." in split.lower():
                    ParticipantName = split.strip()
                    break
            if len(ParticipantName) > 5000:
                ParticipantName = ''
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractZimbabweCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content.decode('utf-8', 'ignore'))
        all_divs = html_text.findall(".//div[@class='field-item odd']")
        if all_divs is not None:
            number = all_divs[0].text_content().strip()
            caseNo = all_divs[2].text_content().strip().replace("(", "").replace(")", "")
            decisiondate = all_divs[3].text_content().strip()
            CaseId = caseNo
            DecisionDate = decisiondate
        participantTag = html_text.find(".//h1[@class='title']")
        if participantTag is not None:
            participant = participantTag.text_content().strip()
            participantSplit = participant.split(" ")
            lastWord = participantSplit[len(participantSplit) - 1]
            if lastWord.lower() == number or lastWord.lower == "(pvt)":
                participant = participant.replace(lastWord, "")
            ParticipantName = participant
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise




for year, folder in files.items():
        for file in folder:
            extractZimbabweCourtReferences(file)
for file in folder:
    extractUnitedStatesCourtReferences(file)