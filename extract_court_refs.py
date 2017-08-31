# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import lxml.html as lh
from lxml import etree
import pandas as pd
from dateutil.parser import parse
import re
import translations
import helpers
import os


def extractAustriaCourtReferences(file_path):
    CaseId = ''
    DecisionDate = ''
    ParticipantName = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        html_text = lh.fromstring(file_content)
        caseString = html_text.findall(".//p[@class='ErlText AlignJustify']")
        if caseString is not None:
            CaseId = caseString[2].text_content()
            DecisionDate = caseString[1].text_content()
        ParticipantName = None
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractAustraliaCourtReferences(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
        html_text = lh.fromstring(file_content)
        element = html_text.find(".//h2")
        if element is not None:
            elementSplit = element.text_content().split(';')
        caseAndDate = elementSplit[len(elementSplit)-1]
        index = len(caseAndDate)-1
        while(caseAndDate[index] != '('):
            index = index - 1
        endIndex = index
        while(caseAndDate[endIndex] != ')'):
            endIndex += 1
        CaseId = caseAndDate[0:index-1].replace('\r\n', '').strip(' ')
        DecisionDate = caseAndDate[index+1:endIndex].replace('\r\n', '').strip(' ')
        ParticipantName = elementSplit[0].strip()
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractBotswanaCourtReference(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
        # html_text = BeautifulSoup(file_content, "html.parser")
        html_text = lh.fromstring(file_content)
        textToBeExtracted = html_text.find(".//title")
        if textToBeExtracted is not None:
            text = textToBeExtracted.text_content()
        pattern = re.compile("\\((.*?)\\)")
        matched_array = pattern.findall(text)
        if len(matched_array) > 0:
            firstIndex = text.find(matched_array[len(matched_array) - 2])
            if firstIndex == -1:
                ParticipantName = text
            else:
                ParticipantName = text[0:firstIndex-1].strip()
            DecisionDate = matched_array[len(matched_array) - 1]
            CaseId = matched_array[len(matched_array) - 2]
        else:
            ParticipantName = text
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
        print e
        raise


def extractCanadaCourtReference(file_path):
    try:
        file_content = helpers.getFileText(file_path, html=False)
        soup = BeautifulSoup(file_content.decode('utf-8', 'ignore'), "html.parser")
        metadata = soup.find("div", {"class": "metadata"})
        tablerows = metadata.findAll('tr')
        court_data = []
        for row in tablerows:
            info = row.find('td', {"class": "metadata"}).get_text().strip()
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
                    monthString = key.decode('utf-8')
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
        file_content = helpers.getFileText(file_path, html=False)
        # dateString = re.compile("\\bD\.?C\.?,.*\d{4}")
        # dateString = re.compile("\\bD\.\s?C\.?\,.*[\s\S]*?.*\d{4}[,|:|;|\\?|!|)|(]")
        html_text = lh.fromstring(file_content)
        spans = html_text.findall('.//span[@lang]')
        if len(spans) == 0:
            spans = html_text.findall('.//span')
        spanishDate = ''
        CaseId = ''
        caseidString = re.compile("\>?\bSentencia\b[\s\S]*?.*\d{1}", re.IGNORECASE)
        dateString1 = re.compile("\bD\.\s?C\.?\,.*[\s\S]*?.*\d{4}[,|:|;|\?|!|)|(]")
        dateString2 = re.compile("\bBogot\xe1\b[\s\S]*?\d{4}")
        dateString2 = re.compile("\bBogot\xe1\b[\s\S]*?\d{4}")
        dateString3 = re.compile("\((\w+\s)?\d{1,2}.*\r?\n?\d{4}\W?\)")
        for span in spans:
            text = span.text_content()
            # caseidString = re.compile("\>?\\bSentencia\\b\sC\-\d+\/\d{2}", re.IGNORECASE)
            caseMatch = caseidString.search(text)
            if caseMatch is not None and len(CaseId) == 0:
                CaseId = caseMatch.group()
                break
        for span in spans:
            text = span.text_content()
            dateMatch1 = dateString1.search(text)
            if dateMatch1 is not None:
                spanishDate = dateMatch1.group()
                break
        if len(spanishDate) == 0:
            for span in spans:
                text = span.text_content()
                dateMatch2 = dateString2.search(text)
                if dateMatch2 is not None :
                    spanishDate = dateMatch2.group()
                    break
        if len(spanishDate) == 0:
            for span in spans:
                text = span.text_content()
                dateMatch3 = dateString3.search(text)
                if dateMatch3 is not None:
                    spanishDate = dateMatch3.group()
                    break
        if len(CaseId) == 0:
            # caseidString = re.compile("\>?\\bSentencia\\b[\s\S]*?\d{3}\/\d{2}", re.IGNORECASE)
            caseidString2 = re.compile("\>?\bSentencia\b[\s\S]*?.*\d{1}", re.IGNORECASE)
            # caseidString = re.compile("\\bSentencia\\b[\s\S]*?.*\d{1}",re.IGNORECASE)
            caseid_matches = caseidString2.findall(file_content)
            if len(caseid_matches) > 0:
                CaseId = lh.fromstring(caseid_matches[0]).text_content().encode('utf-8')
                CaseId = CaseId.replace(">", "")
                CaseId = CaseId.replace("\r", '')
                CaseId = CaseId.replace("\n", " ")
        if len(CaseId) > 30 or len(CaseId) == 0:
            b_tags = html_text.findall('.//b')
            caseidString3 = re.compile("\>?Sentencia[\s\S]*?.*\d{1}", re.IGNORECASE)
            for b in b_tags:
                caseMatch1 = caseidString3.search(b.text_content())
                if caseMatch1 is not None:
                    CaseId = caseMatch1.group()
                    break
            if len(CaseId) > 30:
                bodytext = html_text.find('.//body').text_content()
                caseMatch2 = caseidString3.search(bodytext)
                if caseMatch2 is not None:
                    CaseId = caseMatch2.group()
        if len(spanishDate) == 0:
            dateString = re.compile("\\bD\.\s?C\.?\,.*[\s\S]*?.*\d{4}[,|:|;|\\?|!|)|(]")
            dateMatch = dateString.findall(file_content)
            if len(dateMatch) > 0:
                spanishDate = lh.fromstring(dateMatch[0]).text_content()
            dateString = re.compile("\\bBogot.*\\b[\s\S]*?\d{4}", re.IGNORECASE)
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
                    dayString = key.decode('utf-8')
                    spanishDate = spanishDate.replace(dayString, '')
        spanishDate = spanishDate.replace(",", "")
        spanishDate = spanishDate.replace(")", "")
        spanishDate = spanishDate.replace("(", "")
        year = spanishDate[len(spanishDate)-4:len(spanishDate)]
        month = ''
        for key, value in translations.spanishtoEngMonths.items():
            if key.decode('utf-8') in spanishDate:
                month = value
                monthString = key.decode('utf-8')
                spanishDate = spanishDate.replace(monthString, '')
                break
        DecisionDate = day+' '+month+' '+year
        ParticipantName = ''
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


def extractLesothoCourtReferences(file_path):
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
                # case_match = re.search(r'\((.*?)\)', split_text[1])
                case_match = re.search(r'\((.*?)\)\)?(.*\))?', split_text[1])
                if case_match is not None:
                    CaseId = case_match.group()
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
                ParticipantName = ParticipantName.upper().replace('CASO', '').replace(':', '')
            if len(extractedText) > 4:
                spanishDate = extractedText.strip()
                splitedDate = spanishDate.split(',')
                if len(splitedDate) > 1:
                    splitDate = splitedDate[1]
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
    day = ''
    month = ''
    year = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content)
        caseAndDate = html_text.find(".//p[@class='BOOK']")
        participant = html_text.find(".//p[@class='CASETITLE']")
        if caseAndDate is not None:
            caseAndDateText = caseAndDate.text_content()
            for mon in translations.numberToMonth.values():
                if mon in caseAndDateText.lower():
                    month = mon
                    break
            yearString = re.compile("\d{4}\]")
            dayString = re.compile('\d{2}\,')
            caseIdString = re.compile('\w\.\w.\sNo\..*\.')
            if yearString.search(caseAndDateText) is not None:
                year = yearString.search(caseAndDateText).group().replace(']', '')
            if dayString.search(caseAndDateText) is not None:
                day = dayString.search(caseAndDateText).group().replace(',', '')
            if caseIdString.search(caseAndDateText) is not None:
                CaseId = caseIdString.search(caseAndDateText).group()
            if day and month and year:
                DecisionDate = day + "-" + month + "-" + year
        if participant is not None:
            ParticipantName = participant.text_content()
        return CaseId, DecisionDate, ParticipantName.encode('utf-8')
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
        decisionDateString = html_text.find(".//li[@id='resolucion-identifier']")
        if decisionDateString is not None:
            splitString = decisionDateString.text_content().split(',')
            if len(splitString) > 1:
                spanishDate = splitString[1].strip()
                year = ''
                yearString = re.compile("[0-9]{4}")
                if yearString.search(spanishDate) is not None:
                    year = yearString.search(spanishDate).group()
                month = ''
                for key, value in translations.spanishtoEngMonths.items():
                    if key.decode('utf-8') in spanishDate:
                        month = value
                        monthString = key
                        spanishDate = spanishDate.replace(monthString, '')
                        break
                day = ''
                dayString = re.compile("([1-9]{1}|[0-9]{2})")
                if dayString.search(spanishDate) is not None:
                    day = dayString.search(spanishDate).group()
                DecisionDate = day+" "+month+" "+year
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
        div_text = html_text.find(".//div[@class='paraatf']")
        if div_text is not None:
            dateString = div_text.text_content()
            # need to check french and german
            year = ''
            yearString = re.compile("[0-9]{4}")
            if yearString.search(dateString) is not None:
                year = yearString.search(dateString).group()
            month = ''
            day = ''
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
                if len(month) == 0:
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
    participantString = ''
    caseSearchString = re.compile('No\.\s\d+\-\d+|No\.\s\d{2,3}|Nos\.\s\d+.+\s\d{2,3}',re.IGNORECASE)
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content.decode('utf-8', 'ignore'))
            details1 = html_text.findall(".//center")
            caseIdString = ''
            if len(details1) > 0:
                for center in details1:
                    if "No." in center.text_content():
                        caseIdString = center.text_content()
                        break
                for center in details1:
                    if "[" in center.text_content():
                        date = re.search('\[(.*)\]', center.text_content())
                        if date is not None:
                            decisionString = date.group(1)
                            break
                for center in details1:
                    for child in center:
                        if "<br/>" in etree.tostring(child):
                            participantString = child.text_content().strip()
                            if child.getnext() is not None:
                                if len(child.getnext()) > 0:
                                    participantString += child.getnext().text_content().strip()
                            participantString = participantString.replace('\n', ' ').replace('\r', '')
                            participantString = re.sub("\s\s+", " ", participantString)
                            cert = participantString.lower().find("certiorari")
                            case_no = caseSearchString.search(participantString)
                            if cert != -1:
                                participantString = participantString[:cert]
                            elif case_no is not None:
                                case_idx = participantString.lower().find(case_no.group().lower())
                                if case_idx != -1:
                                    participantString = participantString[:case_idx]
                if len(participantString) == 0:
                    for center in details1:
                        if "v." in center.text_content():
                            participantString = center.text_content().strip().replace('\n', ' ').replace('\r', '')
                if len(participantString) > 0:
                    ParticipantName = participantString
            if len(caseIdString) > 0:
                if caseSearchString.search(caseIdString) is not None:
                    caseString = caseSearchString.search(caseIdString).group().replace('No.', '').replace('Nos.', '').strip()
                    CaseId = caseString
                decidedDateIdx = caseIdString.find('Decided')
                if decidedDateIdx != -1 and decisionString is None:
                    DecisionDate = caseIdString[decidedDateIdx+8:decidedDateIdx+24].replace('.', '').strip()
            if len(caseIdString) == 0 and decisionString is not None and len(DecisionDate) == 0:
                DecisionDate = decisionString
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


def extractMalaysiaCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        caseidPatternString = re.compile("(([R][A][Y][U][A][N]\s[J][E][N][A][Y][A][H])?([R][A][Y][U][A][N]\s[S][I][V][I][L])?([C][I][V][I][L]\s[A][P][P][E][L])?([C][R][I][M][I][N][A][L]\s[A][P][P][E][A][L])?([C][I][V][I][L]\s[A][P][P][E][A][L])?\s[N][Oo][.:]\s[A-Z]?[-]?\d{1,2}(\([I][M]\))?(\\[[A-Z]\\])?[-]\d{1,4}[-]\d{2,4}(\([A-Z]\))?)")
        decisionDatePatternString = re.compile("(([D][a][t][e]\s[o][f]\s)([D][e][c][i][s][i][o][n])?([J][u][d][g][m][e][n][t])?[:]\s)")
        decisionString = decisionDatePatternString.search(file_content)
        caseIdString = caseidPatternString.search(file_content)
        if decisionString is not None:
            DecisionDate = decisionString.group()
        if caseIdString is not None:
            CaseId = caseIdString.group()
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractNigeriaCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        caseidPatternString = re.compile("([A-Z][A-Z]([A-Z]\\d{2,3})?(\\d{2,3})?[/]\\d{2,4})")
        decisionDatePatternString = re.compile("([1-9]|[12][0-9]|3[01])([t][h]|[r][d]|[s][t]|[n][d])\\s([Dd][Aa][Yy]\\s[o][f])\\s(Jan(uary)?|Feb(ruary)?|Mar(ch)?|Apr(il)?|May|Jun(e)?|Jul(y)?|Aug(ust)?|Sep(tember)?|Oct(ober)?|Nov(ember)?|Dec(ember)?)[- /.]?\\s(1[9][0-9][0-9]|2[0-9][0-9][0-9])")
        caseIdString = caseidPatternString.search(file_content)
        if caseIdString is not None:
            CaseId = caseIdString.group()
            CaseId = CaseId.replace("/", ":")
        decisionString = decisionDatePatternString.search(file_content)
        if decisionString is not None:
            dateString = decisionString.group()
            start1 = dateString.find("Day of")
            if start1 != -1:
                DecisionDate = dateString[0:start1-3] + " " + dateString[start1 + 7:len(dateString)]
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractSouthAfricaCourtReferences(file_path):
    '''
    Inconsistent for participant name
    '''
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        caseidPatternString = re.compile("[A-Z]{3}\/?\s?[0-9]{1,2}\/[0-9]{2}")
        caseIdString = caseidPatternString.search(file_content)
        if caseIdString is not None:
            CaseId = caseIdString.group()
            CaseIdIdx = file_content.find(CaseId)
            heardOnIdx = file_content.lower().find('heard on')
            if CaseIdIdx != -1 and heardOnIdx != -1:
                participantString = file_content[CaseIdIdx+len(CaseId):heardOnIdx].replace('\n', '').strip()
                participantString = ' '.join(participantString.split())
                ParticipantName = participantString
            decidedOnIdx = file_content.lower().find('decided on')
            judgmentIdx = file_content.find('JUDGMENT')
            if decidedOnIdx != -1 and judgmentIdx != -1:
                DecisionDate = file_content[decidedOnIdx+len('decided on'):judgmentIdx].strip().replace(':', '')
        if len(ParticipantName) == 0 and len(DecisionDate) == 0:
            participantNamePatternString = re.compile("(IN THE CONSTITUTIONAL COURT OF SOUTH AFRICA).*(JUDGMENT)")
            participantNameString = participantNamePatternString.search(file_content)
            if participantNameString is not None:
                participantString = participantNameString.group()
                participantString = participantString.replace("IN THE CONSTITUTIONAL COURT OF SOUTH AFRICA", "")
                participantString = participantString.replace("CASE NO", "")
                participantString = participantString.replace("CASE NO :", "")
                participantString = participantString.replace(CaseId, "")
                matterString = "In the matter of:"
                if matterString not in participantString:
                    matterString = "In the matter of :"
                if matterString in participantString:
                    matterStartIdx = participantString.find(matterString)
                    matterEndIdx = matterStartIdx + len(matterString)
                    if "Applicantv" in participantString or "Applicantversus" in participantString:
                        participantString = participantString.replace("Applicant", '')
                    if "Applicantand" in participantString or "Applicantsand" in participantString:
                        participantString = participantString.replace("Applicantand", ' versus ')
                        participantString = participantString.replace("Applicantsand", ' versus ')
                    respondentIdx = participantString.lower().find("respondent")
                    ParticipantName = participantString[matterEndIdx:respondentIdx]
                    ParticipantName = ParticipantName.replace(":", "")
                    ParticipantName = ParticipantName.strip()
                deliveredStartIdx = participantString.lower().find('delivered on')
                deliveredEndIdx = deliveredStartIdx+len('delivered on')
                judgmentIdx = participantString.lower().find('judgment')
                DecisionDate = participantString[deliveredEndIdx:judgmentIdx]
                DecisionDate = DecisionDate.replace("_", "")
                DecisionDate = DecisionDate.replace(":", "")
                DecisionDate = DecisionDate.strip()
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractBelgiumCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        caseidPatternString = re.compile("(Arr\xeat\sn\xb0\s\d{1,3}(\/\d{1,4})?)")
        decisionDatePatternString = re.compile("([d][u]\s\d{1,2}\s\w{1,10}\s\d{4})")
        caseIdString = caseidPatternString.search(file_content)
        if caseIdString is not None:
            CaseId = caseIdString.group()
        decisionString = decisionDatePatternString.search(file_content)
        if decisionString is not None:
            dateString = decisionString.group()
            dateString = dateString.replace("\n", " ")
            decisionSplit = dateString.split(" ")
            day = decisionSplit[1]
            month = translations.frenchtoEngMonth.get(decisionSplit[2], '')
            year = decisionSplit[3]
            DecisionDate = ' '.join([day, month, year])
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractIndiaCourtReferences(file_path):
    ParticipantName = ''
    CaseId = ''
    DecisionDate = ''
    try:
        file_content = helpers.getFileText(file_path, html=False)
        if file_content:
            html_text = lh.fromstring(file_content.decode('utf-8', 'ignore'))
        documentText = html_text.find(".//textarea")
        if documentText is not None:
            extractedText = documentText.text_content()
            caseIdPatternString = re.compile("(CASE NO\.?\:?)\r?\n?\r?.*")
            caseIdString = caseIdPatternString.search(extractedText)
            if caseIdString is not None:
                CaseId = caseIdString.group().replace('CASE NO.:', '').strip()
            if len(CaseId) == 0:
                caseIdPatternString = re.compile("(CITATION\:?)\r?\n?\r?.*")
                caseIdString = caseIdPatternString.search(extractedText)
                if caseIdString is not None:
                    CaseId = caseIdString.group().replace('CITATION', '').strip()
                    CaseId = CaseId.replace(':', '').strip()
            petitionerPatternString = re.compile("(PETITIONER\:?)\r?\n?\r?.*")
            petitionerIdString = petitionerPatternString.search(extractedText)
            petitionerText = ''
            respondentText = ''
            if petitionerIdString is not None:
                petitionerText = petitionerIdString.group().replace('PETITIONER', '').strip()
                petitionerText = petitionerText.replace(':', '').strip()
            respondentPatternString = re.compile("(RESPONDENT\:?)\r?\n?\r?.*")
            respondentIdString = respondentPatternString.search(extractedText)
            if respondentIdString is not None:
                respondentText = respondentIdString.group().replace('RESPONDENT', '').strip()
                respondentText = respondentText.replace(':', '').strip()
            if len(petitionerText) > 0 and len(respondentText) > 0:
                ParticipantName = petitionerText + " versus " + respondentText
            datePatternString = re.compile("(DATE OF JUDGMENT\:?)\r?\n?\r?.*")
            dateString = datePatternString.search(extractedText)
            if dateString is not None:
                DecisionDate = dateString.group().replace('DATE OF JUDGMENT', '').strip()
                DecisionDate = DecisionDate.replace(':', '').strip()
        return CaseId, DecisionDate, ParticipantName
    except Exception, e:
            print e
            raise


def extractBelarusCourtReferences(file_path):
    pass


def extractLithuaniaCourtReferences(file_path):
    pass


def extractLatviaCourtReferences(file_path):
    '''
    use datefinder for getting date of decision
    case Id in format No.YYYY-##-####
    participants name: reviewed the case "Case Title here"
    '''
    pass
    # try:
    #     ParticipantName = ''
    #     CaseId = ''
    #     DecisionDate = ''
    #     file_content = helpers.getFileText(file_path, html=False)
    #     # html_text = unicode(lh.fromstring(file_content).text_content())
    #     html_text = BeautifulSoup(file_content, "html.parser")
    #     pass
    #     return CaseId, DecisionDate, ParticipantName
    # except Exception, e:
    #     print e
    #     raise


countryRefFunctions = {
    'Austria': extractAustriaCourtReferences,
    'Australia': extractAustraliaCourtReferences,
    'Botswana': extractBotswanaCourtReference,
    'Belgium': extractBelgiumCourtReferences,
    'Canada': extractCanadaCourtReference,
    'Chile': extractChileCourtReferences,
    'Colombia': extractColombiaCourtReferences,
    'France': extractFranceCourtReferences,
    'Germany': extractGermanyCourtReferences,
    'India': extractIndiaCourtReferences,
    'Ireland': extractIrelandCourtReferences,
    # 'Latvia': extractLatviaCourtReferences,
    # 'Lithuania': extractLithuaniaCourtReferences,
    'Lesotho': extractLesothoCourtReferences,
    'Malawi': extractMalawiCourtReferences,
    'Malaysia': extractMalaysiaCourtReferences,
    'New Zealand': extractNewZealandCourtReferences,
    'Nigeria': extractNigeriaCourtReferences,
    'Papua New Guinea': extractPapaNewGuineaCourtReferences,
    'Peru': extractPeruCourtReferences,
    'Philippines': extractPhilippinesCourtReferences,
    'Spain': extractSpainCourtReferences,
    'South Africa': extractSouthAfricaCourtReferences,
    'Switzerland': extractSwitzerlandCourtReferences,
    'Uganda': extractUgandaCourtReferences,
    'United Kingdom': [extractUKSupremeCourtReferences,
                       extractUKHouseofLordsCourtReferences,
                       extractUKPrivyCouncilCourtReferences],
    'United States': extractUnitedStatesCourtReferences,
    'Zimbabwe': extractZimbabweCourtReferences
}


def insertCaseRefData(case_info, country_name, country_df, year, id,
                      source_file, mysql_table, connection_info):
    try:
        case_info_list = list(case_info)
        case_info_list = [x.encode('utf-8') if x is not None and x is not isinstance(x, (int, long)) else x for x in case_info_list]
        case_info_list.extend([country_df.loc[country_name][0], year, id])
        case_info_list.extend([os.path.basename(source_file)])
        case_info_df = pd.DataFrame(columns=['case_id', 'decision_date',
                                             'participant_name', 'country_id',
                                             'year', 'id', 'source_file_name'])
        case_info_df.loc[0] = case_info_list
        case_info_df['']
        case_info_df.to_sql(name=mysql_table, con=connection_info,
                            index=False, if_exists='append')
    except Exception, error:
        print error
        raise
