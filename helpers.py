'''
General functions for CGLP Project
'''
import PyPDF2
import re
import os
from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup
import lxml.html as lh
import csv
import pandas as pd
from tqdm import tqdm
import itertools
from sqlalchemy import create_engine
import esm
from pyth.plugins.rtf15.reader import Rtf15Reader
from pyth.plugins.plaintext.writer import PlaintextWriter
import helpers
import time
import cchardet
import MySQLdb as mysql
import MySQLdb.cursors


def connectDb(db_name, db_password):
    engine = create_engine("mysql+mysqldb://root:%s@localhost/%s?charset=utf8" % (db_password, db_name), encoding='utf-8')
    return engine

def createTables(db_name, db_password, drop_table=False):
    db = mysql.connect(host='127.0.0.1', port=3306, user='root', db= db_name, passwd=db_password)
    c = db.cursor(MySQLdb.cursors.DictCursor)
    c1 = db.cursor(MySQLdb.cursors.DictCursor)
    c2 = db.cursor(MySQLdb.cursors.DictCursor)
    if drop_table:
        c.execute("DROP TABLE IF EXISTS citations; DROP TABLE IF EXISTS case_info")
    c1.execute("CREATE TABLE `citations`( `id` int(11) NOT NULL, `citation_id` int(11) NOT NULL AUTO_INCREMENT, `citation_type` int(11) DEFAULT NULL, `year` int (11) DEFAULT NULL, `source_country_id` int(11) DEFAULT NULL, `source_court_id` int(11) DEFAULT NULL, `country_id` int(11) DEFAULT NULL, `court_code` int(11) DEFAULT NULL, `court_id` int(11) DEFAULT NULL, `intl_crt_id` int(11) DEFAULT NULL, `treaty_id` int(11) DEFAULT NULL, `softlaw_id` int(11) DEFAULT NULL, `context` varchar(2000) DEFAULT NULL, `source_file_name` varchar(100) DEFAULT NULL, PRIMARY KEY (`id`,`citation_id`))ENGINE=MyISAM DEFAULT CHARSET=utf8")
    c2.execute("CREATE TABLE `case_info`(`id` int(11) NOT NULL, `case_id` varchar(100) DEFAULT NULL,`decision_date` varchar(100) DEFAULT NULL, `participant_name` varchar(2000) DEFAULT NULL, `country_id` int(11) DEFAULT NULL, `year` int (11) DEFAULT NULL, `source_file_name` varchar(100) DEFAULT NULL, PRIMARY KEY (`id`) )ENGINE=MyISAM DEFAULT CHARSET=utf8")


def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    # Skip headers
    next(utf8_data)
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8').encode('utf-8').decode('utf-8') for cell in row]


def getFileText(file_path, html=False, pdf_utf8=False):
    '''
    input: string of file path
    output: either raw string or parsed html text content
    '''
    file_extension = os.path.splitext(file_path)[1]
    if file_extension.lower() != ".py":
        if file_extension.lower() == ".html" or file_extension.lower() == '.htm':
            file_content = open(file_path).read()
            if html:
                html_text = lh.fromstring(file_content).text_content()
                return html_text
            else:
                return file_content
        if file_extension == ".pdf":
            pdf_content = open(file_path, "rb")
            pdfReader = PyPDF2.PdfFileReader(pdf_content)
            num_pages = pdfReader.getNumPages()
            page_text = ""
            for i in range(0, num_pages):
                pageObj = pdfReader.getPage(i)
                page_text = page_text + " " + pageObj.extractText()
            # Need to check for pdfs that are just scanned images
            if len(page_text) <= num_pages:
                return None
            else:
                if pdf_utf8:
                    return page_text.encode('utf-8')
                else:
                    return page_text
        if file_extension == ".rtf":
            doc = Rtf15Reader.read(open(file_path))
            page_text = PlaintextWriter.write(doc).getvalue()
            uni_page_text = page_text.decode('utf-8')
            return uni_page_text
    return None


def getCountryFiles(folder_path, country_name):
    '''
    input: path to folder with text files and name of the country
    output: dictionary with year as key and values are the file paths
    for the documents in that year
    Notes: updated to walk through the entire path to get files in subfolders
    as well. Doesn't check that files are html though.
    '''
    full_path = folder_path + '/' + country_name + '/'
    sub_folders = os.listdir(full_path)
    regex = re.compile("([A-Za-z])\w.*")
    year_folders = {}
    if country_name == 'Uganda' or country_name == 'UK':
        for sub_folder in sub_folders:
            if sub_folder != ".DS_Store" and ".py" not in sub_folder:
                sub = os.listdir(full_path + '/' + sub_folder)
                for folder in sub:
                    if folder != ".DS_Store" and ".py" not in folder:
                        path = full_path + '/' + sub_folder + '/' + folder
                        if regex.findall(folder):
                            year = folder[-4:]
                        else:
                            year = folder
                        year_folders[year] = [val for sublist in [[os.path.join(i[0], j) for j in i[2]] for i in os.walk(path)] for val in sublist if '.DS_Store' not in val and '.py' not in val]
    else:
        for folder in sub_folders:
            if folder != ".DS_Store" or ".py" not in folder:
                path = full_path+'/'+folder
                subs = os.listdir(path)
                if regex.findall(folder):
                    year = folder[-4:]
                else:
                    year = folder
                year_folders[year] = [val for sublist in [[os.path.join(i[0], j) for j in i[2]] for i in os.walk(path)] for val in sublist if '.DS_Store' not in val and '.py' not in val]
    return year_folders


def findAllInstances(text, term):
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


def timed(f):
    start = time.time()
    ret = f
    elapsed = time.time() - start
    return ret, elapsed


def convert_encoding(data, new_coding='UTF-8'):
    encoding = cchardet.detect(data)['encoding']
    if new_coding.upper() != encoding.upper():
        data = data.decode(encoding, data).encode(new_coding)
    return data
