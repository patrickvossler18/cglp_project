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


def connectDb(db_name, db_password):
    engine = create_engine("mysql+mysqldb://root:%s@localhost/%s?charset=utf8" % (db_password, db_name), encoding='utf-8')
    return engine


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
    if file_extension.lower() == ".html" or file_extension == '.htm':
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
    for folder in sub_folders:
        if folder != ".DS_Store":
            path = full_path+folder
            if regex.findall(folder):
                year = folder[-4:]
            else:
                year = folder
            year_folders[year] = [val for sublist in [[os.path.join(i[0], j) for j in i[2]] for i in os.walk(path)] for val in sublist if '.DS_Store' not in val] 
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
