import pandas as pd
import itertools
import esm
import helpers


def createSoftLawRegexDf(folder_path=None, file_name=None):
    # Get regex table
    file_name = 'softlaw_regex_20161003.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+file_name))
    for row in reader:
        regex_table.append(row)
    softlaw_names = esm.Index()
    for word in regex_table:
        softlaw_names.enter(word[0].encode('utf-8'), word[0])
    softlaw_names.fix()
    regex_test = dict((row[0], row[1:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df, columns=['info'])
    regex_df[['citation_type', 'softlaw_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info', inplace=True, axis=1)
    return softlaw_names, regex_df


def createForeignCourtsDf(folder_path=None, file_name=None):
    # Get regex table
    file_name = 'foreign_courts_regex_20161007.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+file_name))
    for row in reader:
        regex_table.append(row)
    def extract_key(v):
        return v[0]
    data = sorted(regex_table, key=extract_key)
    regex_result = [[k, [x[2] for x in g]] for k, g in itertools.groupby(data, extract_key)]
    country_names = esm.Index()
    court_names = esm.Index()
    for word in regex_result:
        country_names.enter(word[0].encode('utf-8'), word[0])
        for row in word[1]:
            court_names.enter(row.encode('utf-8'), [word[0], row])
    country_names.fix()
    court_names.fix()
    regex_test = dict((tuple([row[0], row[2]]), row[3:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df, columns=['info'])
    regex_df[['citation_type', 'country_id', 'court_code', 'court_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info', inplace=True, axis=1)
    return country_names, court_names, regex_df


def createIntlCourtsRegexDf(folder_path=None,file_name=None):
    # Get regex table
    file_name = 'intl_courts_regex_20161003.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+file_name))
    for row in reader:
        regex_table.append(row)
    intl_court_names = esm.Index()
    for word in regex_table:
        intl_court_names.enter(word[0].encode('utf-8'), word[0])
    intl_court_names.fix()
    regex_test = dict((row[0], row[1:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df, columns=['info'])
    regex_df[['citation_type', 'intl_crt_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info', inplace=True, axis=1)
    return intl_court_names, regex_df


def createTreatiesRegexDf(folder_path=None,file_name=None):
    # Get regex table
    file_name = 'treaties_regex_20161003.csv'
    regex_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+file_name))
    for row in reader:
        regex_table.append(row)
    treaty_names = esm.Index()
    for word in regex_table:
        treaty_names.enter(word[0].encode('utf-8'), word[0])
    treaty_names.fix()
    regex_test = dict((row[0], row[1:]) for row in regex_table)
    regex_df = pd.Series(regex_test)
    regex_df = pd.DataFrame(regex_df, columns=['info'])
    regex_df[['citation_type', 'treaty_id']] = regex_df['info'].apply(pd.Series)
    regex_df.drop('info', inplace=True, axis=1)
    return treaty_names, regex_df


def createCountryDf(folder_path=None, file_name=None):
    file_name = 'country_ids_20161210.csv'
    country_table = []
    reader = helpers.unicode_csv_reader(open(folder_path+file_name))
    for row in reader:
        country_table.append(row)
    country_dict = dict((row[0], row[1]) for row in country_table)
    country_dict = pd.Series(country_dict)
    country_df = pd.DataFrame(country_dict)
    country_df.columns = ['country_id']
    return country_df
