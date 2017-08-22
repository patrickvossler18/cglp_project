import re
import os
import pandas as pd
import itertools


def findallCitationMatches(text, ref_terms, term_idx=0):
    '''
    For +/- 10 words use
    r"(?i)((?:\S+\s+){0,10})\b"
    + re.escape(search_term)+ r"\b\s*((?:\S+\s+){0,10})"
    '''
    results = []
    matches = ref_terms.query(text.encode('utf-8'))
    for match in matches:
        court_name = match[1]
        court_string = re.compile(r"(?i)((?:\S+\s+){0,10})\b" + re.escape(court_name) + r"\b\s*((?:\S+\s+){0,10})", re.IGNORECASE)
        # use start of match
        match_location = match[0][0]
        buffer_area = 500 + len(court_name)
        lower_bound = (match_location) - buffer_area
        upper_bound = (match_location) + buffer_area
        if (match_location - buffer_area) < 0:
            lower_bound = 0
        if (match_location + buffer_area) > len(text):
            upper_bound = len(text)
        search_area = text[lower_bound:upper_bound]
        context = court_string.search(search_area)
        if context:
            # Clean up context_string so that excel output is readable
            context_string = context.group().replace("\n", "").replace("\r\n", "")
            match = [court_name, context_string]
            results.append(match)
    results = [list(x) for x in set(tuple(x) for x in results)]
    return results


def getCitationData(text, regex_df, ref_terms, id_string, file, country_df,
                    country_name=None, year=None):
    '''
    Inputs:
        text: raw string or parsed html
        country_name: optional for now but will be used if
        excluding self-references
        year: optional
        regex_df: citation info for each country in dataframe form
        ref_terms: list of lists of search terms
    '''
    try:
        regex_results = findallCitationMatches(text, ref_terms)
        if regex_results:
            regex_dict = dict((row[0], row[1:]) for row in regex_results)
            dataset = pd.Series(regex_dict)
            dataset = pd.DataFrame(dataset, columns=['matches'])
            merged_results = pd.merge(dataset, regex_df, right_index=True,
                                      left_index=True).reset_index()
            merged_results = pd.concat([pd.DataFrame(dict(zip(merged_results.columns, merged_results.ix[i]))) for i in range(len(merged_results))])
            merged_results = merged_results.rename(columns={'matches': 'context'})
            merged_results['year'] = year
            merged_results['source_file_name'] = os.path.basename(file)
            merged_results['source_country_id'] = country_df.loc[country_name][0]
            merged_results['id'] = id_string
            merged_results = merged_results.rename_axis(None)
            merged_results.drop(['index'], inplace=True, axis=1)
            return merged_results
        else:
            return pd.DataFrame()
    except Exception, error:
        print error
        raise


def findallForeignCourtMatches(text, country_names, court_names, cntry_name):
    '''
    What to do about self references? Keep or skip?
    Goes through each country in regex table and returns the index of the matches as a tuple
    If there are matches, get +/- 10 characters (or words?) around the result and check if other term present
    For +/- 10 words use r"(?i)((?:\S+\s+){0,10})\b" + re.escape(search_term)+ r"\b\s*((?:\S+\s+){0,10})"
    '''
    results = []
    matches = country_names.query(text.encode('utf-8'))
    for match in matches:
        country_name = match[1]
        # Remove this condition
        # if country_name != cntry_name:
        country_string = re.compile(r"(?i)((?:\S+\s+){0,10})\b" + re.escape(country_name)+ r"\b\s*((?:\S+\s+){0,10})", re.IGNORECASE)
        # use start of match
        match_location = match[0][0]
        buffer_area = 500 + len(country_name)
        lower_bound = (match_location) - buffer_area
        upper_bound = (match_location) + buffer_area
        if (match_location - buffer_area) < 0:
            lower_bound = 0
        if (match_location + buffer_area) > len(text):
            upper_bound = len(text)
        search_area = text[lower_bound:upper_bound]
        context = country_string.search(search_area)
        if context:
            context_string = context.group().replace("\n", "").replace("\r\n", "")
            find_court = court_names.query(context_string.encode('utf-8'))
            court_match = None
            for result in find_court:
                if result[1][1] != ' ' and result[1][0] == country_name.strip():
                    court_match = result[1][1]
            if court_match:
                match = [country_name, court_match, context_string]
                results.append(match)
    results = [list(x) for x in set(tuple(x) for x in results)]
    return results


def getForeignCourtsData(text, regex_df, country_names, court_names, id_string, file,
                         country_df, country_name=None, year=None):
    '''
    Inputs:
        text: raw string or parsed html
        country_name: optional for now but will be used if
        excluding self-references
        year: optional
        regex_df: citation info for each country in dataframe form
        country_names: list of lists of search terms and court names
    '''
    try:
        # Store matches in list
        regex_ct_results = findallForeignCourtMatches(text, country_names, court_names, country_name)
        if regex_ct_results:
            # If there are matches, reshape the results into a dataframe, merge with regex_df and clean up for inserting into table
            def extract_key(v):
                return [v[0], v[1]]
            data = sorted(regex_ct_results, key=extract_key)
            regex_data = [[k, [x[2] for x in g]] for k, g in itertools.groupby(data, extract_key)]
            regex_dict = dict((tuple(row[0]), row[1]) for row in regex_data)
            dataset = pd.Series(regex_dict)
            dataset = pd.DataFrame(dataset, columns=['matches'])
            merged_results = pd.merge(dataset, regex_df, right_index=True, left_index=True).reset_index()
            merged_results = pd.concat([pd.DataFrame(dict(zip(merged_results.columns, merged_results.ix[i]))) for i in range(len(merged_results))])
            merged_results = merged_results.rename(columns={'matches': 'context'})
            merged_results.drop(['level_1', 'level_0'], inplace=True, axis=1)
            merged_results['year'] = year
            merged_results['source_file_name'] = os.path.basename(file)
            merged_results['id'] = id_string
            merged_results['source_country_id'] = country_df.loc[country_name][0]
            merged_results = merged_results.rename_axis(None)
            # merged_results.drop(['index'],inplace=True,axis=1)
            return merged_results
        else:
            return pd.DataFrame()
    except Exception, error:
        print error
        raise