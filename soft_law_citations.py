import re
import os
import pandas as pd


def findallSoftLawMatches(text, softlaw_names):
    '''
    What to do about self references? Keep or skip?
    Goes through each country in regex table and returns the
    index of the matches as a tuple
    If there are matches, get +/- 10 characters (or words?) around the result
    and check if other term present
    For +/- 10 words use r"(?i)((?:\S+\s+){0,10})\b" + re.escape(search_term)+
    r"\b\s*((?:\S+\s+){0,10})"
    '''
    results = []
    matches = softlaw_names.query(text.encode('utf-8'))
    for match in matches:
        law_name = match[1]
        law_string = re.compile(r"(?i)((?:\S+\s+){0,10})\b" + re.escape(law_name)+ r"\b\s*((?:\S+\s+){0,10})",re.IGNORECASE)
        # use start of match
        match_location = match[0][0]
        buffer_area = 500 + len(law_name)
        lower_bound = (match_location) - buffer_area
        upper_bound = (match_location) + buffer_area
        if (match_location - buffer_area) < 0:
            lower_bound = 0
        if (match_location + buffer_area) > len(text):
            upper_bound = len(text)
        search_area = text[lower_bound:upper_bound]
        context = law_string.search(search_area)
        if context:
            context_string = context.group()
            match = [law_name, context_string]
            results.append(match)
    results = [list(x) for x in set(tuple(x) for x in results)]
    return results


def getSoftLawData(text, regex_df, softlaw_names, id_num, file, country_df,
                   country_name=None, year=None):
    '''
    Inputs:
        text: raw string or parsed html
        country_name: optional for now but will be used if excluding self-references
        year: optional
        regex_df: citation info for each country in dataframe form
        softlaw_names: list of lists of search terms and court names
    '''
    regex_sl_results = findallSoftLawMatches(text, softlaw_names)
    if regex_sl_results:
        regex_dict = dict((row[0], row[1:]) for row in regex_sl_results)
        dataset = pd.Series(regex_dict)
        dataset = pd.DataFrame(dataset, columns=['matches'])
        merged_results = pd.merge(dataset, regex_df, right_index=True, left_index=True).reset_index()
        merged_results = pd.concat([pd.DataFrame(dict(zip(merged_results.columns, merged_results.ix[i]))) for i in range(len(merged_results))])
        merged_results = merged_results.rename(columns={'matches': 'context'})
        merged_results['year'] = year
        merged_results['source_file_name'] = os.path.basename(file)
        merged_results['source_country_id'] = country_df.loc[country_name][0]
        merged_results['id'] = id_num
        merged_results = merged_results.rename_axis(None)
        merged_results.drop(['index'], inplace=True, axis=1)
        return merged_results
    else:
        return pd.DataFrame()


def insertSoftLawData(country_name, year, file, fileText, regex_df,
                      softlaw_names, id_num, mysql_table, connection_info, country_df):
    '''
    Inputs:
        country_name: name of the source country
        file: a string of the file path to the decision
        regex_df: regex dataframe used to merge in metadata
        information for matches
        softlaw_names: regex table in list form used to find matches
        mysql_table: mysql table name to insert matches into
        connection_info: mysql table connection info
    Output:
        None, inserts matches into mysql table
    '''
    try:
        softLaws = getSoftLawData(text=fileText, country_name=country_name,
                                  year=year, regex_df=regex_df,
                                  softlaw_names=softlaw_names,
                                  id_num=id_num,
                                  file=file, country_df=country_df)
        if not softLaws.empty:
            softLaws.to_sql(name=mysql_table, con=connection_info,
                            index=False, if_exists='append')
    except Exception, error:
        print error
        raise
