import json
import base64
from pymongo import MongoClient
import requests
import sys
import itertools
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId
from os import listdir
import pandas as pd
from logs import extended_logger
from merge_duplicates import merger


# ******************************************************************************************************** 
# NOT USED - JUST FOR REFERENCE
'''
climate_eng_query    = """content_group:21 AND text:"climate change" """
covid_eng_query      = """content_group:21 AND text:"covid vaccine" """
immigrants_eng_query = """content_group:21 AND text:"immigrant" """
'''

climate_eng_query    = """text:"climate change" """
covid_eng_query      = """text:"covid vaccine" """
immigrants_eng_query = """text:"immigrant" """


'''
climate_eng_query    = """content_group:7 AND text:"climate change" """
covid_eng_query      = """content_group:7 AND text:"covid vaccine" """
immigrants_eng_query = """content_group:7 AND text:"immigrant" """
'''

climate_es_query     = """content_group:20 AND text:"Cambio climático" """
covid_es_query       = """content_group:20 AND (text:"covid" OR text:"coronavirus") """
immigrants_es_query  = """content_group:20 AND text:"inmigrante" """

# ********************************************************************************************************

load_dotenv()

# Solr Configuration
solr_url = os.getenv('SOLR_URL')
solr_username = os.getenv('SOLR_UNAME')
solr_password = os.getenv('SOLR_PWD')
auth_key = os.getenv('auth_key')

# MongoDB Configuration
mongodb_client = os.getenv('MONGO_URL')
mongodb_database = os.getenv('MONGO_DB')
mongodb_collection_en = os.getenv('MONGO_COLLECTION_EN')
mongodb_collection_es = os.getenv('MONGO_COLLECTION_ES')

file_climate_english = open("keywords/climate_english.txt","r+", encoding='utf-8')
file_climate_spanish = open("keywords/climate_spanish.txt","r+", encoding='utf-8')

file_covid_english = open("keywords/covid19_english.txt","r+", encoding='utf-8')
file_covid_spanish = open("keywords/covid19_spanish.txt","r+", encoding='utf-8')

file_immigration_english = open("keywords/immigration_english.txt","r+", encoding='utf-8')
file_immigration_spanish = open("keywords/immigration_spanish.txt","r+", encoding='utf-8')

############################################
# load paths for English articles

path_EN_CH_HOW  = os.getenv("path_EN_CH_HOW")
path_EN_CH_IS   = os.getenv("path_EN_CH_IS")
path_EN_CH_WHAT = os.getenv("path_EN_CH_WHAT")

path_EN_CV_SH = os.getenv("path_EN_CV_SH")
path_EN_CV_WA = os.getenv("path_EN_CV_WA")
path_EN_CV_WH = os.getenv("path_EN_CV_WH")

path_EN_IM_AR = os.getenv("path_EN_IM_AR")
path_EN_IM_HO = os.getenv("path_EN_IM_HO")
path_EN_IM_IS = os.getenv("path_EN_IM_IS")

############################################
# load paths for Spanish articles

path_ES_CH_AQ = os.getenv("path_ES_CH_AQ")
path_ES_CH_CO = os.getenv("path_ES_CH_CO")
path_ES_CH_ES = os.getenv("path_ES_CH_ES")

path_ES_CV_DE = os.getenv("path_ES_CV_DE")
path_ES_CV_FU = os.getenv("path_ES_CV_FU")
path_ES_CV_QU = os.getenv("path_ES_CV_QU")

path_ES_IM_CO = os.getenv("path_ES_IM_CO")
path_ES_IM_LA = os.getenv("path_ES_IM_LA")
path_ES_IM_RE = os.getenv("path_ES_IM_RE")

############################################


# Establish connection to MongoDB
try:
    mongo_client = MongoClient(mongodb_client)
    extended_logger.info("Connected to MongoDB")
except Exception as e:
    extended_logger.error(e)

try:
    db = mongo_client[mongodb_database]
    extended_logger.info("Connected to Database")
except Exception as e:
    extended_logger.error(e)
    
try:
    collection_en = db[mongodb_collection_en]
    extended_logger.info("Connected to Collection EN")
except Exception as e:
    extended_logger.error(e)
    

try:
    collection_es = db[mongodb_collection_es]
    extended_logger.info("Connected to Collection ES")
except Exception as e:
    extended_logger.error(e)

# ******************************************************************************************************** 

# returns a list that contains all the CSV files in the 'path_to_dir' directory
def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]


# creates a dataframe from a CSV file. Input 'file' is of structure C:\\path\\to\\dir\\file.csv
def df_from_path(file):
    file = file.replace("\\\\","\\")
    
    try:
        temp_df = pd.read_csv(file)
    except Exception as e:
        file = os.path.normpath(file)
        temp_df = pd.read_csv(file)
    
    # remove the 'desc' column, its always empty
    temp_df = temp_df.drop(['desc'], axis=1)
    
    return temp_df


# reads a .txt file that contains keywords, and creates a list containing these keywords
def make_list_from_file(input_file):
    # input is a txt file, output is a list containing each line of input
    temp_list = []
    
    # append to list each line of the file
    for line in input_file:
        temp_list.append(line.strip())

    # return the list
    return temp_list


# receive English keyword lists as inputs
# return 3 separate lists of scored English articles
def eng_score_routine(climate_input_list, covid_input_list, immigration_input_list):
    
    #  START WITH CLIMATE KEYWORDS 
    extended_logger.info("Scoring climate articles...")
    climate_articles = climateScoringV2(climate_input_list, "en")
    
    # CONTINUE WITH COVID KEYWORDS
    extended_logger.info("Scoring covid articles...")
    covid_articles = covidScoringV2(covid_input_list, "en")
    
    # END WITH IMMIGRATION KEYWORDS
    extended_logger.info("Scoring immigration articles...")
    immigration_articles = immigrationScoringV2(immigration_input_list, "en")
    
    return climate_articles, covid_articles, immigration_articles


# Used in Spanish articles to remove words like "del", "la", "los" etc. from queries
# Input is a list that contains the keywords
# Output is a list that has been cleaned from the aforementioned words
def keywordsCleaner(input_list):
    
    result = input_list
    
    forbidden_words = ["del", "el", "en", "los", "la", "de", "a", "las", "del", "de", "&", "la", "por", "ante", "través"]
    
    for word in forbidden_words:
        if word in input_list:
            result.remove(word)
    
    return result


# Creates a query string from a list of keywords, suitable for Solr queries
# Input is a list of keywords
# Output is a string
def queryFromKeywordsList(input_list):
    
    # if there are more than two words, we need to concat some strings together
    if len(input_list) > 1:
        # first part of the string must be -> text: " keyword1 "
        tok1 = ["""text: " """ + input_list[0] + """ " """]
        tokens_list = []
        # then, we have to add the "AND" field on the querystring, and append more text: keyword pairs
        for index in range(1, len(input_list)):
            tokens_list.append(""" AND text: " """ + input_list[index] + """ " """)
        
        tokens_full = tok1 + tokens_list
        result = ''.join(tokens_full)
        return result
    
    # if there is only one word, then we construct a simple query string
    else:
        # so the query is something like that -> text: " keyword "
        return ("""text: " """ + input_list[0] + """ " """)
    

# Used in testing, checks how many scrapped documents have been assigned with a query field
def docChecker(input_list):
    counter = 0
    
    for doc in input_list:
        if 'query' in doc:
            counter += 1
            
    return counter


# receive Spanish keyword lists as inputs
# return 3 separate lists of scored Spanish articles
def es_score_routine(climate_input_list, covid_input_list, immigration_input_list):
    #  START WITH CLIMATE KEYWORDS 
    extended_logger.info("Scoring Spanish climate articles...")
    climate_articles = climateScoringV2(climate_input_list, "es")
    
    # CONTINUE WITH COVID KEYWORDS
    extended_logger.info("Scoring Spanish covid articles...")
    covid_articles = covidScoringV2(covid_input_list, "es")
    
    # END WITH IMMIGRATION KEYWORDS
    extended_logger.info("Scoring Spanish immigration articles...")
    immigration_articles = immigrationScoringV2(immigration_input_list, "es")
    
    return climate_articles, covid_articles, immigration_articles


# Receives a list of scored articles. Also receices language input ("en", or "es")
# Uploads to designated MongoDB collection, based on language
def upload_documents(input_list, lang):
    if lang == "en":
        for document in input_list:
            collection_en.insert_one(document)
    else:
        for document in input_list:
            collection_es.insert_one(document)



# Query Solr Db using the query string input, and returns the JSON document
def get_solr_data(query):
    
    solr_params = {
        "indent":"true",
        "q.op":"OR",
        "q":query,
        "rows": 7000,
        "wt":"json"
    }
    

    # Fetch data from Solr with HTTP Basic Authentication
    #auth_header = base64.b64encode(f"{solr_username}:{solr_password}".encode('utf-8')).decode('utf-8')
    #headers = {'Authorization': f'Basic {auth_header}'}
    
    # Set the headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_key}"
    }
    
    response = requests.get(solr_url, params=solr_params, headers=headers)
    try:
        solr_data = json.loads(response.text)
    except:
        extended_logger.error(query)
    
    documents = solr_data['response']['docs']
    
    return documents


# -------------------- NEW FUNCTIONS START -------------------- #
# Receives a list containg climate keywords
# Also receives language ("en" or "es")
# Returns a list of articles, scored for climate keywords
def climateScoringV2(climate_input_list, lang):
    
    id_list = []
    documents_list = [] # this is going to be a large list!
    
    for item in climate_input_list:
        
        # split keywords on space key
        tokens = item.split(" ")
        
        # remove de, de la, en, los, etc
        clean = keywordsCleaner(tokens)
        
        query_string = queryFromKeywordsList(clean)
        
        # replace "/", "(" and ")" from the query string
        query_string.replace("/", " ")
        query_string.replace("(", " ")
        query_string.replace(")", " ")
        
        # create a query string from the cleaned keywords list
        my_query = query_string
        
        documents = get_solr_data(my_query)
        
        for doc in documents:
            # the unique key for each document is the field "doc"
            key_id = doc["id"]
            
            # add the key to the list, if it isn't there yet
            if key_id not in id_list:
                id_list.append(key_id)
                
                # set score to 1, and add it to the document
                climate_score = 1
                
                doc["climate_score"] = climate_score
                
                # update document list of found words
                climate_list_found_words = []
                climate_list_found_words.append(item)
                
                doc["climate_found_keywords"] = climate_list_found_words
                
                # save the updated document to our list
                documents_list.append(doc)
                
            # if the key already exists, we need to update the document
            else:
                # found the document with the specific "doc" id
                # loop all documents...
                index = 0 # update index after each false iteration
                for document in documents_list:
                    # ... and update only the one that we want
                    if document["id"] == key_id:
                        
                        # score already exists, update it
                        climate_score = document["climate_score"]
                        climate_score = climate_score + 1
                        document["climate_score"] = climate_score
                        
                        # found words list already exists, update it
                        climate_list_found_words = document["climate_found_keywords"]
                        climate_list_found_words.append(item)
                        document["climate_found_keywords"] = climate_list_found_words
                        
                        # document has been modified, update the list
                        documents_list[index] = document
                    else:
                        # this is not the document we are looking for, increase the index
                        index = index + 1
    
    extended_logger.info("Number of Climate scored articles " + str(len(documents_list)))
    # climate scoring is finished, now let's add the query string to the body of each json item
    
    # create lists with the names of each file
    # each name, is essentially the query that was done for that dataset
    if lang == "en":
        filenames_1 = find_csv_filenames(path_EN_CH_HOW)
        filenames_2 = find_csv_filenames(path_EN_CH_IS)
        filenames_3 = find_csv_filenames(path_EN_CH_WHAT)
    else:
        filenames_1 = find_csv_filenames(path_ES_CH_AQ)
        filenames_2 = find_csv_filenames(path_ES_CH_CO)
        filenames_3 = find_csv_filenames(path_ES_CH_ES)
        
    
    # each item in the documents_list has a key labelled "id", which is a web link
    # this web link is unique
    # so, for each item in the list, get that link and find where it exists, in which csv file
    
    for doc in documents_list:
        doc_link = doc["id"]
    
        for name in filenames_1:
            if lang == "en":
                links_df = df_from_path(path_EN_CH_HOW + "\\" + name)
            else:
                links_df = df_from_path(path_ES_CH_AQ + "\\" + name)
        
            # check if the link exists in the dataframe. If it does, return the filename
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_CH_HOW.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_CH_AQ.split("\\")
                    doc["query"] = tok[-1]
                    
                
        # if it wasn't in the 1st directory, check the 2nd
        for name in filenames_2:
            if lang == "en":
                links_df = df_from_path(path_EN_CH_IS + "\\" + name)
            else:
                links_df = df_from_path(path_ES_CH_CO + "\\" + name)
                
        
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_CH_IS.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_CH_CO.split("\\")
                    doc["query"] = tok[-1]
                    
                
        # if it wasn't in the 2nd, it must be on the 3rd. Do a check just in case
        for name in filenames_3:
            if lang == "en":
                links_df = df_from_path(path_EN_CH_WHAT + "\\" + name)
            else:
                links_df = df_from_path(path_ES_CH_ES + "\\" + name)
        
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_CH_WHAT.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_CH_ES.split("\\")
                    doc["query"] = tok[-1]
                    
                
    num = docChecker(documents_list)
    extended_logger.info("Number of documents with query field: " + str(num))
    
    # documents list is fully updated now
    return documents_list


# Receives a list containg covid keywords
# Also receives language ("en" or "es")
# Returns a list of articles, scored for covid keywords
def covidScoringV2(covid_input_list, lang):
    
    id_list = []
    documents_list = [] # this is going to be a large list!
    
    for item in covid_input_list:
        # split keywords on space key
        tokens = item.split(" ")
        
        # remove de, de la, en, los, etc
        clean = keywordsCleaner(tokens)
        
        query_string = queryFromKeywordsList(clean)
        
        # replace "/", "(" and ")" from the query string
        query_string.replace("/", " ")
        query_string.replace("(", " ")
        query_string.replace(")", " ")
        
        # create a query string from the cleaned keywords list
        my_query = query_string
        
        documents = get_solr_data(my_query)
        
        for doc in documents:
            # the unique key for each document is the field "doc"
            key_id = doc["id"]
            
            # add the key to the list, if it isn't there yet
            if key_id not in id_list:
                id_list.append(key_id)
                
                # set score to 1, and add it to the document
                covid_score = 1
                
                doc["covid_score"] = covid_score
                
                # update document list of found words
                covid_list_found_words = []
                covid_list_found_words.append(item)
                
                doc["covid_found_keywords"] = covid_list_found_words
                
                # save the updated document to our list
                documents_list.append(doc)
                
            # if the key already exists, we need to update the document
            else:
                # found the document with the specific "doc" id
                # loop all documents...
                index = 0 # update index after each false iteration
                for document in documents_list:
                    # ... and update only the one that we want
                    if document["id"] == key_id:
                        
                        # score already exists, update it
                        covid_score = document["covid_score"]
                        covid_score = covid_score + 1
                        document["covid_score"] = covid_score
                        
                        # found words list already exists, update it
                        covid_list_found_words = document["covid_found_keywords"]
                        covid_list_found_words.append(item)
                        document["covid_found_keywords"] = covid_list_found_words
                        
                        # document has been modified, update the list
                        documents_list[index] = document
                    else:
                        # this is not the document we are looking for, increase the index
                        index = index + 1
    
    extended_logger.info("Number of Covid scored articles " + str(len(documents_list)))
    # COVID scoring is finished, now let's add the query string to the body of each json item
    
    # create lists with the names of each file
    # each name, is essentially the query that was done for that dataset
    if lang == "en":
        filenames_1 = find_csv_filenames(path_EN_CV_SH)
        filenames_2 = find_csv_filenames(path_EN_CV_WA)
        filenames_3 = find_csv_filenames(path_EN_CV_WH)
    else:
        filenames_1 = find_csv_filenames(path_ES_CV_DE)
        filenames_2 = find_csv_filenames(path_ES_CV_FU)
        filenames_3 = find_csv_filenames(path_ES_CV_QU)
    
    # each item in the documents_list has a key labelled "id", which is a web link
    # this web link is unique
    # so, for each item in the list, get that link and find where it exists, in which csv file
    
    for doc in documents_list:
        doc_link = doc["id"]
    
        for name in filenames_1:
            if lang == "en":
                links_df = df_from_path(path_EN_CV_SH + "\\" + name)
            else:
                links_df = df_from_path(path_ES_CV_DE + "\\" + name)
        
            # check if the link exists in the dataframe. If it does, return the filename
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_CV_SH.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_CV_DE.split("\\")
                    doc["query"] = tok[-1]
                
        # if it wasn't in the 1st directory, check the 2nd
        for name in filenames_2:
            if lang == "en":
                links_df = df_from_path(path_EN_CV_WA + "\\" + name)
            else:
                links_df = df_from_path(path_ES_CV_FU + "\\" + name)
        
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_CV_WA.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_CV_FU.split("\\")
                    doc["query"] = tok[-1]
                
        # if it wasn't in the 2nd, it must be on the 3rd. Do a check just in case
        for name in filenames_3:
            if lang == "en":
                links_df = df_from_path(path_EN_CV_WH + "\\" + name)
            else:
                links_df = df_from_path(path_ES_CV_QU + "\\" + name)
        
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_CV_WH.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_CV_QU.split("\\")
                    doc["query"] = tok[-1]
    
                
    num = docChecker(documents_list)
    extended_logger.info("Number of documents with query field: " + str(num))
    
    # documents list is fully updated now
    return documents_list


# Receives a list containg immigration keywords
# Also receives language ("en" or "es")
# Returns a list of articles, scored for immigration keywords
def immigrationScoringV2(immigration_input_list, lang):
    
    id_list = []
    documents_list = [] # this is going to be a large list!
    
    for item in immigration_input_list:
        my_query = """text:" """ + item + """ " """
        
        documents = get_solr_data(my_query)
        
        for doc in documents:
            # the unique key for each document is the field "doc"
            key_id = doc["id"]
            
            # add the key to the list, if it isn't there yet
            if key_id not in id_list:
                id_list.append(key_id)
                
                # set score to 1, and add it to the document
                immigration_score = 1
                
                doc["immigration_score"] = immigration_score
                
                # update document list of found words
                immigration_list_found_words = []
                immigration_list_found_words.append(item)
                
                doc["immigration_found_keywords"] = immigration_list_found_words
                
                # save the updated document to our list
                documents_list.append(doc)
                
            # if the key already exists, we need to update the document
            else:
                # found the document with the specific "doc" id
                # loop all documents...
                index = 0 # update index after each false iteration
                for document in documents_list:
                    # ... and update only the one that we want
                    if document["id"] == key_id:
                        
                        # score already exists, update it
                        immigration_score = document["immigration_score"]
                        immigration_score = immigration_score + 1
                        document["immigration_score"] = immigration_score
                        
                        # found words list already exists, update it
                        immigration_list_found_words = document["immigration_found_keywords"]
                        immigration_list_found_words.append(item)
                        document["immigration_found_keywords"] = immigration_list_found_words
                        
                        # document has been modified, update the list
                        documents_list[index] = document
                    else:
                        # this is not the document we are looking for, increase the index
                        index = index + 1
    
    extended_logger.info("Number of immigration scored articles " + str(len(documents_list)))
    # immigration scoring is finished, now let's add the query string to the body of each json item
    
    # create lists with the names of each file
    # each name, is essentially the query that was done for that dataset
    if lang == "en":
        filenames_1 = find_csv_filenames(path_EN_IM_AR)
        filenames_2 = find_csv_filenames(path_EN_IM_HO)
        filenames_3 = find_csv_filenames(path_EN_IM_IS)
    else:
        filenames_1 = find_csv_filenames(path_ES_IM_CO)
        filenames_2 = find_csv_filenames(path_ES_IM_LA)
        filenames_3 = find_csv_filenames(path_ES_IM_RE)
    
    # each item in the documents_list has a key labelled "id", which is a web link
    # this web link is unique
    # so, for each item in the list, get that link and find where it exists, in which csv file
    
    for doc in documents_list:
        doc_link = doc["id"]
    
        for name in filenames_1:
            if lang == "en":
                links_df = df_from_path(path_EN_IM_AR + "\\" + name)
            else:
                links_df = df_from_path(path_ES_IM_CO + "\\" + name)
        
            # check if the link exists in the dataframe. If it does, return the filename
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_IM_AR.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_IM_CO.split("\\")
                    doc["query"] = tok[-1]
                
        # if it wasn't in the 1st directory, check the 2nd
        for name in filenames_2:
            if lang == "en":
                links_df = df_from_path(path_EN_IM_HO + "\\" + name)
            else:
                links_df = df_from_path(path_ES_IM_LA + "\\" + name)
        
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_IM_HO.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_IM_LA.split("\\")
                    doc["query"] = tok[-1]
                
        # if it wasn't in the 2nd, it must be on the 3rd. Do a check just in case
        for name in filenames_3:
            if lang == "en":
                links_df = df_from_path(path_EN_IM_IS + "\\" + name)
            else:
                links_df = df_from_path(path_ES_IM_RE + "\\" + name)
        
            check = (links_df['link'].eq(doc_link)).any()
            if(check):
                if lang == "en":
                    tok = path_EN_IM_IS.split("\\")
                    doc["query"] = tok[-1]
                else:
                    tok = path_ES_IM_RE.split("\\")
                    doc["query"] = tok[-1]
    
                
    num = docChecker(documents_list)
    extended_logger.info("Number of documents with query field: " + str(num))
    
    # documents list is fully updated now
    return documents_list


    
# -------------------- NEW FUNCTIONS END -------------------- #

# ******************************************************************************************************** 

# ******************************************************************************************************** 

def main():
    # create lists from txt files first
    List_climate_english = make_list_from_file(file_climate_english)
    
    List_climate_spanish = make_list_from_file(file_climate_spanish)
    
    List_covid_english = make_list_from_file(file_covid_english)
    
    List_covid_spanish = make_list_from_file(file_covid_spanish)
    
    List_immigration_english = make_list_from_file(file_immigration_english)
    
    List_immigration_spanish = make_list_from_file(file_immigration_spanish)
    
    # get English articles with their scores. This is a list of JSON documents
    '''EN_climate_scored, EN_covid_score, EN_immigration_scored = eng_score_routine(List_climate_english, 
                                                                                 List_covid_english, 
                                                                                 List_immigration_english)'''
    
    # get Spanish articles with their scores. This is a list of JSON documents
    ES_climate_scored, ES_covid_score, ES_immigration_scored = es_score_routine(List_climate_spanish, 
                                                                                List_covid_spanish, 
                                                                                List_immigration_spanish)
    
    # upload English articles to MongoDB
    '''extended_logger.info("uploading documents...")
    upload_documents(EN_climate_scored, "en")
    upload_documents(EN_covid_score, "en")
    upload_documents(EN_immigration_scored, "en")'''
    
    extended_logger.info("uploaded English articles")
    
    # upload Spanish articles to MongoDB
    upload_documents(ES_climate_scored, "es")
    upload_documents(ES_covid_score, "es")
    upload_documents(ES_immigration_scored, "es")
    
    extended_logger.info("uploaded Spanish articles")
    
    # after the uploading process, clean MongoDB from duplicates. Yes, there will be duplicates!
    merger()

# ******************************************************************************************************** 

if __name__ == '__main__':
    main()