import logging
import matplotlib
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
import string
import nltk
from nltk.corpus import stopwords
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from textblob import Word
import textblob
import spacy
from spacy import displacy
from collections import Counter
import spacy
import logging
import os
import sys
import generics
import indeed_scrapers
import database_connectivity
import datetime


def scrape_indeed(search_params):
	"""Pulls in indeed job posting info to the Jobs and Jobs detail DB's
	based on the paramaters passed as a dict called search params

	here is a sample search param dict

		search_dict = {
		'job':"Data Science".replace(" ", "%20"),
		'load_chunk':'/jobs?q=',
		'location':"Kensington%2C+MD",
		'domain':"https://www.indeed.com",
		'searchdepth':1
	}

	"""
	url_list = indeed_scrapers.get_PaginationDepthList(search_params)

	for url in url_list:
		logger.info("URL searching is " + str(url))
		page_jobs, k_url = indeed_scrapers.scrape_JobsListingPage(url)
		jbdb.pandas_to_db_snipits(page_jobs, k_url, search_params)

	searchtime = search_params['SearchBatchTime']
	rows = jbdb.query(f'SELECT JobLink FROM JOBS where SearchBatchTime="{searchtime}"')
	url_list = []
	for row in rows:
		url_list.append(search_params['domain'] + str(row[0]))

	jobs_df = indeed_scrapers.grab_bulk_job_details(url_list)
	jbdb.pandas_to_db_details(jobs_df)
	logger.info("indeed scraping function compleated")


def set_loggers():
	""" sets up logger configurations for the jobs script
	this is the funciton where you should define logger levels, filters, file output etc
	"""
	logging.basicConfig(
		level=logging.NOTSET,
		format='%(asctime)s|%(name)s|%(levelname)s|%(message)s (%(filename)s:%(lineno)d)',
		handlers=[
			logging.FileHandler("App.log"),
			logging.StreamHandler(sys.stdout)
		]
	)
	logging.getLogger("urllib3").setLevel(logging.WARNING)
	logger = logging.getLogger("JobScraper")
	# create console handler with a higher log level
	logger.disabled = False
	return logger


if __name__ == '__main__':
	stop = stopwords.words('english')
	logger = set_loggers()
	jbdb = database_connectivity.JobsDB()
	jbdb.generate_job_tables_prechecks()
	# job_querys = jbdb.query("select * from Job_SearchParams")
	jobs_query_results = jbdb.query("select * from Job_SearchParams")
	for job_query in jobs_query_results:
		jobs_dict = {'job': job_query[0].replace(" ", "%20"),
		             'load_chunk': '/jobs?q=',
		             'location': job_query[2].replace(" ", "%2C+"),
		             'domain': job_query[1],
		             'searchdepth': job_query[3],
		             'jobSearchIndex': job_query[4],
		             'SearchBatchTime': generics.format_time()
		             }
		logger.info("running search based on search params " + str(jobs_dict))
		scrape_indeed(jobs_dict)
	# scrape_indeed(search_dict)
	logger.info("program completed")
	jbdb.__exit__






