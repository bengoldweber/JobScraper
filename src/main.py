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



def check_tables_exist(table_name):
	cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name="{table_name}"''')
	# if the count is 1, then table exists
	if cursor.fetchone()[0] == 1:
		logger.info('Jobs Table  exist')
		return 0
	else:
		logger.warning('Jobs Table doesnt exist')
		return 1


def pandas_to_DB_details(df):
	for i, row in df.iterrows():
		d_JobLink = row['JobLink']
		d_description = row['Description']
		d_activity = row['Activity']
		d_insights = row['Insights']
		values = str("('" + d_JobLink + "', '" + d_description + "', '" + d_activity + "', '" + d_insights + "')")
		insert_into = "INSERT OR IGNORE INTO JOB_Details (JobLink, Description, Activity, Insights) VALUES"
		sqlite_insert_query = insert_into + values
		try:
			cursor.execute(sqlite_insert_query)
		except:
			logger.error(sqlite_insert_query)

	conn.commit()


def return_insights(insight_soup):
	insights_list = []
	for insight in insight_soup:
		insights_list.append(insight.get_text())

	return insights_list


def grab_job_details(url):
	"""
	:param url:
	:return:
	"""
	jobs_details = {}
	soup = html_code(url)
	jobs_details['JobLink'] = str(url)
	try:
		jobs_details['Description'] = str(soup.find("div", {"class": "jobsearch-jobDescriptionText"}).get_text())
	except:
		jobs_details['Description'] = "NaN"
	try:
		jobs_details["Insights"] = str(return_insights(soup.find_all("p", {"class": "jobsearch-HiringInsights-entry"})))
	except:
		jobs_details["Insights"] = "Nan"
	try:
		jobs_details["Activity"] = str(
			return_insights(soup.find_all("p", {"class": "jobsearch-HiringInsights-entry--bullet"})))
	except:
		jobs_details["Activity"] = "NaN"

	return jobs_details


def grab_bulk_job_details(url_list):
	job_list = []
	for url in url_list:
		job_dict = grab_job_details(url)
		job_list.append(job_dict)

	jobs_df = pd.DataFrame(job_list)
	clear_list = ['Insights', 'Activity']
	for item in clear_list:
		jobs_df[item] = jobs_df[item].str.replace('[', "", regex=False)
		jobs_df[item] = jobs_df[item].str.replace(']', "", regex=False)
		jobs_df[item] = jobs_df[item].str.replace("'", "", regex=False)

	jobs_df['Description'] = jobs_df['Description'].str.replace("'", "", regex=False)
	jobs_df['JobLink'] = jobs_df['JobLink'].str.replace("https://www.indeed.com", "", regex=False)
	pandas_to_DB_details(jobs_df)


def get_job_titles(soup):
	""" get list of jobs from beutiful soup """
	jobs_list = []
	for item in soup.select('a[class*=jcs-JobTitle]'):
		jobs_list.append(item.get_text())
	return jobs_list


# soup.select('a[class*=jcs-JobTitle]'):
def get_jobs(soup):
	"""
	:param soup:
	"""
	jobs_list = []
	jobs_soup = soup.find_all('div', {'class': 'job_seen_beacon'})
	for job_html in jobs_soup:
		parsed_job = get_job(job_html)
		jobs_list.append(parsed_job)

	return jobs_list


def get_job(job_soup):
	"""
	:param job_soup:
	"""
	dict_jobs = {}
	# dict_jobs['JobLink'] = job_soup.find("a", {"class": "jcs-JobTitle"})['href']
	# dict_jobs['Title'] = job_soup.find("a", {"class": "jcs-JobTitle"}).get_text()
	# dict_jobs['Company'] = job_soup.find("span", {"class": "companyName"}).get_text()
	# dict_jobs['Location'] = job_soup.find("div", {"class": "companyLocation"}).get_text()
	# dict_jobs['Salary'] = job_soup.find("div", {"class": "metadata salary-snippet-container"}).get_text()
	# dict_jobs['Rating'] = job_soup.find("span", {"class": "ratingNumber"}).get_text()
	# dict_jobs['JobSnipit'] = job_soup.find("div", {"class": "job-snippet"}).get_text()

	# dict_jobs = {}

	try:
		dict_jobs['JobLink'] = job_soup.find("a", {"class": "jcs-JobTitle"})['href']
	except:
		dict_jobs['JobLink'] = "NaN"

	try:
		dict_jobs['Title'] = job_soup.find("a", {"class": "jcs-JobTitle"}).get_text()
	except:
		dict_jobs['Title'] = "NaN"

	try:
		dict_jobs['Company'] = job_soup.find("span", {"class": "companyName"}).get_text()
	except:
		dict_jobs['Company'] = "NaN"

	try:
		dict_jobs['Location'] = job_soup.find("div", {"class": "companyLocation"}).get_text()
	except:
		dict_jobs['Location'] = "NaN"

	try:
		dict_jobs['Salary'] = job_soup.find("div", {"class": "metadata salary-snippet-container"}).get_text()
	except:
		dict_jobs['Salary'] = "NaN"

	try:
		dict_jobs['Rating'] = job_soup.find("span", {"class": "ratingNumber"}).get_text()
	except:
		dict_jobs['Rating'] = "NaN"

	try:
		dict_jobs['JobSnipit'] = job_soup.find("div", {"class": "job-snippet"}).get_text()
	except:
		dict_jobs['JobSnipit'] = "NaN"

	return dict_jobs


def getdata(url):
	r = requests.get(url)
	return r.text


def html_code(url):
	# pass the url
	# into getdata function
	htmldata = getdata(url)
	soup = BeautifulSoup(htmldata, 'html.parser')
	# return html code
	return (soup)


def scrape_JobsListingPage(url):
	soup = html_code(url)
	jobs_list = get_jobs(soup)
	page_jobs = pd.DataFrame(jobs_list)
	pandas_to_DB_snipits(page_jobs, url)


def pandas_to_DB_snipits(df, url):
	for i, row in df.iterrows():
		d_Link = row['JobLink'].replace("'", "''")
		d_Title = row['Title'].replace("'", "''")
		d_Company = row['Company'].replace("'", "''")
		d_Location = row['Location'].replace("'", "''")
		d_Salary = row['Salary'].replace("'", "''")
		d_Snipit = row['JobSnipit'].replace("'", "''")
		d_Rating = row['Rating'].replace("'", "''")
		d_SearchParams = url.replace("'", "''")
		values = str(
			"('" + d_Link + "', '" + d_Title + "', '" + d_Company + "', '" + d_Location + "', '" +
			d_Salary + "', '" + d_Snipit + "', '" + d_Rating + "', '" + d_SearchParams + "')")
		insertInto = "INSERT OR IGNORE INTO JOBS (JobLink, Title, Company, Location, Salary, JobSnipit, Rating, SearchParams) VALUES"
		full_insert = insertInto + values
		sqlite_insert_query = full_insert
		try:
			cursor.execute(sqlite_insert_query)
		except:
			print("FAILURE" + sqlite_insert_query)

	conn.commit()


def get_pagination_urls(url):
	"""
	:param url: url to get pagination urls from
	"""
	soup = html_code(url)
	pagination_urls = soup.find('ul', {'class': 'pagination-list'}).find_all("li")
	urls_list = []
	for item in pagination_urls:
		try:
			urls_list.append(item.find("a")['href'])
		except:
			print("none returned")
	return urls_list

# grabbed_url = item.find("a")['href']
# print(grabbed_url)

def get_PaginationDepthList(dict_urlrules, depth):
	urls_list = []
	for x in range(depth):
		start = f"&start={x}0"
		if x == 0:
			url_string = dict_urlrules['domain'] + dict_urlrules['load_chunk'] + dict_urlrules['job'] + "&l+=" + dict_urlrules['location']
		else:
			url_string = dict_urlrules['domain'] + dict_urlrules['load_chunk'] + dict_urlrules['job'] + "&l+=" + dict_urlrules['location'] + start
		urls_list.append(url_string)
	return urls_list


def generate_table_checks(table, DDL_List):
	if check_tables_exist(table) == 1:
		for Count, DDL in enumerate(DDL_List):
			try:
				cursor.execute(DDL)
				logger.info(f"DDL list item {Count} executed for {table}")
			except sqlite3.Error as er:
				logger.error(f"SQLITE3 table generation error for {table} on DDL list item {Count}")
	else:
		logger.info(f"{table} already exists, skipping DDL LIST execution")


if __name__ == '__main__':
	stop = stopwords.words('english')
	logging.basicConfig(
		level=logging.NOTSET,
		format='%(asctime)s|%(name)s|%(levelname)s|%(message)s (%(filename)s:%(lineno)d)',
		handlers=[
			logging.FileHandler("std.log"),
			logging.StreamHandler(sys.stdout)
		]
	)
	logging.getLogger("urllib3").setLevel(logging.WARNING)
	logger = logging.getLogger()
	# create console handler with a higher log level
	logger.disabled = False

	database = "jobs_db.sqlite"

	try:
		conn = sqlite3.connect(database)
	except sqlite3.OperationalError:
		logger.error('SQLITE OPERATIONAL ERROR')
	cursor = conn.cursor()
	JOBS_DDLS = ["""
			create table Jobs
			(
			    "index"      INTEGER
			        constraint Jobs_pk
			            primary key autoincrement,
			    JobLink      TEXT,
			    Title        TEXT,
			    Company      TEXT,
			    Location     TEXT,
			    Salary       TEXT,
			    Rating       TEXT,
			    JobSnipit    TEXT,
			    SearchParams integer,
			    Timestamp    Datetime default CURRENT_TIMESTAMP
			);
	""", """create unique index Jobs_temp_JobLink_uindex
			    on Jobs (JobLink);"""]

	JOBS_DETAILS_DDLS = ["""

	create table Job_Details
	(
	    description BLOB,
	    Activity    TEXT,
	    Insights    TEXT,
	    Timestamp   DATETIME default CURRENT_TIMESTAMP,
	    JobLink     TEXT
	);
	""", """ create unique index Job_Details_JobLink_uindex  on Job_Details (JobLink);""", """
	create index ix_Job_Details_index
	    on Job_Details ("index");
	"""]

	generate_table_checks("Jobs", JOBS_DDLS)
	generate_table_checks("Job_Details", JOBS_DETAILS_DDLS)

	dict_url = {}
	dict_url['job'] = "Data Science"
	dict_url['job'] = dict_url['job'].replace(" ", "%20")
	dict_url['load_chunk'] = "/jobs?q="
	dict_url['location'] = "Kensington%2C+MD"
	# dict_url['location'] = ""
	dict_url['domain'] = "https://www.indeed.com"
	starting_url = dict_url['domain'] + "/jobs?q=" + dict_url['job'] + "&l+=" + dict_url['location']
	page_depth = 1
	#url_list = get_pagination_urls(starting_url)
	url_list = get_PaginationDepthList(dict_url, page_depth)
	for url in url_list:
		logger.info("URL searching is " + str(url))
		scrape_JobsListingPage(url)

	cursor.execute("SELECT JobLink FROM JOBS")
	rows = cursor.fetchall()
	url_list = []
	for row in rows:
		url_list.append(dict_url['domain'] + str(row[0]))

	grab_bulk_job_details(url_list)
	print("done")
	# scrape_JobsListingPage(starting_url)

	cursor.close()
	conn.close()

