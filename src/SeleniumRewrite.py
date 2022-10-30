import time

from bs4 import BeautifulSoup
from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.common.by import By

import database_connectivity
from nltk.corpus import stopwords
import logging
import sys
import generics
from src import indeed_scrapers
from src.indeed_scrapers import scrape_JobsListingPageSoup, return_insights
import pandas as pd
from selenium.webdriver.chrome.options import Options

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



chromedriver_autoinstaller.install()  # Check if the current version of chromedriver exists
                                      # and if it doesn't exist, download it automatically,
                                      # then add chromedriver to path


def initialize_driver():
	"""
	:return: browser driver to use elsewhere
	"""
	chrome_options = Options()
	chrome_options.headless = False # also works

	# chrome_options.headless = True # also works
	driver = webdriver.Chrome(options=chrome_options)
	driver.get("https://www.indeed.com/")

	return driver



jbdb = database_connectivity.JobsDB()

#create tables if they dont exist. Skip if they do
jbdb.generate_job_tables_prechecks()

#set stopword type to english
stop = stopwords.words('english')

#initlize logger as logger
logger = set_loggers()

#grab search params
jobs_query_results = jbdb.query("select * from Job_SearchParams")



def grab_job_details(soup, url):
	"""
	:param url:
	:return:
	"""
	jobs_details = {}
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
		sleep_per_request = 1
		logger.info(f"Grabbing core details for {url}")
		driver.get(url)
		#time.sleep(.2)
		if "Checking if the site connection is secure" in driver.page_source:
			input("Please fix the cloudflare check then hit enter:")
			sleep_per_request = sleep_per_request + 1
			time.sleep(3)

		time.sleep(sleep_per_request)
		page_source  = driver.page_source
		soup = BeautifulSoup(page_source, 'html.parser')
		job_dict = grab_job_details(soup, url)
		job_list.append(job_dict)




	jobs_df = pd.DataFrame(job_list)
	jobs_df['Activity'].str.replace("^\['|'\]$","")

	#jobs_df['Activity'].str.replace('[^\w\s]', '')

	logger.info(f"Assembling insights")
	jobs_df['Description'] = jobs_df['Description'].str.replace("'", "", regex=False)
	jobs_df['JobLink'] = jobs_df['JobLink'].str.replace("https://www.indeed.com", "", regex=False)
	logger.info(f"Done grabbing core details")
	return jobs_df




def scrape_jobs(search_params):
	url_list = indeed_scrapers.get_PaginationDepthList(search_params)


	for URL in url_list:
		driver.get(URL)
		page_source  = driver.page_source
		page_jobs, k_url = scrape_JobsListingPageSoup(URL, page_source)
		jbdb.pandas_to_db_snipits(page_jobs, k_url, search_params)

	searchtime = search_params['SearchBatchTime']
	rows = jbdb.query(f'SELECT JobLink FROM JOBS where SearchBatchTime="{searchtime}"')
	url_list = []
	for row in rows:
		url_list.append(search_params['domain'] + str(row[0]))

	jobs_df = grab_bulk_job_details(url_list)
	jbdb.pandas_to_db_details(jobs_df)
	logger.info("indeed scraping function compleated")


for job_query in jobs_query_results:
	jobs_dict = {'job': job_query[0].replace(" ", "%20"),
	             'load_chunk': '/jobs?q=',
	             'location': job_query[2],  # .replace(" ", "%2C+"),
	             'domain': job_query[1],
	             'searchdepth': job_query[3],
	             'jobSearchIndex': job_query[4],
	             'SearchBatchTime': generics.format_time()
	             }

driver = initialize_driver()

scrape_jobs(jobs_dict)

