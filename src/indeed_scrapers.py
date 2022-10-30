# soup.select('a[class*=jcs-JobTitle]'):
from bs4 import BeautifulSoup

import generics
import pandas as pd
import database_connectivity
import logging

indeedscraper_logger = logging.getLogger('JobScraper.indeedscraper')

def scrape_JobsListingPageSoup(url, soup):
	soup = BeautifulSoup(soup, 'html.parser')
	jobs_list = get_jobs(soup)
	page_jobs = pd.DataFrame(jobs_list)
	return page_jobs, url


def scrape_JobsListingPage(url):
	soup = generics.html_code(url)
	jobs_list = get_jobs(soup)
	page_jobs = pd.DataFrame(jobs_list)
	return page_jobs, url


def get_jobs(soup):
	"""
	:param soup:
	"""
	jobs_list = []
	jobs_soup = soup.find_all('div', {'class': 'job_seen_beacon'})
	print(soup)
	for job_html in jobs_soup:
		parsed_job = get_job(job_html)
		jobs_list.append(parsed_job)
	print(f"JOBS LIST IS {jobs_list}")
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


def get_job_titles(soup):
	""" get list of jobs from beutiful soup """
	jobs_list = []
	for item in soup.select('a[class*=jcs-JobTitle]'):
		jobs_list.append(item.get_text())
	return jobs_list


def grab_job_details(url):
	"""
	:param url:
	:return:
	"""
	jobs_details = {}
	soup = generics.html_code(url)
	print(soup)
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

def get_pagination_urls(url):
	"""
	:param url: url to get pagination urls from
	"""
	soup = generics.html_code(url)
	pagination_urls = soup.find('ul', {'class': 'pagination-list'}).find_all("li")
	urls_list = []
	for item in pagination_urls:
		try:
			urls_list.append(item.find("a")['href'])
		except:
			print("none returned")
	return urls_list


def grab_bulk_job_details(url_list):
	job_list = []
	for url in url_list:
		indeedscraper_logger.info(f"Grabbing core details for {url}")
		job_dict = grab_job_details(url)
		job_list.append(job_dict)

	print(job_list)
	jobs_df = pd.DataFrame(job_list)
	print(jobs_df)

	clear_list = ['Insights', 'Activity']
	indeedscraper_logger.info(f"Assembling insights")
	print(clear_list)
	# for item in clear_list:
	# 	jobs_df[item] = jobs_df[item].str.replace('[', "", regex=False)
	# 	jobs_df[item] = jobs_df[item].str.replace(']', "", regex=False)
	# 	jobs_df[item] = jobs_df[item].str.replace("'", "", regex=False)

	jobs_df['Description'] = jobs_df['Description'].str.replace("'", "", regex=False)
	jobs_df['JobLink'] = jobs_df['JobLink'].str.replace("https://www.indeed.com", "", regex=False)
	indeedscraper_logger.info(f"Done grabbing core details")
	return jobs_df


def return_insights(insight_soup):
	insights_list = []
	for insight in insight_soup:
		insights_list.append(insight.get_text())
	return insights_list


def get_PaginationDepthList(dict_urlrules):
	urls_list = []
	depth = dict_urlrules['searchdepth']
	depth = int(depth)
	for x in range(depth):
		start = f"&start={x}0"
		if x == 0:
			url_string = dict_urlrules['domain'] + dict_urlrules['load_chunk'] + dict_urlrules['job'] + "&l+=" + dict_urlrules['location']
		else:
			url_string = dict_urlrules['domain'] + dict_urlrules['load_chunk'] + dict_urlrules['job'] + "&l+=" + dict_urlrules['location'] + start
		urls_list.append(url_string)
	return urls_list



def grab_bulk_job_details(driver, url_list):
	job_list = []
	for url in url_list:
		indeedscraper_logger.info(f"Grabbing core details for {url}")
		driver.get(url)
		job_dict = grab_job_details(url)
		job_list.append(job_dict)

	jobs_df = pd.DataFrame(job_list)

	clear_list = ['Insights', 'Activity']
	indeedscraper_logger.info(f"Assembling insights")

	jobs_df['Description'] = jobs_df['Description'].str.replace("'", "", regex=False)
	jobs_df['JobLink'] = jobs_df['JobLink'].str.replace("https://www.indeed.com", "", regex=False)
	indeedscraper_logger.info(f"Done grabbing core details")
	return jobs_df
