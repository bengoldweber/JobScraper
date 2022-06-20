from bs4 import BeautifulSoup
import requests


def html_code(url):
	# pass the url
	# into getdata function
	htmldata = getdata(url)
	soup = BeautifulSoup(htmldata, 'html.parser')
	# return html code
	return (soup)


def getdata(url):
	r = requests.get(url)
	return r.text
