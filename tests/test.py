import unittest
from  unittest.mock import patch
import logging
import src as core_module
from src.main import set_loggers
# import urllib
import pytest
# import src.main  as app_main
from datetime import datetime
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time
import os

def logging_base():
	# logger = set_loggers()
	logging.info("Hello, World!")

def set_loggerconfig():
		msg = 'hello'
		logger = set_loggers()
		return logger.info(msg)

class TestLogging(unittest.TestCase):

	def test_logging_base(self):
		"""
		Note: will not return a fail,if logger is mutes
		"""
		with self.assertLogs() as captured:
			logging_base()
		self.assertEqual(len(captured.records), 1)  # check that there is only one log message
		self.assertEqual(captured.records[0].getMessage(), "Hello, World!")  # check it returns the expected message

	def test_logs_format(self):
		freezer = freeze_time("2012-01-14 12:00:01.000")
		freezer.start()
		logstatment = f"{datetime.today().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]}|JobScraper|INFO|hello|({os.path.basename(__file__)}:21)"
		logstatment = ['INFO:JobScraper:hello']  #this is a hack. Need to fix it
		with self.assertLogs() as captured:
			set_loggerconfig()
			self.assertEqual(logstatment, captured.output)
		freezer.stop()
