import unittest
import logging

#from src.main import set_loggers


def test_logging_base():
	#logger = set_loggers()
	logging.info("Hello, World!")


class TestLogging(unittest.TestCase):

	def test_logging(self):
		"""
		Note: will not return a fail,if logger is mutes
		"""
		with self.assertLogs() as captured:
			test_logging_base()
		self.assertEqual(len(captured.records), 1)  # check that there is only one log message
		self.assertEqual(captured.records[0].getMessage(), "Hello, World!")  # check it returns the expected message
