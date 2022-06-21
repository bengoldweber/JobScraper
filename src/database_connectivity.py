import logging
import sqlite3

DBconnectivity_logger = logging.getLogger('JobScraper.DBconnectivity')


class CoreDB():
	""" Database connection class"""
	__DB_LOCATION = "jobs_db.sqlite"

	def __init__(self):
		""" initalize db class vars """
		self.__db_connection = sqlite3.connect(self.__DB_LOCATION)
		self.cursor = self.__db_connection.cursor()

	def __exit__(self):
		self.cursor.close()
		# if isinstance(exc_value, Exception):
		# 	self.__db_connection.rollback()
		# else:
		# 	self.__db_connection.commit()
		self.__db_connection.close()

	def execute(self, new_data):
		self.cursor.execute(new_data)

	def query(self, query):
		self.execute(query)
		return self.cursor.fetchall()

	def commit(self):
		"""commit changes to database"""
		self.__db_connection.commit()


class JobsDB(CoreDB):
	"""
	Database connection to the Job tables, as well as useful functions to format, insert and confirm tables exists.
	Auto generates tables off of DDL if they dont.
	"""

	def __init__(self):
		# Calling constructor of
		# Base class
		CoreDB.__init__(self)
		df = ""
		url = ""

	def check_tables_exist(self, table_name):
		"""
		:param table_name:
		:return: a 1 query was successful, a 0 if it failed
		"""
		self.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name="{table_name}"''')
		# if the count is 1, then table exists
		if self.cursor.fetchone()[0] == 1:
			DBconnectivity_logger.info('Jobs Table  exist')
			return 0
		else:
			DBconnectivity_logger.warning('Jobs Table doesnt exist')
			return 1

	def generate_job_tables(self, table, DDL_List):
		""" Generate Jobs info and details tables if they dont already exist"""
		if self.check_tables_exist(table) == 1:
			for Count, DDL in enumerate(DDL_List):
				try:
					self.execute(DDL)
					DBconnectivity_logger.info(f"DDL list item {Count} executed for {table}")
				except sqlite3.Error as er:
					print("error")
					DBconnectivity_logger.error(f"SQLITE3 table generation error for {table} on DDL list item {Count}")
		else:
			print("skipping")
		DBconnectivity_logger.info(f"{table} already exists, skipping DDL LIST execution")

	def generate_job_tables_prechecks(self):
		""" defines DDLs for job tables and then passes then to table gen checks  """
		JOBS_DDLS = ["""
					create table IF NOT EXISTS Jobs
					(
					    "index"      INTEGER
			        constraint Jobs_pk
			            primary key autoincrement,
			    JobLink      TEXT
			        constraint Jobs_pk_2
			            unique,
			    Title        TEXT,
			    Company      TEXT,
			    Location     TEXT,
			    Salary       TEXT,
			    Rating       TEXT,
			    JobSnipit    TEXT,
			    SearchParams integer,
			    SearchBatchTime Datetime,
			    Timestamp    Datetime default CURRENT_TIMESTAMP,
			    SearchURL    text
					);
			""", """create unique index Jobs_temp_JobLink_uindex
					    on Jobs (JobLink);"""]
		JOBS_DETAILS_DDLS = ["""

			create table  IF NOT EXISTS Job_Details
			(
			       description BLOB,
				    Activity    TEXT,
				    Insights    TEXT,
				    Timestamp   DATETIME default CURRENT_TIMESTAMP,
				    JobLink     TEXT
				        constraint Job_Details_pk
				            primary key
			);
			""", """ 
				create unique index Job_Details_JobLink_uindex
				    on Job_Details (JobLink);
			""", """ create index ix_Job_Details_index
    on Job_Details ("index");"""]
		JOBS_SEARCH_PARAM_DDLS = [""" create table Job_SearchParams
(
    job          text,
    domain       text,
    location     text,
    search_depth integer,
    "index"      integer
        constraint Job_SearchParams_pk
            primary key autoincrement
);""", """ create unique index Job_SearchParams_index_uindex
    on Job_SearchParams ("index");"""]
		self.generate_job_tables("Jobs", JOBS_DDLS)
		self.generate_job_tables("Job_Details", JOBS_DETAILS_DDLS)
		self.generate_job_tables("Job_SearchParams", JOBS_SEARCH_PARAM_DDLS)

	def pandas_to_db_snipits(self, df, url, search_p):
		""" push pd df of job snipit info to the Jobs table """
		for i, row in df.iterrows():
			d_Link = row['JobLink'].replace("'", "''")
			d_Title = row['Title'].replace("'", "''")
			d_Company = row['Company'].replace("'", "''")
			d_Location = row['Location'].replace("'", "''")
			d_Salary = row['Salary'].replace("'", "''")
			d_Snipit = row['JobSnipit'].replace("'", "''")
			d_Rating = row['Rating'].replace("'", "''")
			d_URL = url.replace("'", "''")
			d_SearchParams = search_p['jobSearchIndex']
			d_SearchBatchTime = search_p['SearchBatchTime']
			# print(d_SearchParams)
			values = "('" + d_Link + "', '" + d_Title + "', '" + d_Company + "', '" + d_Location + "', '" + d_Salary \
			         + \
			         "', '" + d_Snipit + "', '" + d_Rating + "', '" + d_URL + "', '" + str(
				d_SearchBatchTime) + "', '" + str(d_SearchParams) + "')"
			insertInto = "INSERT OR IGNORE INTO JOBS (JobLink, Title, Company, Location, Salary, JobSnipit, Rating, " \
			             "SearchURL, SearchBatchTime, SearchParams) VALUES"
			sqlite_insert_query = insertInto + values
			# print(sqlite_insert_query)
			try:
				self.execute(sqlite_insert_query)
			except:
				print("FAILURE" + sqlite_insert_query)
		self.commit()

	def pandas_to_db_details(self, df):
		"""
		:param df: Panads DF of Job Details to opuh
		"""
		DBconnectivity_logger.info("pushing data to able")
		for i, row in df.iterrows():
			d_JobLink = row['JobLink']
			d_description = row['Description']
			d_activity = row['Activity']
			d_insights = row['Insights']
			sqlite_insert_query = "INSERT OR IGNORE INTO JOB_Details (JobLink, Description, Activity, Insights) " \
			                      "VALUES" + \
			                      str("('" + d_JobLink + "', '" + d_description + "', '" + d_activity + "', "
			                                                                                            "'" +
			                          d_insights + "')")
			try:
				self.execute(sqlite_insert_query)
			except:
				DBconnectivity_logger.error(sqlite_insert_query)
		self.commit()
