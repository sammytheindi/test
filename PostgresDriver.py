import psycopg2
import base64

from EpiSettings import EpiSettings

class PostgresDriver(object):

	def __init__(self, backend = 'prod'):
		epiSettings = EpiSettings.getDBSettings( backend = backend )
		self.dbSettings = epiSettings
		self.connected = None

	def connectToPostgres(self):
		# Try to connect
		try: 
			self.conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (self.dbSettings.dbname, self.dbSettings.user, self.dbSettings.host, self.dbSettings.password))
			self.connected = True
			return self.connected
		except:
			print("Unable to connect to the database - Check DB Settings")
			return self.connected

	def queryRequest(self, Query):
		s3_links = []

		if self.connected:
			# Set db cursor
			cur = self.conn.cursor()

			# Try to execute query
			try:
				cur.execute(Query)

				# Query response
				rows = cur.fetchall()
				for row in rows:
					s3_links.append(self.dbSettings.s3_base_url + base64.urlsafe_b64encode(row[6].encode('UTF-8')).decode('ascii'))
			except:
				print ("Error on Query")
		else:
			print ("No DB connection")

		return s3_links