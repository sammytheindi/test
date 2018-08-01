from getpass import getpass

class EpiSettings(object):
	def getDBSettings( backend = 'prod' ):
		dbSettings = type('', (), {})()
		dbSettings.dbname = "epiwatch"
		dbSettings.user = "sshah99"
		if backend == 'stage':
			dbSettings.host = "epiwatch-stage.cxoyodq5cuh1.us-east-1.rds.amazonaws.com"
		else:
			dbSettings.host = "epiwatch.cxoyodq5cuh1.us-east-1.rds.amazonaws.com"
		dbSettings.password = getpass()
		if backend == 'stage':
			dbSettings.s3_base_url = "https://tholtsj8kd.execute-api.us-east-1.amazonaws.com/stage/gettimeseries/"
		else:
			dbSettings.s3_base_url = "https://tholtsj8kd.execute-api.us-east-1.amazonaws.com/prod/gettimeseries/"
		return dbSettings

	def getS3Settings( backend = 'prod' ):
		s3Settings = type('', (), {})()
		if backend == 'stage':
			token = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxMTE3NyIsInNjb3BlcyI6WyJSRVNFQVJDSEVSIiwiVVNFUiJdLCJpc3MiOiJodHRwczovL2VwaXdhdGNoZGV2LmpoLmVkdSIsImlhdCI6MTUzMTI0MDgyOCwiZXhwIjoyMjg2MDMxMjI4fQ.Jh1ck5ZjCbZ57iTaYcHcUlyN-1MaBqgyfeZHUoEG2nE6SCsKKgtKt5UK4NmWDAwk7iL9DISQPOsQTwLRxJisBg"
		else:
			token = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxMjQ3NSIsInNjb3BlcyI6WyJSRVNFQVJDSEVSIiwiVVNFUiJdLCJpc3MiOiJodHRwczovL2VwaXdhdGNoLWFwaS5qaC5lZHUiLCJpYXQiOjE1MzEyNDA5MDksImV4cCI6MjI4NjAzMTMwOX0.9O0A_4hjfAA150zFf0WlzOjubowfb4pZfAIHiVxiYHe_BRPpiaXxd-gtWPuc9xi-bzSzRO2_FVFCQLwRMj9KZw"
		s3Settings.headers = {'X-Authorization': token}
		return s3Settings