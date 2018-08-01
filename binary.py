import sys
import json
import base64
import requests
import numpy as np

import bitstring
import array
from bitstring import MmapByteArray
from bitstring import Bits, BitArray, ConstByteStore, ByteStore

from EpiSettings import EpiSettings

class S3Driver(object):
	def __init__(self, backend = 'prod'):
		epiSettings = EpiSettings.getS3Settings( backend = backend )
		self.s3Settings = epiSettings
		self.debug = False



	def tranlateData(self, content):
		# turn payload into a BitArray
		payloadBits = BitArray(content)
		# Get header slice, 16 elements * 64 bits = 1024
		headerDataBits = payloadBits[0:1024]
		#get numpy array from header bytes
		headerArray = np.fromstring(headerDataBits.bytes, dtype=np.int64)

		build = headerArray[0]
		dataFormat = headerArray[1]
		secondsFromGMT = headerArray[2]
		userID = headerArray[3]
		sessionID = headerArray[4]
		partID = headerArray[5]
		accelerometerByteOffset = headerArray[6]
		heartRateByteOffset = headerArray[7]
		reserved = headerArray[8:16]

		# Get accelerometer slice
		# goes from end of header 1024 to begining of HeartRate
		accelerometerByteOffset = accelerometerByteOffset*8
		accelerometerDataBits = payloadBits[1024:heartRateByteOffset*8]
		#get numpy array of accelerometer tuple from accelerometer bytes
		accelerometerDataType = [('x', 'f8'), ('y', 'f8'), ('z', 'f8'), ('dateTime', 'f8')]
		accelerometerArray = np.fromstring(accelerometerDataBits.bytes, dtype=accelerometerDataType)

		# Get HeartRate slice
		# goes from end of accelerometer to end of data
		heartRateBitOffset = heartRateByteOffset*8
		hearRateDataBits = payloadBits[heartRateBitOffset:len(payloadBits)]
		#get numpy array of hearRate tuple from heartRate bytes
		heartRateDataType = [('hr1', 'i4'), ('hr2', 'i4'), ('dateTime', 'f8')]
		heartRateArray = np.fromstring(hearRateDataBits.bytes, dtype=heartRateDataType)

		epiData = type('', (), {})()
		epiData.dataFormat = dataFormat
		epiData.secondsFromGMT = secondsFromGMT
		epiData.userID = userID
		epiData.sessionID = sessionID
		epiData.partID = partID
		epiData.accelerometerData = accelerometerArray
		epiData.heartRateData = heartRateArray

		return epiData

	def MakeS3Requests(self, urls):
		order = True # Default to order by descending order
		return self.MakeS3RequestsWithOrder(urls, order)

	def MakeS3RequestsWithOrder(self, urls, order):
		epiDataCollection = type('', (), {})()
		epiDataCollection.epiHeaderData = type('', (), {})()
		epiDataCollection.epiHeaderData.secondsFromGMT = 0
		epiDataCollection.epiHeaderData.userID = 0
		epiDataCollection.epiHeaderData.sessionID = []
		epiDataCollection.epiHeaderData.partID = []
		epiDataCollection.epiSensorData = type('', (), {})()
		epiDataCollection.badPayloads = []
		accelerometerDataType = [('x', 'f8'), ('y', 'f8'), ('z', 'f8'), ('dateTime', 'f8')]
		epiDataCollection.epiSensorData.accelerometerData = np.array([], dtype=accelerometerDataType)
		heartRateDataType = [('hr1', 'i4'), ('hr2', 'i4'), ('dateTime', 'f8')]
		epiDataCollection.epiSensorData.heartRateData = np.array([], dtype=heartRateDataType)

		# FLipping the order of the s3 links backed on which order by in query. (Payload data is aways in ace)
		if order:
			urls = np.fliplr([urls])[0]
		print("number of payloads: {}".format(len(urls)))
		for idx, val in enumerate(urls):
			request = self.MakeS3Request(val)
			if hasattr(request, 'status_code') and request.status_code == 200:
				try:
					epiData = self.tranlateData(request.content)
					if idx == 0:
						epiDataCollection.epiHeaderData.secondsFromGMT = epiData.secondsFromGMT
						epiDataCollection.epiHeaderData.userID = epiData.userID

					epiDataCollection.epiHeaderData.sessionID.append(epiData.sessionID)
					epiDataCollection.epiHeaderData.partID.append(epiData.partID)
					epiDataCollection.epiSensorData.accelerometerData = np.concatenate([epiDataCollection.epiSensorData.accelerometerData, epiData.accelerometerData])
					epiDataCollection.epiSensorData.heartRateData = np.union1d(epiDataCollection.epiSensorData.heartRateData, epiData.heartRateData)
				except ValueError as e:
					file_name = val.split("/")[-1]
					try:
						file_name = base64.urlsafe_b64decode(file_name).decode()
					except:
						print("failed to base64 decode sensor payload name")
					epiDataCollection.badPayloads.append("s3_link[{}]: {}".format(idx, file_name))
			else:
				print("Error making request! - ", val)
		if len(epiDataCollection.badPayloads) > 0:
			print("Bad payloads: {}".format(len(epiDataCollection.badPayloads)))
			if self.debug:
				for i, badFile in enumerate(epiDataCollection.badPayloads):
					print(badFile)
		print("Done.")

		return epiDataCollection

	def MakeS3Request(self, url):
		req = None
		try:
			req = requests.get(url, headers = self.s3Settings.headers)
		except:
			print ("Error in Request - Check S3 Settings")
		return(req)