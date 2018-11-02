import boto3
import os, sys
import calendar
from datetime import datetime
import time
import threading
import simplejson as json
from multiprocessing import Pool
from functools import partial
from botocore.errorfactory import ClientError

session = boto3.Session(profile_name='ProfileName')
s3 = session.client('s3')

directory = os.path.dirname(os.path.realpath(__file__)) + '/'
bucket = 'BucketName'
topPrefix = 'Folder/Inside/The/Bucket'
sourceDir = directory + 'OriginFolder/'

dirPath = sourceDir 

#A Log object contains the path, size and when was uploaded
class LogObject(object):
	"""docstring for LogObject"""
	def __init__(self, path, fileSize, time):
		self.path = path
		self.fileSize = fileSize
		self.time = time
		
#A log object to save the upload progress
class UploadProgress(object):
	def __init__(self, filename):
		self._dirPath = dirPath
		self._size = os.path.getsize(filename)
		self._totalFiles = 0
		self._seen_so_far = 0
		self._files_so_far = 0
		self._lock = threading.Lock()

	def __call__(self, bytes_amount):
		with self._lock:
			self._seen_so_far += bytes_amount
			percentage = (self._seen_so_far / self._size) * 100
			sys.stdout.write(
				"\r%s  %s / %s  (%.2f%%)" % (
        			self._dirPath, self._seen_so_far, self._size,
        			percentage))
			sys.stdout.flush()


def upload_data(filepath):
	transfersize = 0

	relPath = os.path.relpath(filepath, sourceDir)
	
	if relPath != ".DS_Store":
		destKey = topPrefix + relPath.replace("\\","/")
		#Part added to check if file already exists on s3
		try:
			s3.head_object(Bucket=bucket, Key=destKey)
		#If not exist we upload the file
		except ClientError:
			print("\nuploading to S3: " + destKey)
			s3.upload_file(filepath, 'BucketName', destKey, ExtraArgs={'ServerSideEncryption': "AES256"}, Callback=UploadProgress(filepath))
			transfersize += os.path.getsize(filepath)
		#if exist we show the file was already uploaded			
		else:
			print("File " + destKey + " already uploaded.")

	log = LogObject(filepath,transfersize,time.ctime(time.time()))
	return log

if __name__ == '__main__':

	date = datetime.today()
	#This create a log file, first verify if the file not exist if true that not exist we create it
	if (not(os.path.isfile(directory + 'logupload' + str(date.year) + str(date.month) + str(date.day) +'.json'))):
		createLog = open(directory + 'logupload' + str(date.year) + str(date.month) + str(date.day) + '.json', 'w+')
		createLog.write("[]")
		createLog.flush()
		createLog.close()

	for (dirPath, dirnames, filenames) in os.walk(sourceDir):

		files_for_upload = []
		for idx, f in enumerate(filenames):
			filepath = os.path.join(dirPath + '/' + f)
			files_for_upload.append(filepath)

		#We use a 4 threads is the best way to run it in parallel the upload task (Disclaimer: i used 4 but you can use more o less depending your machine)
		p = Pool(4)
		totalFiles = len(files_for_upload)
		for idx, result in enumerate(p.imap_unordered(upload_data, files_for_upload)):
			#Verify if the file size uploaded is more than 0. If true you will show total files uploaded, increase the total size uploaded and record a log
			#If false increase the repeated files.
			if result.fileSize > 0 :
				print("\nuploaded to S3, file # " + str(idx+1) + " of " + str(totalFiles)) 
				transfer_size += result.fileSize+1
				#create a temporal log
				logs = [] 

				#open the log file and assing the all the data to logs
				with open(directory + 'logupload' + str(date.year) + str(date.month) + str(date.day) + '.json') as logfile_in:
					logs = json.load(logfile_in)

				#we add the log data about the file uploaded
				with open(directory + 'logupload' + str(date.year) + str(date.month) + str(date.day) + '.json', 'w+') as out_results:
					logresult = {
						"time": result.time,
						"fileSize": result.fileSize,
						"path": result.path 
					}
					logs.append(logresult)
					json.dump(logs, out_results, indent=2)

				#save the final result
				with open(directory + 'logupload' + '.json', 'w') as out_results:
					json.dump(logs, out_results, indent=2)
