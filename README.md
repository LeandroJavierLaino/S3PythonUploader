# S3PythonUploader

Is a S3 upload script, with a sync approach because boto3 hasn't this feature for S3. This sync approach have a bug if you delete a file from the bucket and try upload again you will get a message that was already uploaded, a work around is upload through the web interface.

You may need the following dependencies:
	* boto3
	* simplejson
	* botocore