component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Copy()", body=function() {
			it(title="check region with blackbaze",skip=isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=getBackBlazeCredentials();
				
				// create variables
				var srcBucketName=cred.PREFIX&"src-filecopy";
				var trgBucketName=cred.PREFIX&"trg-filecopy";
				var srcObjectName="src/test.txt";
				var trgObjectName="trg/test.txt";

				// create source bucket
				if(!S3Exists( 
					bucketName:srcBucketName,  objectName:srcObjectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY,host:cred.HOST)) {
					S3Write( 
						location:"us-east-1",
						value:"Susi Sorglos",
						bucketName:srcBucketName,  objectName:srcObjectName, 
						accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY,host:cred.HOST);
				}
				// copy
				S3Copy( 
					location:"us-east-1",
					srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:trgObjectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY,host:cred.HOST);
				
				
				
				//assertEquals("http://bundle-download.s3.eu-west-1.amazonaws.com/sentry-log4j-1.7.22.jar", res);
			});	

			it(title="check region with amazon",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				
				// create variables
				var srcBucketName=cred.PREFIX&"src-filecopy";
				var trgBucketName=cred.PREFIX&"trg-filecopy";
				var srcObjectName="src/test.txt";
				var trgObjectName="trg/test.txt";

				// create source bucket
				if(!S3Exists( 
					bucketName:srcBucketName,  objectName:srcObjectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY)) {
					S3Write( 
						location:"us-east-1",
						value:"Susi Sorglos",
						bucketName:srcBucketName,  objectName:srcObjectName, 
						accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY);
				}
				// copy
				S3Copy( 
					location:"us-east-1",
					srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:trgObjectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY);
				
				
				
				//assertEquals("http://bundle-download.s3.eu-west-1.amazonaws.com/sentry-log4j-1.7.22.jar", res);
			});			
	
		});
	}

	private function doFind(value){
		return value EQ "world";
	}
	
	private boolean function isNotSupported() {
		res= getCredentials();
		return isNull(res) || len(res)==0;
	}
	private struct function getCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		var SECRET_KEY=server.system.environment.S3_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY};
	}

	private boolean function isBackBlazeNotSupported() {
		res= getBackBlazeCredentials();
		return isNull(res) || len(res)==0;
	}
	private struct function getBackBlazeCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_BACKBLAZE_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_BACKBLAZE_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var HOST=server.system.environment.S3_BACKBLAZE_HOST?:nullValue();
		if(isNull(HOST) || isEmpty(HOST)) return {};
		
		var PREFIX=server.system.environment.S3_BACKBLAZE_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,HOST:HOST,PREFIX:PREFIX};
	}
}