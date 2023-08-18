component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Copy()", body=function() {
			it(title="check region with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials(),"us-east-5");
			});	

			it(title="check region with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials(),"us-east-1");
			});	

			it(title="check region with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials(),"eu-central-1");
			});			
	
		});
	}


	private function testit(cred,region) {
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
				location:region,
				value:"Susi Sorglos",
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY);
		}
		// copy
		S3Copy( 
			location:region,
			srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:trgObjectName, 
			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY);
		
		var meta=S3GetMetadata( 
			bucketName:srcBucketName,  objectName:srcObjectName, 
			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY);
		
		assertEquals(region, meta.region);
		assertEquals(srcBucketName, meta.bucketName);
		assertEquals(srcObjectName, meta.objectName);


		var meta=S3GetMetadata( 
			bucketName:trgBucketName,  objectName:trgObjectName, 
			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY);
		
		assertEquals(region, meta.region);
		assertEquals(trgBucketName, meta.bucketName);
		assertEquals(trgObjectName, meta.objectName);
	}


	private function doFind(value){
		return value EQ "world";
	}
	
}