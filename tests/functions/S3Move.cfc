component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Move()", body=function() {
			it(title="check with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials());
			});	

			it(title="check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials());
			});	

			it(title="check with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials());
			});		

			it(title="check with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials());
			});			
	
		});
	}


	private function testit(cred,region) {
		try {
			// create variables
			var srcBucketName=cred.PREFIX&"src-move";
			var trgBucketName=cred.PREFIX&"trg-move";
			var srcObjectName="src/test.txt";
			var trgObjectName="trg/test.txt";
			

			// create source bucket
			if(!S3Exists( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					location:region,
					value:"Susi Sorglos",
					bucketName:srcBucketName,  objectName:srcObjectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}

			assertTrue(S3Exists( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertFalse(S3Exists( 
				bucketName:trgBucketName,  objectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			
			// move
			S3Move( 
				location:region,
				srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			assertFalse(S3Exists( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertTrue(S3Exists( 
				bucketName:trgBucketName,  objectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
				
		}
		finally {
			Util::deleteBucketEL(cred,srcBucketName);
			Util::deleteBucketEL(cred,trgBucketName);
		}
	}


	private function doFind(value){
		return value EQ "world";
	}
	
}