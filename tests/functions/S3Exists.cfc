component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3ClearBucket()", body=function() {
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


	private function testit(cred) {
		try {
			// create variables
			var bucketName=cred.PREFIX&"-exists";
			var objectName="sub/test.txt";
			

			// create source bucket
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:"Susi Sorglos",
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}
			// existing bucket 
			assertTrue(
				S3Exists( 
					bucketName:bucketName,  
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);
			// existing bucket/object
			assertTrue(
				S3Exists( 
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);

			// NOT existing bucket 
			assertFalse(
				S3Exists( 
					bucketName:bucketName&"NOT",  
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);
			// NOT existing bucket/object
			assertFalse(
				S3Exists( 
					bucketName:bucketName&"NOT",  objectName:objectName&"NOT", 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);
			// NOT existing object
			assertFalse(
				S3Exists( 
					bucketName:bucketName,  objectName:objectName&"NOT", 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}


	private function doFind(value){
		return value EQ "world";
	}
	
}