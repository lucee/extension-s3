component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for content type", body=function() {
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
			var bucketName=Util::createBucketName("contenttype");
			var objectName="content-type.xml";
			var path="s3:///#bucketName#/#objectName#";
			var pathWithCred="s3://#cred.ACCESS_KEY_ID#:#cred.SECRET_KEY#@#(isNull(cred.HOST)?nullvalue():cred.HOST)#/#bucketName#/#objectName#";
			
			// create source bucket TODO remove true
			if(true || !S3Exists( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:'<?xml version="1.0" encoding="UTF-8"?>
<test/>',
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}

			var md=s3getmetadata(bucketName:bucketName,objectName:objectName, 
    			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));


			assertTrue(len(md["contentType"])>0);
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}
}