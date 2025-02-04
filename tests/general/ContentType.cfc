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
			fileWrite(objectName, '<?xml version="1.0" encoding="UTF-8"?>
<test/>');
			systemOutput("--------- move ----------",1,1);
			systemOutput(objectName,1,1);
			systemOutput("s3:///#bucketName#/#objectName#",1,1);
			fileMove(objectName, "s3:///#bucketName#/#objectName#");
			assertTrue(s3getmetadata(bucketName,objectName)["contentType"]);
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}
}