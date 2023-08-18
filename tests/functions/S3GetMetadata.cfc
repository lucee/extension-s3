component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GetMetadata()", body=function() {
			it(title="check region with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials());
			});	

			it(title="check region with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials());
			});	

			it(title="check region with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials());
			});		

			it(title="check region with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials());
			});			
	
		});
	}


	private function testit(cred) {
		try {
			// create variables
			var bucketName=cred.PREFIX&"-get-metadata";
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

			var meta=S3GetMetadata( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			throw serialize(meta);
			assertEquals(bucketName, meta.bucketName);
			assertEquals(objectName, meta.objectName);

		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
		
	}


	private function doFind(value){
		return value EQ "world";
	}
	
}