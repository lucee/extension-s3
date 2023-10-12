component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Copy()", body=function() {
			it(title="check with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				throw "#structKeyList(arguments)#";
				testit(Util::getBackBlazeCredentials(),"us-east-005");
			});	

			it(title="check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials(),"us-east-1");
			});	

			it(title="check with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials(),"eu-central-1");
			});		

			it(title="check with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials(),"us-east-1");
			});			
	
		});
	}


	private function testit(cred,region) {
		try {
			// create variables
			var srcBucketName=cred.PREFIX&"src-filecopy"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var trgBucketName=cred.PREFIX&"trg-filecopy"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var srcObjectName="src/test.txt";
			var trgObjectName="trg/test.txt";
			
			Util::deleteIfExists(cred,srcBucketName,srcObjectName);
			Util::deleteIfExists(cred,trgBucketName,trgObjectName);

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
			// copy
			S3Copy( 
				location:region,
				srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			var meta=S3GetMetadata( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			assertEquals(region, meta.region);
			assertEquals(srcBucketName, meta.bucketName);
			assertEquals(srcObjectName, meta.objectName);


			var meta=S3GetMetadata( 
				bucketName:trgBucketName,  objectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			assertEquals(region, meta.region);
			assertEquals(trgBucketName, meta.bucketName);
			assertEquals(trgObjectName, meta.objectName);
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) throw e;
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