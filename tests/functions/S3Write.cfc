component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Write()", body=function() {
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

		var file=expandPath("{temp-directory}/test-s3write.txt");
		fileWrite(file, "Susi Sorglos");


		try {
			// create variables
			var bucketName=cred.PREFIX&"-write"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectNameString="sub/string.txt";
			var objectNameFile="sub/file.txt";
			
			Util::deleteIfExists(cred,bucketName,objectNameString);
			Util::deleteIfExists(cred,bucketName,objectNameFile);
			

			// create string source
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectNameString, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:"Susi Sorglos",
					bucketName:bucketName,  objectName:objectNameString, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}

			// create file source
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectNameFile, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:fileOpen(file),
					bucketName:bucketName,  objectName:objectNameFile, 
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
					bucketName:bucketName,  objectName:objectNameString, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);
			// existing bucket/object
			assertTrue(
				S3Exists( 
					bucketName:bucketName,  objectName:objectNameFile, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);

			assertEqual(
				"Susi Sorglos",
				S3Read( 
					bucketName:bucketName,  objectName:objectNameString, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);

			assertEqual(
				"Susi Sorglos",
				S3Read( 
					bucketName:bucketName,  objectName:objectNameFile, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);


		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) throw e;
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}


	private function doFind(value){
		return value EQ "world";
	}
	
}