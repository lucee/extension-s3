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

		var file1=expandPath("{temp-directory}/test-s3upload1.txt");
		fileWrite(file1, "Susi Sorglos 1");
		var file2=expandPath("{temp-directory}/test-s3upload2.txt");
		fileWrite(file2, "Susi Sorglos 2");


		try {
			// create variables
			var bucketName=cred.PREFIX&"-upload"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectNameFile1="sub/file1.txt";
			var objectNameFile2="sub/file2.txt";
			
			Util::deleteIfExists(cred,bucketName,objectNameFile1);
			Util::deleteIfExists(cred,bucketName,objectNameFile2);
			

			// create string file source
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectNameFile1, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Upload( 
					source:file1,
					bucketName:bucketName,  objectName:objectNameFile1, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}

			// create file source
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectNameFile2, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Upload( 
					source:fileOpen(file2),
					bucketName:bucketName,  objectName:objectNameFile2, 
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
					bucketName:bucketName,  objectName:objectNameFile1, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);

			assertEquals(
				"Susi Sorglos 1",
				S3Read( 
					bucketName:bucketName,  objectName:objectNameFile1, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);

			assertTrue(
				S3Exists( 
					bucketName:bucketName,  objectName:objectNameFile2, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))
			);

			assertEquals(
				"Susi Sorglos 2",
				S3Read( 
					bucketName:bucketName,  objectName:objectNameFile2, 
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