component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3ListBucket()", body=function() {
			it(title="check region with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials());
				testUDF(Util::getBackBlazeCredentials());
				testCFC(Util::getBackBlazeCredentials());
			});	

			it(title="check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials());
				testUDF(Util::getAWSCredentials());
				testCFC(Util::getAWSCredentials());
			});	

			it(title="check with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials());
				testUDF(Util::getWasabiCredentials());
				testCFC(Util::getWasabiCredentials());
			});		

			it(title="check with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials());
				testUDF(Util::getGoogleCredentials());
				testCFC(Util::getGoogleCredentials());
			});			
	
		});
	}


	private function testit(cred) {
		try {
			// create variables
			var bucketName=cred.PREFIX&"-list-bucket-"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectName="sub/test.txt";
			
			Util::deleteIfExists(cred,bucketName,objectName);
			

			// create source bucket
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:"Susi Sorglos",
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}

			var kids=S3ListBucket(bucketName:bucketName, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY,host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertTrue(kids.recordcount>0);
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) throw e;
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}

	private function testCFC(cred) {
		try {
			// create variables
			var bucketName=cred.PREFIX&"-list-bucket-"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectNames=["sub/test1.txt","sub/test2.txt","sub/test3.txt","sub/test4.txt","sub/test5.txt"];
			
			// create empty bucket
			Util::deleteIfExists(cred,bucketName);
			S3CreateBucket( 
					bucketName:bucketName,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));

			// create objects
			loop array=objectNames item="local.objectName" {
				// create source bucket
				if(!S3Exists( 
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
					S3Write( 
						value:"Susi Sorglos",
						bucketName:bucketName,  objectName:objectName, 
						accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				}
			}

			var listener=new S3ListBucketListener();
			S3ListBucket(
				bucketName:bucketName, 
				listener:listener,
				blockfactor:3,
				accessKeyId:cred.ACCESS_KEY_ID, 
				secretAccessKey:cred.SECRET_KEY,
				host:(isNull(cred.HOST)?nullvalue():cred.HOST)
			);
			assertEquals("-->3;2;<--", listener.getData());
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) throw e;
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}

	private function testUDF(cred) {
		try {
			// create variables
			var bucketName=cred.PREFIX&"-list-bucket-"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectNames=["sub/test1.txt","sub/test2.txt","sub/test3.txt","sub/test4.txt","sub/test5.txt"];
			
			// create empty bucket
			Util::deleteIfExists(cred,bucketName);
			S3CreateBucket( 
					bucketName:bucketName,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));

			// create objects
			loop array=objectNames item="local.objectName" {
				// create source bucket
				if(!S3Exists( 
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
					S3Write( 
						value:"Susi Sorglos",
						bucketName:bucketName,  objectName:objectName, 
						accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				}
			}

			var res="";
			S3ListBucket(
				bucketName:bucketName, 
				listener:function (data){
					res&=data.recordcount&";";
        			dump(arguments);
				},
				blockfactor:3,
				accessKeyId:cred.ACCESS_KEY_ID, 
				secretAccessKey:cred.SECRET_KEY,
				host:(isNull(cred.HOST)?nullvalue():cred.HOST)
			);
			assertEquals("3;2;", res);
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