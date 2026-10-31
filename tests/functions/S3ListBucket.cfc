component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3ListBucket()", body=function() {
			it(title="check region with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials());
				testUDF(Util::getBackBlazeCredentials());
				testCFC(Util::getBackBlazeCredentials());
				testPrefix(Util::getBackBlazeCredentials());
			});	

			it(title="check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials());
				testUDF(Util::getAWSCredentials());
				testCFC(Util::getAWSCredentials());
				testPrefix(Util::getAWSCredentials());
			});	

			it(title="check with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials());
				testUDF(Util::getWasabiCredentials());
				testCFC(Util::getWasabiCredentials());
				testPrefix(Util::getWasabiCredentials());
			});		

			it(title="check with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials());
				testUDF(Util::getGoogleCredentials());
				testCFC(Util::getGoogleCredentials());
				testPrefix(Util::getGoogleCredentials());
			});			
	
		});
	}


	private function testit(cred) {
		try {
			// create variables
			var bucketName=Util::createBucketName("list-bucket");
			var objectName="sub/test.txt";
			
			Util::deleteIfExists(cred,bucketName,objectName);
			

			// create source bucket
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:"Susi Sorglos",
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host= ( isNull(cred.HOST) ? nullvalue() : cred.HOST ) );
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
			var bucketName=Util::createBucketName("list-bucket-cfc");
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

	/**
	 * LDEV-5170: the optional argument "prefix" is handed to S3, so only matching keys come back.
	 * Without the argument (or with an empty one) the whole bucket is listed as before.
	 */
	private function testPrefix(cred) {
		try {
			var bucketName=Util::createBucketName("list-bucket-prefix");
			var objectNames=["images/originals/a.txt","images/originals/b.txt","images/thumbs/a.txt","other.txt"];
			var host=(isNull(cred.HOST)?nullvalue():cred.HOST);

			// create empty bucket
			Util::deleteIfExists(cred,bucketName);
			S3CreateBucket(
					bucketName:bucketName,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);

			// create objects
			loop array=objectNames item="local.objectName" {
				S3Write(
					value:"Susi Sorglos",
					bucketName:bucketName,  objectName:objectName,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			}

			// no prefix: everything, same as before
			var all=S3ListBucket(bucketName:bucketName, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			assertEquals(4, all.recordcount);

			// empty prefix is the same as no prefix
			var empty=S3ListBucket(bucketName:bucketName, prefix:"", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			assertEquals(4, empty.recordcount);

			// deepest "folder"
			var originals=S3ListBucket(bucketName:bucketName, prefix:"images/originals/", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			assertEquals(2, originals.recordcount);
			assertEquals("images/originals/a.txt,images/originals/b.txt", arrayToList(queryColumnData(originals,"objectName")));

			// parent "folder" includes its sub folders
			var images=S3ListBucket(bucketName:bucketName, prefix:"images/", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			assertEquals(3, images.recordcount);

			// a prefix is not limited to folders
			var partial=S3ListBucket(bucketName:bucketName, prefix:"oth", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			assertEquals(1, partial.recordcount);
			assertEquals("other.txt", partial.objectName);

			// nothing matches
			var none=S3ListBucket(bucketName:bucketName, prefix:"does/not/exist/", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:host);
			assertEquals(0, none.recordcount);

			// prefix together with a listener and blockfactor
			var res="";
			S3ListBucket(
				bucketName:bucketName,
				prefix:"images/",
				listener:function (data){
					res&=data.recordcount&";";
				},
				blockfactor:2,
				accessKeyId:cred.ACCESS_KEY_ID,
				secretAccessKey:cred.SECRET_KEY,
				host:host
			);
			assertEquals("2;1;", res);
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
			var bucketName=Util::createBucketName("list-bucket-udf");
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