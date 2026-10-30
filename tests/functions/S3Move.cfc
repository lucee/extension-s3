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

			// ACL: the buckets are created by this test, AWS is left out because new AWS buckets do not allow ACLs (BucketOwnerEnforced)
			it(title="check ACL with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testACL(Util::getWasabiCredentials());
			});

			it(title="check ACL with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testACL(Util::getGoogleCredentials());
			});

			it(title="check ACL with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testACL(Util::getBackBlazeCredentials());
			});

			// no S3 connection needed, the ACL is validated before anything is sent
			it(title="ACL as array of structs is parsed as ACL grants", body = function( currentSpec ) {
				var msg=Util::messageOfFailure(function() {
					S3Move( srcBucketName:"ldev-acl-src", srcObjectName:"a.txt", trgBucketName:"ldev-acl-trg", trgObjectName:"b.txt",
						acl:[{"group":"no-such-group","permission":"READ"}], accessKeyId:"dummy", secretAccessKey:"dummy" );
				});
				assertTrue(findNoCase("invalid group definition",msg)>0, "unexpected message [#msg#]");
			});

			it(title="ACL with an invalid canned name is rejected", body = function( currentSpec ) {
				var msg=Util::messageOfFailure(function() {
					S3Move( srcBucketName:"ldev-acl-src", srcObjectName:"a.txt", trgBucketName:"ldev-acl-trg", trgObjectName:"b.txt",
						acl:"no-such-acl", accessKeyId:"dummy", secretAccessKey:"dummy" );
				});
				assertTrue(findNoCase("invalid access control list definition",msg)>0, "unexpected message [#msg#]");
			});
	
		});
	}


	private function testit(cred) {
		try {
			// create variables
			var id=createUniqueID();
			var srcBucketName=Util::createBucketName("src-move");
			var trgBucketName=Util::createBucketName("trg-move");
			var srcObjectName="src/test#id#.txt";
			var trgObjectName="trg/test#id#.txt";

			Util::deleteIfExists(cred,srcBucketName,srcObjectName);
			Util::deleteIfExists(cred,trgBucketName,trgObjectName);

			// create source bucket
			if(!S3Exists( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:"Susi Sorglos",
					bucketName:srcBucketName,  objectName:srcObjectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}
			// a freshly created bucket is not always visible right away (NoSuchBucket), wait until the object can be read
			Util::waitUntilReadable(cred,srcBucketName,srcObjectName);

			assertTrue(S3Exists( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertFalse(S3Exists( 
				bucketName:trgBucketName,  objectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			
			// move
			S3Move( 
				srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			assertFalse(S3Exists( 
				bucketName:srcBucketName,  objectName:srcObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertTrue(S3Exists( 
				bucketName:trgBucketName,  objectName:trgObjectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
				
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) throw e;
		}
		finally {
			Util::deleteBucketEL(cred,srcBucketName);
			Util::deleteBucketEL(cred,trgBucketName);
		}
	}


	private function testACL(cred) {
		var srcBucketName=Util::createBucketName("src-aclmove");
		var trgBucketName=Util::createBucketName("trg-aclmove");
		var id=createUniqueID();
		var objects=[
			{src:"src/grants#id#.txt", trg:"trg/grants#id#.txt"},
			{src:"src/canned#id#.txt", trg:"trg/canned#id#.txt"}
		];
		try {
			loop array=objects item="local.o" {
				S3Write(
					value:"Susi Sorglos",
					bucketName:srcBucketName,  objectName:o.src,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				Util::waitUntilReadable(cred,srcBucketName,o.src);
			}

			// array of grant structs
			S3Move(
				srcBucketName:srcBucketName,  srcObjectName:objects[1].src, trgBucketName:trgBucketName, trgObjectName:objects[1].trg,
				acl:[{"group":"authenticated","permission":"READ"}],
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertFalse(S3Exists(
				bucketName:srcBucketName,  objectName:objects[1].src,
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			Util::waitUntilReadable(cred,trgBucketName,objects[1].trg);
			var acl=S3GetACL(
				bucketName:trgBucketName, objectName:objects[1].trg,
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertTrue(Util::hasGrant(acl,"authenticated","READ"), "READ for authenticated users is missing: #serializeJSON(acl)#");

			// canned ACL
			S3Move(
				srcBucketName:srcBucketName,  srcObjectName:objects[2].src, trgBucketName:trgBucketName, trgObjectName:objects[2].trg,
				acl:"public-read",
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertFalse(S3Exists(
				bucketName:srcBucketName,  objectName:objects[2].src,
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			Util::waitUntilReadable(cred,trgBucketName,objects[2].trg);
			var acl=S3GetACL(
				bucketName:trgBucketName, objectName:objects[2].trg,
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertTrue(Util::hasGrant(acl,"all","READ"), "READ for all users is missing: #serializeJSON(acl)#");
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