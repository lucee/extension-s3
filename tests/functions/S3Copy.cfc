component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Copy()", body=function() {
			it(title="check with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
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

			// ACL: the buckets are created by this test, AWS is left out because new AWS buckets do not allow ACLs (BucketOwnerEnforced)
			it(title="check ACL with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testACL(Util::getWasabiCredentials(),"eu-central-1");
			});

			it(title="check ACL with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testACL(Util::getGoogleCredentials(),"us-east-1");
			});

			it(title="check ACL with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testACL(Util::getBackBlazeCredentials(),"us-east-005");
			});

			// no S3 connection needed, the ACL is validated before anything is sent
			it(title="ACL as array of structs is parsed as ACL grants", body = function( currentSpec ) {
				var msg=Util::messageOfFailure(function() {
					S3Copy( srcBucketName:"ldev-acl-src", srcObjectName:"a.txt", trgBucketName:"ldev-acl-trg", trgObjectName:"b.txt",
						acl:[{"group":"no-such-group","permission":"READ"}], accessKeyId:"dummy", secretAccessKey:"dummy" );
				});
				assertTrue(findNoCase("invalid group definition",msg)>0, "unexpected message [#msg#]");
			});

			it(title="ACL with an invalid canned name is rejected", body = function( currentSpec ) {
				var msg=Util::messageOfFailure(function() {
					S3Copy( srcBucketName:"ldev-acl-src", srcObjectName:"a.txt", trgBucketName:"ldev-acl-trg", trgObjectName:"b.txt",
						acl:"no-such-acl", accessKeyId:"dummy", secretAccessKey:"dummy" );
				});
				assertTrue(findNoCase("invalid access control list definition",msg)>0, "unexpected message [#msg#]");
			});
	
		});
	}


	private function testit(cred,region) {
		try {
			// create variables
			var srcBucketName=Util::createBucketName("src-filecopy");
			var trgBucketName=Util::createBucketName("trg-filecopy");
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
			// a freshly created bucket is not always visible right away (NoSuchBucket), wait until the object can be read
			Util::waitUntilReadable(cred,srcBucketName,srcObjectName);
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


	private function testACL(cred,region) {
		var srcBucketName=Util::createBucketName("src-aclcopy");
		var trgBucketName=Util::createBucketName("trg-aclcopy");
		var srcObjectName="src/test.txt";
		try {
			S3Write(
				location:region,
				value:"Susi Sorglos",
				bucketName:srcBucketName,  objectName:srcObjectName,
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			Util::waitUntilReadable(cred,srcBucketName,srcObjectName);

			// array of grant structs
			S3Copy(
				location:region,
				srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/grants.txt",
				acl:[{"group":"authenticated","permission":"READ"}],
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			Util::waitUntilReadable(cred,trgBucketName,"trg/grants.txt");
			var acl=S3GetACL(
				bucketName:trgBucketName, objectName:"trg/grants.txt",
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertTrue(Util::hasGrant(acl,"authenticated","READ"), "READ for authenticated users is missing: #serializeJSON(acl)#");

			// canned ACL
			S3Copy(
				location:region,
				srcBucketName:srcBucketName,  srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/canned.txt",
				acl:"public-read",
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			Util::waitUntilReadable(cred,trgBucketName,"trg/canned.txt");
			var acl=S3GetACL(
				bucketName:trgBucketName, objectName:"trg/canned.txt",
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