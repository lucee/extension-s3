component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	// LDEV-5175: conditional requests (ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince)
	// for S3Read, S3ReadBinary, S3Download, S3Copy (conditions apply to the source object),
	// S3Write and S3Upload (writes only support ifMatch and ifNoneMatch)

	variables.PRECONDITION_TYPE="s3.preconditionfailed";

	function run( testResults , testBox ) {
		describe( title="Test suite for conditional S3 requests", body=function() {

			it(title="read with conditions, check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testRead(Util::getAWSCredentials());
			});

			it(title="copy with conditions, check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testCopy(Util::getAWSCredentials());
			});

			it(title="write with conditions, check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testWrite(Util::getAWSCredentials());
			});

			// no S3 connection needed, the conditions are validated before anything is sent
			it(title="ifModifiedSince that is not a date is rejected by S3Read", body = function( currentSpec ) {
				var msg=Util::messageOfFailure(function() {
					S3Read( bucketName:"ldev-cond-src", objectName:"a.txt", ifModifiedSince:"not a date", accessKeyId:"dummy", secretAccessKey:"dummy" );
				});
				assertTrue(findNoCase("ifModifiedSince",msg)>0, "unexpected message [#msg#]");
			});

			it(title="ifUnmodifiedSince that is not a date is rejected by S3Copy", body = function( currentSpec ) {
				var msg=Util::messageOfFailure(function() {
					S3Copy( srcBucketName:"ldev-cond-src", srcObjectName:"a.txt", trgBucketName:"ldev-cond-trg", trgObjectName:"b.txt", ifUnmodifiedSince:"not a date", accessKeyId:"dummy", secretAccessKey:"dummy" );
				});
				assertTrue(findNoCase("ifUnmodifiedSince",msg)>0, "unexpected message [#msg#]");
			});

		});
	}


	private function putTestObject(cred,bucketName,objectName,value) {
		S3Write( value:value, bucketName:bucketName, objectName:objectName, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
		// a freshly created bucket is not always visible right away (NoSuchBucket), wait until the object can be read
		Util::waitUntilReadable(cred,bucketName,objectName);
		// the ETag of the object, without quotes
		return S3GetMetaData( bucketNameOrPath:bucketName, objectName:objectName, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)).eTag;
	}

	private function testRead(cred) {
		try {
			var bucketName=Util::createBucketName("cond-read");
			var objectName="sub/test.txt";
			var content="Susi Sorglos";
			var etag=putTestObject(cred,bucketName,objectName,content);
			var past=createDateTime(2000,1,1,0,0,0);
			var future=dateAdd("d",1,now());
			var target=getTempFile(getTempDirectory(),"s3cond","txt");

			// without conditions nothing changes
			assertEquals(content, S3Read( bucketName:bucketName, objectName:objectName, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));

			// ifMatch
			assertEquals(content, S3Read( bucketName:bucketName, objectName:objectName, ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(content, S3Read( bucketName:bucketName, objectName:objectName, ifMatch:'"#etag#"', accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Read( bucketName:bucketName, objectName:objectName, ifMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));

			// ifNoneMatch
			assertEquals(content, S3Read( bucketName:bucketName, objectName:objectName, ifNoneMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Read( bucketName:bucketName, objectName:objectName, ifNoneMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));

			// ifModifiedSince
			assertEquals(content, S3Read( bucketName:bucketName, objectName:objectName, ifModifiedSince:past, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Read( bucketName:bucketName, objectName:objectName, ifModifiedSince:future, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));

			// ifUnmodifiedSince
			assertEquals(content, S3Read( bucketName:bucketName, objectName:objectName, ifUnmodifiedSince:future, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Read( bucketName:bucketName, objectName:objectName, ifUnmodifiedSince:past, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));

			// the error is not swallowed in case the object does not exist, that is a different error
			assertNotEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Read( bucketName:bucketName, objectName:"no/such/object.txt", ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));

			// S3ReadBinary
			assertEquals(content, toString(S3ReadBinary( bucketName:bucketName, objectName:objectName, ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))));
			assertEquals(content, toString(S3ReadBinary( bucketName:bucketName, objectName:objectName, ifNoneMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3ReadBinary( bucketName:bucketName, objectName:objectName, ifMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3ReadBinary( bucketName:bucketName, objectName:objectName, ifUnmodifiedSince:past, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));

			// S3Download
			assertEquals(content, toString(S3Download( bucketName:bucketName, objectName:objectName, ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))));
			assertEquals(content, S3Download( bucketName:bucketName, objectName:objectName, charset:"UTF-8", ifNoneMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Download( bucketName:bucketName, objectName:objectName, ifNoneMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Download( bucketName:bucketName, objectName:objectName, ifModifiedSince:future, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			// nothing is written to the target when the condition is not met
			fileDelete(target);
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Download( bucketName:bucketName, objectName:objectName, target:target, ifMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertFalse(fileExists(target));
			S3Download( bucketName:bucketName, objectName:objectName, target:target, ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertEquals(content, fileRead(target));
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) rethrow;
		}
		finally {
			if(!isNull(target) && fileExists(target)) fileDelete(target);
			Util::deleteBucketEL(cred,bucketName);
		}
	}

	private function testCopy(cred) {
		try {
			var srcBucketName=Util::createBucketName("cond-src");
			var trgBucketName=Util::createBucketName("cond-trg");
			var srcObjectName="src/test.txt";
			var content="Susi Sorglos";
			var etag=putTestObject(cred,srcBucketName,srcObjectName,content);
			var past=createDateTime(2000,1,1,0,0,0);
			var future=dateAdd("d",1,now());

			// conditions that are not met, nothing is copied
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/ifmatch.txt", ifMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/ifnonematch.txt", ifNoneMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/ifmodifiedsince.txt", ifModifiedSince:future, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/ifunmodifiedsince.txt", ifUnmodifiedSince:past, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			for(var name in ["ifmatch","ifnonematch","ifmodifiedsince","ifunmodifiedsince"]) {
				assertFalse(S3Exists( bucketName:trgBucketName, objectName:"trg/#name#.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)), "[#name#] was copied although the condition was not met");
			}

			// conditions that are met
			S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/a.txt", ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/b.txt", ifNoneMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/c.txt", ifModifiedSince:past, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			S3Copy( srcBucketName:srcBucketName, srcObjectName:srcObjectName, trgBucketName:trgBucketName, trgObjectName:"trg/d.txt", ifUnmodifiedSince:future, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			for(var name in ["a","b","c","d"]) {
				Util::waitUntilReadable(cred,trgBucketName,"trg/#name#.txt");
				assertEquals(content, S3Read( bucketName:trgBucketName, objectName:"trg/#name#.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			}
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) rethrow;
		}
		finally {
			Util::deleteBucketEL(cred,srcBucketName);
			Util::deleteBucketEL(cred,trgBucketName);
		}
	}

	private function testWrite(cred) {
		try {
			var bucketName=Util::createBucketName("cond-write");
			var file=getTempFile(getTempDirectory(),"s3cond","txt");
			fileWrite(file,"from file");

			// create the bucket and an object
			var etag=putTestObject(cred,bucketName,"existing.txt","version 1");

			// ifNoneMatch="*": only write when the object does not exist yet
			S3Write( value:"created", bucketName:bucketName, objectName:"new.txt", ifNoneMatch:"*", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertEquals("created", S3Read( bucketName:bucketName, objectName:"new.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Write( value:"overwritten", bucketName:bucketName, objectName:"existing.txt", ifNoneMatch:"*", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals("version 1", S3Read( bucketName:bucketName, objectName:"existing.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));

			// ifMatch: only write when the current ETag matches
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Write( value:"overwritten", bucketName:bucketName, objectName:"existing.txt", ifMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals("version 1", S3Read( bucketName:bucketName, objectName:"existing.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			S3Write( value:"version 2", bucketName:bucketName, objectName:"existing.txt", ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertEquals("version 2", S3Read( bucketName:bucketName, objectName:"existing.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// the ETag changed with the write, so the old one does not match anymore
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Write( value:"version 3", bucketName:bucketName, objectName:"existing.txt", ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals("version 2", S3Read( bucketName:bucketName, objectName:"existing.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));

			// S3Upload
			S3Upload( bucketName:bucketName, objectName:"uploaded.txt", source:file, ifNoneMatch:"*", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			assertEquals("from file", S3Read( bucketName:bucketName, objectName:"uploaded.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Upload( bucketName:bucketName, objectName:"uploaded.txt", source:file, ifNoneMatch:"*", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			assertEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Upload( bucketName:bucketName, objectName:"uploaded.txt", source:file, ifMatch:"no-such-etag", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
			var uploadedEtag=S3GetMetaData( bucketNameOrPath:bucketName, objectName:"uploaded.txt", accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)).eTag;
			S3Upload( bucketName:bucketName, objectName:"uploaded.txt", source:file, ifMatch:uploadedEtag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));

			// a missing object with ifMatch is a different error (NoSuchKey), not a precondition failure
			assertNotEquals(PRECONDITION_TYPE, Util::typeOfFailure(function() {
				S3Write( value:"x", bucketName:bucketName, objectName:"no/such/object.txt", ifMatch:etag, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}));
		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) rethrow;
		}
		finally {
			if(!isNull(file) && fileExists(file)) fileDelete(file);
			Util::deleteBucketEL(cred,bucketName);
		}
	}

}
