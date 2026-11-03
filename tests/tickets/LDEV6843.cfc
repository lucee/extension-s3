/**
 * LDEV-6843: the exceptions of the S3 functions carry the HTTP status and the S3 error code (and the
 * request ids) as structured data: e.errorCode, e.additional.statusCode, e.additional.requestId, ...
 * The type and the message of the exceptions are not changed.
 * https://luceeserver.atlassian.net/browse/LDEV-6843
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6843: structured details of the S3 exceptions", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6843" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="s3Read() of a missing object: NoSuchKey, 404", body=function() {
				var e = catchIt( function() {
					s3Read( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "missing.txt" ) );
				});
				expect( e.errorCode ).toBe( "NoSuchKey" );
				expect( e.additional.statusCode ).toBe( 404 );
				// the message is still the one with the error code
				expect( e.message ).toInclude( "error-code:NoSuchKey" );
				expect( e.type ).toBe( "org.lucee.extension.resource.s3.S3Exception" );
			});

			it( title="s3ReadBinary() and s3Download() of a missing object: NoSuchKey, 404", body=function() {
				var e = catchIt( function() {
					s3ReadBinary( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "missing.bin" ) );
				});
				expect( e.errorCode ).toBe( "NoSuchKey" );
				expect( e.additional.statusCode ).toBe( 404 );

				e = catchIt( function() {
					s3Download( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "missing.bin" ), file=getTempFile( getTempDirectory(), "ldev6843" ) );
				});
				expect( e.errorCode ).toBe( "NoSuchKey" );
				expect( e.additional.statusCode ).toBe( 404 );
			});

			it( title="the request id is part of the details when the server sends one", body=function() {
				var e = catchIt( function() {
					s3Read( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "missing.txt" ) );
				});
				// the servers differ in what they send (MinIO and AWS: yes), so only check the type
				if ( structKeyExists( e.additional, "requestId" ) ) {
					expect( e.additional.requestId ).toBeString();
					expect( len( e.additional.requestId ) ).toBeGT( 0 );
				}
			});

			it( title="s3GetMetaData() of a missing object (HEAD without body): NotFound, 404", body=function() {
				var e = catchIt( function() {
					s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=CustomS3::key( ctx, "missing.txt" ) );
				});
				expect( e.errorCode ).toBe( "NotFound" );
				expect( e.additional.statusCode ).toBe( 404 );
				// not changed
				expect( e.message ).toInclude( "404 Not Found" );
			});

			it( title="s3Delete() of a missing object: NotFound, 404, with bucket and object", body=function() {
				var obj = CustomS3::key( ctx, "missing.txt" );
				var e = catchIt( function() {
					s3Delete( bucketName=ctx.bucket, objectName=obj );
				});
				expect( e.errorCode ).toBe( "NotFound" );
				expect( e.additional.statusCode ).toBe( 404 );
				expect( e.additional.bucket ).toBe( ctx.bucket );
				expect( e.additional.objectName ).toBe( obj );
				expect( e.message ).toInclude( "does not exist" );
			});

			it( title="a missing bucket: NoSuchBucket, 404", body=function() {
				var bucket = ctx.bucket & "-nosuchbucket";
				var e = catchIt( function() {
					s3Read( bucketName=bucket, objectName="a.txt" );
				});
				expect( e.errorCode ).toBe( "NoSuchBucket" );
				expect( e.additional.statusCode ).toBe( 404 );
			});

			it( title="s3Copy() of a missing object: NoSuchKey, 404", body=function() {
				var e = catchIt( function() {
					s3Copy( srcBucketName=ctx.bucket, srcObjectName=CustomS3::key( ctx, "missing.txt" ), trgBucketName=ctx.bucket, trgObjectName=CustomS3::key( ctx, "target.txt" ) );
				});
				expect( e.errorCode ).toBe( "NoSuchKey" );
				expect( e.additional.statusCode ).toBe( 404 );
				expect( e.type ).toBe( "org.lucee.extension.resource.s3.S3Exception" );
			});

			it( title="s3Copy() that is refused by the server: the message tells why", body=function() {
				var obj = CustomS3::key( ctx, "same.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );
				// a copy of an object onto itself without any change is refused (400) by AWS and MinIO. Some servers
				// (Wasabi) accept it, there is nothing to check then.
				var e = "";
				try {
					s3Copy( srcBucketName=ctx.bucket, srcObjectName=obj, trgBucketName=ctx.bucket, trgObjectName=obj );
				}
				catch ( any ex ) {
					e = ex;
				}
				if ( isStruct( e ) ) {
					expect( e.additional.statusCode ).toBe( 400 );
					expect( len( e.errorCode ) ).toBeGT( 0 );
					// still starts as before, and now has the reason of the server
					expect( left( e.message, 16 ) ).toBe( "could not copy [" );
					expect( e.message ).toInclude( "error-code:" );
				}
			});

			it( title="wrong credentials: 403", body=function() {
				var host = ctx.cred.HOST;
				if ( reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", host ) ) host = "http://" & reReplaceNoCase( host, "^localhost", "127.0.0.1" ); // 2.0 has no pathStyleAccess, a name like localhost would be addressed bucket.localhost
				var obj = CustomS3::key( ctx, "a.txt" );
				var e = catchIt( function() {
					s3Read( bucketName=ctx.bucket, objectName=obj, accessKeyId=ctx.cred.ACCESS_KEY_ID, secretAccessKey="wrong" & ctx.cred.SECRET_KEY, host=host );
				});
				// servers that don't check the signature (like moto without auth) answer 404 for the missing object
				expect( [ 403, 404 ] ).toInclude( e.additional.statusCode );
				if ( e.additional.statusCode == 403 ) {
					expect( e.errorCode ).toBeString();
					expect( len( e.errorCode ) ).toBeGT( 0 );
					expect( e.errorCode ).notToBe( "NoSuchKey" );
				}
			});

			it( title="errors that did not come from the server have no status", body=function() {
				var e = catchIt( function() {
					s3Read( bucketName="", objectName="a.txt" );
				});
				expect( e ).toHaveKey( "message" );
				expect( structKeyExists( e.additional, "statusCode" ) ).toBeFalse();
			});

		});
	}

	/** runs the closure, returns the catch struct of the exception, fails if there is none */
	private struct function catchIt( required function fn ) {
		try {
			fn();
		}
		catch ( any e ) {
			return e;
		}
		fail( "an exception was expected" );
	}
}
