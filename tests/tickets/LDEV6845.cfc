/**
 * LDEV-6845: s3Read(), s3ReadBinary() and s3Download() have the optional arguments range and versionId,
 * s3GetMetaData() returns the versionId.
 * https://luceeserver.atlassian.net/browse/LDEV-6845
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6845: range and versionId", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6845" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			describe( "range", function() {

				beforeEach( function() {
					obj = CustomS3::key( ctx, "range.txt" );
					s3Write( bucketName=ctx.bucket, objectName=obj, value="hello world" );
				});

				it( title="without range the whole object is read", body=function() {
					expect( s3Read( bucketName=ctx.bucket, objectName=obj ) ).toBe( "hello world" );
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range="" ) ).toBe( "hello world" );
					expect( len( s3ReadBinary( bucketName=ctx.bucket, objectName=obj ) ) ).toBe( 11 );
				});

				it( title="s3Read with a range string", body=function() {
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range="0-4" ) ).toBe( "hello" );
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range="6-" ) ).toBe( "world" );
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range="bytes=3-5" ) ).toBe( "lo " );
					// the end is inclusive, a single byte
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range="4-4" ) ).toBe( "o" );
					// an end behind the size is allowed by S3
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range="6-100" ) ).toBe( "world" );
				});

				it( title="s3Read with a range struct", body=function() {
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range={ start: 0, end: 4 } ) ).toBe( "hello" );
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range={ start: 6 } ) ).toBe( "world" );
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, range={ end: 4 } ) ).toBe( "hello" );
				});

				it( title="s3Read with a range and the positional arguments", body=function() {
					// bucketName, objectName, charset, accessKeyId, secretAccessKey, host, timeout, range
					expect( s3Read( ctx.bucket, obj, "utf-8", nullValue(), nullValue(), nullValue(), 10000, "0-4" ) ).toBe( "hello" );
				});

				it( title="s3ReadBinary with a range", body=function() {
					var data = s3ReadBinary( bucketName=ctx.bucket, objectName=obj, range="6-" );
					expect( isBinary( data ) ).toBeTrue();
					expect( toString( data ) ).toBe( "world" );
					expect( toString( s3ReadBinary( bucketName=ctx.bucket, objectName=obj, range={ start: 0, end: 4 } ) ) ).toBe( "hello" );
				});

				it( title="s3Download with a range", body=function() {
					var target = getTempDirectory() & "ldev6845-" & createUUID() & ".txt";
					try {
						s3Download( bucketName=ctx.bucket, objectName=obj, target=target, range="0-4" );
						expect( fileRead( target ) ).toBe( "hello" );
					}
					finally {
						if ( fileExists( target ) ) fileDelete( target );
					}
					// to a closure
					var parts = [];
					s3Download( bucketName=ctx.bucket, objectName=obj, range="6-", target=function( string line ) {
						arrayAppend( parts, line );
						return true;
					});
					expect( arrayToList( parts, "" ) ).toBe( "world" );
				});

				it( title="an invalid range is an error", body=function() {
					expect( function() {
						s3Read( bucketName=ctx.bucket, objectName=obj, range="abc" );
					}).toThrow( message="range" );
					expect( function() {
						s3Read( bucketName=ctx.bucket, objectName=obj, range="5-2" );
					}).toThrow( message="range" );
					expect( function() {
						s3Read( bucketName=ctx.bucket, objectName=obj, range={ foo: 1 } );
					}).toThrow( message="range" );
					expect( function() {
						s3ReadBinary( bucketName=ctx.bucket, objectName=obj, range="-3" );
					}).toThrow( message="range" );
				});

				it( title="a range behind the end of the object is an error of the server", body=function() {
					expect( function() {
						s3Read( bucketName=ctx.bucket, objectName=obj, range="100-200" );
					}).toThrow();
				});

			});

			describe( "versionId", function() {

				// the versioning of a bucket is changed, so the test needs a bucket of its own (not S3_CUSTOM_BUCKET)
				it( title="read and describe the versions of an object", skip=len( CustomS3::getCredentials().BUCKET ) > 0, body=function() {

					var obj = "versioned.txt";
					enableVersioning( ctx );
					try {
						s3Write( bucketName=ctx.bucket, objectName=obj, value="version one" );
						s3Write( bucketName=ctx.bucket, objectName=obj, value="version two" );

						var versions = s3GetVersionInfo( bucketName=ctx.bucket, objectName=obj );
						expect( versions.recordCount ).toBe( 2 );
						var latest = "";
						var older = "";
						loop query=versions {
							if ( versions.isLatest ) latest = versions.id;
							else older = versions.id;
						}
						expect( len( latest ) ).toBeGT( 0 );
						expect( len( older ) ).toBeGT( 0 );

						// default: the latest version
						expect( s3Read( bucketName=ctx.bucket, objectName=obj ) ).toBe( "version two" );
						// by id
						expect( s3Read( bucketName=ctx.bucket, objectName=obj, versionId=older ) ).toBe( "version one" );
						expect( s3Read( bucketName=ctx.bucket, objectName=obj, versionId=latest ) ).toBe( "version two" );
						expect( toString( s3ReadBinary( bucketName=ctx.bucket, objectName=obj, versionId=older ) ) ).toBe( "version one" );

						// together with a range
						expect( s3Read( bucketName=ctx.bucket, objectName=obj, versionId=older, range="8-" ) ).toBe( "one" );
						expect( s3Read( bucketName=ctx.bucket, objectName=obj, versionId=latest, range="8-" ) ).toBe( "two" );

						// s3Download
						var target = getTempDirectory() & "ldev6845-" & createUUID() & ".txt";
						try {
							s3Download( bucketName=ctx.bucket, objectName=obj, target=target, versionId=older );
							expect( fileRead( target ) ).toBe( "version one" );
						}
						finally {
							if ( fileExists( target ) ) fileDelete( target );
						}

						// s3GetMetaData returns the version id
						var md = s3GetMetaData( bucketName=ctx.bucket, objectName=obj );
						expect( md ).toHaveKey( "versionId" );
						expect( md.versionId ).toBe( latest );

						// a version that does not exist
						expect( function() {
							s3Read( bucketName=ctx.bucket, objectName=obj, versionId="does-not-exist" );
						}).toThrow();
					}
					finally {
						deleteAllVersions( ctx, obj );
					}
				});

				it( title="versionId for an object of a bucket without versioning", body=function() {
					var obj = CustomS3::key( ctx, "plain.txt" );
					s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );
					// an empty value is the same as not defined
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, versionId="" ) ).toBe( "hello" );
				});

			});

		});
	}

	// ---- helpers: the extension has no function to change the versioning of a bucket, so the (few) requests
	// needed for the test are sent signed (AWS Signature Version 4) with cfhttp

	private void function enableVersioning( required struct ctx ) {
		var body = '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>';
		var rsp = signedRequest( ctx, "PUT", "/" & ctx.bucket, "versioning=", body );
		if ( rsp.status_code != 200 ) throw( message="could not enable versioning: " & rsp.status_code & " " & rsp.filecontent );
	}

	private void function deleteAllVersions( required struct ctx, required string objectName ) {
		var versions = s3GetVersionInfo( bucketName=ctx.bucket, objectName=objectName );
		loop query=versions {
			var rsp = signedRequest( ctx, "DELETE", "/" & ctx.bucket & "/" & objectName, "versionId=" & encodeForURL( versions.id ) );
			if ( rsp.status_code != 204 && rsp.status_code != 200 ) systemOutput( "LDEV6845 could not delete version: " & rsp.status_code, true );
		}
	}

	private struct function signedRequest( required struct ctx, required string method, required string path, string query="", string body="" ) {
		var cred = ctx.cred;
		var local_ = reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", cred.HOST ) > 0;
		var host = reReplaceNoCase( cred.HOST, "^https?://", "" );
		var scheme = local_ || left( cred.HOST, 7 ) == "http://" ? "http" : "https";
		var region = len( cred.REGION ) ? cred.REGION : "us-east-1";
		if ( !len( cred.REGION ) && reFindNoCase( "^s3\.([a-z0-9-]+)\.wasabisys\.com$", host ) ) {
			region = reReplaceNoCase( host, "^s3\.([a-z0-9-]+)\.wasabisys\.com$", "\1" );
		}

		var now_ = dateConvert( "local2Utc", now() );
		var amzDate = dateTimeFormat( now_, "yyyyMMdd'T'HHnnss'Z'" );
		var day = dateTimeFormat( now_, "yyyyMMdd" );
		var payloadHash = lcase( hash( body, "SHA-256", "utf-8" ) );

		var canonicalHeaders = "host:" & host & chr( 10 ) & "x-amz-content-sha256:" & payloadHash & chr( 10 ) & "x-amz-date:" & amzDate & chr( 10 );
		var signedHeaders = "host;x-amz-content-sha256;x-amz-date";
		var canonical = method & chr( 10 ) & path & chr( 10 ) & query & chr( 10 ) & canonicalHeaders & chr( 10 ) & signedHeaders & chr( 10 ) & payloadHash;
		var scope = day & "/" & region & "/s3/aws4_request";
		var toSign = "AWS4-HMAC-SHA256" & chr( 10 ) & amzDate & chr( 10 ) & scope & chr( 10 ) & lcase( hash( canonical, "SHA-256", "utf-8" ) );

		var kDate = hmacBinary( "AWS4" & cred.SECRET_KEY, day );
		var kRegion = hmacBinary( kDate, region );
		var kService = hmacBinary( kRegion, "s3" );
		var kSigning = hmacBinary( kService, "aws4_request" );
		var signature = lcase( binaryEncode( hmacBinary( kSigning, toSign ), "hex" ) );

		var auth = "AWS4-HMAC-SHA256 Credential=" & cred.ACCESS_KEY_ID & "/" & scope & ", SignedHeaders=" & signedHeaders & ", Signature=" & signature;
		var reqUrl = scheme & "://" & host & path & ( len( query ) ? "?" & query : "" );
		http method=method url=reqUrl result="local.rsp" timeout=30 {
			httpparam type="header" name="x-amz-date" value=amzDate;
			httpparam type="header" name="x-amz-content-sha256" value=payloadHash;
			httpparam type="header" name="Authorization" value=auth;
			if ( len( body ) ) httpparam type="body" value=body;
		}
		return rsp;
	}

	private binary function hmacBinary( required any key, required string message ) {
		return binaryDecode( hmac( message, key, "HmacSHA256", "utf-8" ), "hex" );
	}

}
