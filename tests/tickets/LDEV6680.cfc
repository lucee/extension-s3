/**
 * LDEV-6680: exists() must not report "does not exist" (and must not cache NotExisting)
 * when the request to S3 fails for another reason (pool timeout, no response, refused connection).
 * https://luceeserver.atlassian.net/browse/LDEV-6680
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.objectContent = "LDEV-6680 exists must not lie";

	function run( testResults , testBox ) {

		describe( title="LDEV-6680: client errors are not 'does not exist'", body=function() {

			it( title="exists() on an unreachable endpoint throws instead of returning false", body=function() {
				// nothing listens on port 1, so every request fails with a client error, never with a 404
				var path = "s3://ldev6680key:ldev6680secret@127.0.0.1:1/ldev6680-bucket/some/file.txt";
				for ( var i = 1; i <= 2; i++ ) {
					// the second round makes sure the failure was not cached as "not existing"
					var result = "";
					try {
						result = fileExists( path ) ? "true" : "false";
					}
					catch ( any e ) {
						result = "error";
					}
					expect( result ).toBe( "error", "round #i#: a connection error must not be reported as 'does not exist'" );
				}
			});
		});

		describe( title="LDEV-6680: saturated connection pool", skip=Util::isAWSNotSupported(), body=function() {

			it( title="parallel exists() with a tiny pool never answers false for an existing object", body=function() {
				var cred = Util::getAWSCredentials();
				var bucketName = Util::createBucketName( "6680" );
				var objectName = "ldev-6680.txt";
				try {
					S3CreateBucket( bucketName=bucketName, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY );
					S3Write( value=objectContent, bucketName=bucketName, objectName=objectName, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY );

					// one connection and a 1 ms timeout, so parallel requests run into pool timeouts
					application action="update" s3={
						accessKeyId: cred.ACCESS_KEY_ID,
						awsSecretKey: cred.SECRET_KEY,
						pool: { maxConnections: 1, connectionTimeout: 1 }
					};
					var results = [];
					arraySet( results, 1, 40, "" );
					results = results.map( function( p ) {
						try {
							return fileExists( "s3:///" & bucketName & "/" & objectName ) ? "true" : "false";
						}
						catch ( any e ) {
							if ( findNoCase( "does not exist", e.message ) ) return "missing: " & e.message;
							return "error";
						}
					}, true, 40 );
					for ( var r in results ) {
						expect( r ).notToBe( "false", "an existing object was reported as missing" );
						expect( left( r, 8 ) ).notToBe( "missing:", r );
					}

					// back to a normal pool: the object must be found, so no NotExisting was cached
					application action="update" s3={
						accessKeyId: cred.ACCESS_KEY_ID,
						awsSecretKey: cred.SECRET_KEY,
						pool: { maxConnections: 50, connectionTimeout: 10000 }
					};
					expect( fileExists( "s3:///" & bucketName & "/" & objectName ) ).toBeTrue();
				}
				finally {
					Util::deleteBucketEL( cred, bucketName );
				}
			});
		});
	}
}
