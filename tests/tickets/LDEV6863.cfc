/**
 * LDEV-6863: clearing a bucket (s3ClearBucket, and s3Delete with force) deleted the entries of a versioned bucket
 * with deleteObject(bucket, key), without the version id. That only adds delete markers, the old versions stay and
 * the bucket can not be deleted afterwards.
 * https://luceeserver.atlassian.net/browse/LDEV-6863
 *
 * Needs a server that supports versioning and listing the versions (MinIO only does in some setups, the MinIO of
 * the CI does not list versions), so the tests are skipped when the server does not.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		// the versioning of a bucket is changed, so the tests need a bucket of their own (not S3_CUSTOM_BUCKET)
		variables.versioningSupported = false;
		if ( CustomS3::isSupported() && !len( CustomS3::getCredentials().BUCKET ) ) {
			var probe = CustomS3::prepare( "6863p" );
			try {
				var status = CustomS3::enableVersioning( probe );
				if ( status == 200 ) {
					var rsp = CustomS3::signedRequest( probe, "GET", "/" & probe.bucket, "versions=" );
					if ( rsp.status_code == 200 ) variables.versioningSupported = true;
					else if ( rsp.status_code != 501 ) throw( message="could not check if the server lists versions: " & rsp.status_code & " " & rsp.filecontent );
				}
				else if ( status != 501 ) throw( message="could not check if the server supports versioning: " & status );
			}
			finally {
				CustomS3::cleanup( probe );
			}
		}

		describe( title="LDEV-6863: clearing a versioned bucket", skip=!variables.versioningSupported, body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6863" );
				expect( CustomS3::enableVersioning( ctx ) ).toBe( 200 );
				obj = CustomS3::key( ctx, "versioned.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="version 1" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="version 2" );
				s3Write( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "other.txt" ), value="other" );
				// the preconditions: the object has two versions
				expect( s3GetVersionInfo( bucketName=ctx.bucket, objectName=obj ).recordCount ).toBe( 2 );
			});

			afterEach( function() {
				// a failing test must not leave a bucket with versions behind
				try {
					if ( s3Exists( bucketName=ctx.bucket ) ) s3Delete( bucketName=ctx.bucket, force=true );
				}
				catch ( any e ) {
					systemOutput( "LDEV6863 cleanup failed: " & e.message, true );
				}
			});

			it( title="s3ClearBucket removes all versions and delete markers", body=function() {
				s3ClearBucket( bucketName=ctx.bucket );
				expect( countVersions() ).toBe( 0, "versions or delete markers are left in the bucket" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeFalse();
			});

			it( title="the cleared bucket can be deleted", body=function() {
				s3ClearBucket( bucketName=ctx.bucket );
				var rsp = CustomS3::signedRequest( ctx, "DELETE", "/" & ctx.bucket );
				expect( rsp.status_code ).toBe( 204, "the bucket could not be deleted: " & rsp.status_code & " " & rsp.filecontent );
			});

			it( title="s3Delete with force removes a versioned bucket", body=function() {
				s3Delete( bucketName=ctx.bucket, force=true );
				expect( s3Exists( bucketName=ctx.bucket ) ).toBeFalse();
			});

		});
	}

	/** number of versions and delete markers in the bucket (as the server reports them) */
	private numeric function countVersions() {
		var rsp = CustomS3::signedRequest( ctx, "GET", "/" & ctx.bucket, "versions=" );
		expect( rsp.status_code ).toBe( 200 );
		return arrayLen( reMatchNoCase( "<(Version|DeleteMarker)>", rsp.filecontent ) );
	}
}
