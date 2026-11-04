/**
 * LDEV-6859: clearing / force deleting a bucket fails on S3 compatible servers that do not support listing the
 * object versions (answer 501 NotImplemented), for example the MinIO of the CI. The objects were removed,
 * but the exception stopped the bucket from being deleted.
 * https://luceeserver.atlassian.net/browse/LDEV-6859
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 * Uses a bucket of its own (not S3_CUSTOM_BUCKET), because the bucket is deleted.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6859: clear and delete a bucket with content", skip=( !CustomS3::isSupported() || len( CustomS3::getCredentials().BUCKET ) > 0 ), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6859" );
				s3Write( bucketName=ctx.bucket, objectName="a.txt", value="aaa" );
				s3Write( bucketName=ctx.bucket, objectName="sub/b.txt", value="bbb" );
				s3Write( bucketName=ctx.bucket, objectName="sub/deeper/c.txt", value="ccc" );
			});

			afterEach( function() {
				// in case a test failed half way, best effort
				try {
					if ( s3Exists( bucketName=ctx.bucket ) ) {
						s3ClearBucket( bucketName=ctx.bucket );
						s3DeleteBucket( bucketName=ctx.bucket );
					}
				}
				catch ( any e ) {
					systemOutput( "LDEV6859 cleanup failed: " & e.message, true );
				}
			});

			it( title="s3ClearBucket removes the content and keeps the bucket", body=function() {
				expect( s3Exists( bucketName=ctx.bucket, objectName="sub/b.txt" ) ).toBeTrue();
				s3ClearBucket( bucketName=ctx.bucket );
				expect( s3Exists( bucketName=ctx.bucket ) ).toBeTrue();
				expect( s3Exists( bucketName=ctx.bucket, objectName="a.txt" ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName="sub/deeper/c.txt" ) ).toBeFalse();
			});

			it( title="s3DeleteBucket with force deletes a bucket with content", body=function() {
				s3DeleteBucket( bucketName=ctx.bucket, force=true );
				expect( s3Exists( bucketName=ctx.bucket ) ).toBeFalse();
			});

			it( title="directoryDelete recursive deletes a bucket with content", body=function() {
				directoryDelete( "s3:///" & ctx.bucket, true );
				expect( s3Exists( bucketName=ctx.bucket ) ).toBeFalse();
			});
		});
	}
}
