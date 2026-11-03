/**
 * LDEV-6844: s3Delete() has the argument ignoreMissing, to not fail for an object (or bucket) that does
 * not exist, like the DeleteObject operation of S3. The default is unchanged: it throws.
 * https://luceeserver.atlassian.net/browse/LDEV-6844
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6844: s3Delete() ignoreMissing", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6844" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="by default a missing object is an error", body=function() {
				var obj = CustomS3::key( ctx, "missing.txt" );
				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=obj );
				}).toThrow( message="does not exist" );
				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=obj, ignoreMissing=false );
				}).toThrow( message="does not exist" );
			});

			it( title="ignoreMissing=true: a missing object is not an error", body=function() {
				var obj = CustomS3::key( ctx, "missing.txt" );
				s3Delete( bucketName=ctx.bucket, objectName=obj, ignoreMissing=true );
				// twice, and with the positional form
				s3Delete( ctx.bucket, obj, true, nullValue(), nullValue(), nullValue(), 10000, true );
				// a missing object below a missing folder
				s3Delete( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "nofolder/missing.txt" ), ignoreMissing=true );
			});

			it( title="ignoreMissing=true still deletes an existing object", body=function() {
				var obj = CustomS3::key( ctx, "exists.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeTrue();

				s3Delete( bucketName=ctx.bucket, objectName=obj, ignoreMissing=true );

				expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeFalse();
				// and now it is missing
				s3Delete( bucketName=ctx.bucket, objectName=obj, ignoreMissing=true );
				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=obj );
				}).toThrow( message="does not exist" );
			});

			it( title="ignoreMissing=true does not hide other errors: a directory that is not empty", body=function() {
				var dir = CustomS3::key( ctx, "dir" );
				s3Write( bucketName=ctx.bucket, objectName=dir & "/a.txt", value="hello" );

				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=dir, force=false, ignoreMissing=true );
				}).toThrow( message="not empty" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=dir & "/a.txt" ) ).toBeTrue();

				s3Delete( bucketName=ctx.bucket, objectName=dir, force=true, ignoreMissing=true );
				expect( s3Exists( bucketName=ctx.bucket, objectName=dir & "/a.txt" ) ).toBeFalse();
			});

			it( title="a missing bucket", body=function() {
				var bucket = ctx.bucket & "-gone";
				// by default an error, as before
				expect( function() {
					s3Delete( bucketName=bucket );
				}).toThrow();
				// not an error with ignoreMissing
				s3Delete( bucketName=bucket, ignoreMissing=true );
				// deleting an object of a missing bucket is still an error: the bucket name is wrong
				expect( function() {
					s3Delete( bucketName=bucket, objectName="a.txt", ignoreMissing=true );
				}).toThrow();
			});

		});
	}
}
