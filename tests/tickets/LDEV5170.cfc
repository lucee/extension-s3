/**
 * LDEV-5170: s3ListBucket() has the optional argument prefix, that is handed to S3 (the filtering is not done
 * by the extension), also together with delimiter, startAfter and maxResults.
 * https://luceeserver.atlassian.net/browse/LDEV-5170
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 * When S3_CUSTOM_BUCKET is used, the bucket holds more than the objects of the test, so every prefix used
 * here starts with the unique prefix of the test.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-5170: s3ListBucket() prefix", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "5170" );
				write( "images/originals/a.txt" );
				write( "images/originals/b.txt" );
				write( "images/thumbs/a.txt" );
				write( "other.txt" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="only the keys that start with the prefix are listed", body=function() {
				var qry = s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "images/originals/" );
				expect( names( qry ) ).toBe( [ ctx.prefix & "images/originals/a.txt", ctx.prefix & "images/originals/b.txt" ] );

				// the parent includes the sub folders
				expect( s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "images/" ).recordCount ).toBe( 3 );
			});

			it( title="a prefix is not limited to folders", body=function() {
				var qry = s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "oth" );
				expect( names( qry ) ).toBe( [ ctx.prefix & "other.txt" ] );
			});

			it( title="nothing matches", body=function() {
				expect( s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "does/not/exist/" ).recordCount ).toBe( 0 );
			});

			it( title="an empty prefix is the same as no prefix", body=function() {
				var withEmpty = s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix );
				// with a shared bucket the unique prefix is all there is, so compare with the result without the argument
				if ( !len( ctx.prefix ) ) {
					expect( withEmpty.recordCount ).toBe( s3ListBucket( bucketName=ctx.bucket ).recordCount );
					expect( s3ListBucket( bucketName=ctx.bucket, prefix="" ).recordCount ).toBe( withEmpty.recordCount );
				}
				expect( withEmpty.recordCount ).toBe( 4 );
			});

			it( title="prefix together with delimiter lists one level", body=function() {
				var qry = s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "images/", delimiter="/" );
				expect( names( qry ) ).toBe( [ ctx.prefix & "images/originals/", ctx.prefix & "images/thumbs/" ] );
				expect( qry.isPrefix[ 1 ] ).toBeTrue();
				expect( qry.isPrefix[ 2 ] ).toBeTrue();
			});

			it( title="prefix together with startAfter and maxResults", body=function() {
				var after = s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "images/", startAfter=ctx.prefix & "images/originals/a.txt" );
				expect( names( after ) ).toBe( [ ctx.prefix & "images/originals/b.txt", ctx.prefix & "images/thumbs/a.txt" ] );

				var limited = s3ListBucket( bucketName=ctx.bucket, prefix=ctx.prefix & "images/", maxResults=1 );
				expect( names( limited ) ).toBe( [ ctx.prefix & "images/originals/a.txt" ] );
			});

			it( title="prefix together with a listener and blockfactor", body=function() {
				var res = "";
				s3ListBucket(
					bucketName=ctx.bucket,
					prefix=ctx.prefix & "images/",
					listener=function( data ) {
						res &= data.recordCount & ";";
					},
					blockfactor=2
				);
				expect( res ).toBe( "2;1;" );
			});
		});
	}

	private function write( required string name ) {
		s3Write( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, name ), value="x" );
	}

	private array function names( required query qry ) {
		var res = [];
		for ( var r = 1; r <= qry.recordCount; r++ ) res.append( qry.objectName[ r ] );
		return res;
	}
}
