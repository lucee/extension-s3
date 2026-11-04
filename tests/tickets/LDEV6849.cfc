/**
 * LDEV-6849: the meta data of a bucket (S3InfoSupport.getMetaData()) returned the current time as
 * lastModified instead of the creation date of the bucket, the id of the owner was stored with the
 * misspelled key "ownerNd", and the region was read from a field instead of the region just loaded.
 * https://luceeserver.atlassian.net/browse/LDEV-6849
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6849: meta data of a bucket", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6849" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="lastModified of a bucket is its creation date, not the current time", body=function() {
				// the bucket must be older than a moment ago
				sleep( 2500 );
				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket );
				expect( md ).toHaveKey( "lastModified" );
				expect( md ).toHaveKey( "creationDate" );
				expect( md.lastModified.getTime() ).toBe( md.creationDate.getTime() );
				expect( dateDiff( "s", md.lastModified, now() ) ).toBeGTE( 2 );
			});

			it( title="the same for the s3:// path of the bucket", body=function() {
				sleep( 2500 );
				var md = storeGetMetaData( "s3:///" & ctx.bucket );
				expect( md.lastModified.getTime() ).toBe( md.creationDate.getTime() );
				expect( dateDiff( "s", md.lastModified, now() ) ).toBeGTE( 2 );
			});

			it( title="the id of the owner is available as ownerId", body=function() {
				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket );
				if ( structKeyExists( md, "owner" ) ) {
					expect( md ).toHaveKey( "ownerId" );
					expect( md.ownerId ).toBeString();
					expect( len( md.ownerId ) ).toBeGT( 0 );
					// the misspelled key of earlier versions is still there
					expect( md.ownerNd ).toBe( md.ownerId );
				}
			});

			it( title="the region is the region of the bucket", body=function() {
				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket );
				if ( structKeyExists( md, "region" ) ) {
					expect( md.region ).toBeString();
					expect( len( md.region ) ).toBeGT( 0 );
				}
			});

		});
	}
}
