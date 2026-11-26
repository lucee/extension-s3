/**
 * LDEV-6841: s3SetMetaData() must not add the prefix x-amz-meta- a second time when the name of a
 * metadata entry already has it.
 * https://luceeserver.atlassian.net/browse/LDEV-6841
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6841: s3SetMetaData and the x-amz-meta- prefix", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6841" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="a name with and without the prefix give the same header", body=function() {
				var withPrefix = CustomS3::key( ctx, "with.txt" );
				var without = CustomS3::key( ctx, "without.txt" );
				s3Write( bucketName=ctx.bucket, objectName=withPrefix, value="hello" );
				s3Write( bucketName=ctx.bucket, objectName=without, value="hello" );

				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=withPrefix, metadata={ "x-amz-meta-foo": "1" } );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=without, metadata={ "foo": "1" } );

				var a = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=withPrefix );
				var b = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=without );
				expect( b.foo ).toBe( "1" );
				// s3GetMetaData() removes the first "-" of the names, this is the key if the prefix would be there twice
				expect( a ).notToHaveKey( "xamz-meta-foo" );
				expect( a.foo ).toBe( "1" );
				// the original names of the user metadata (LDEV-6840)
				expect( a.metadata ).toHaveKey( "foo" );
				expect( a.metadata ).notToHaveKey( "x-amz-meta-foo" );
				expect( a.headers ).notToHaveKey( "x-amz-meta-x-amz-meta-foo" );
			});

			it( title="the prefix is removed without looking at the case", body=function() {
				var obj = CustomS3::key( ctx, "case.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );

				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "X-Amz-Meta-Foo": "1", "X-AMZ-META-bar": "2" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				expect( md.foo ).toBe( "1" );
				expect( md.bar ).toBe( "2" );
				expect( md ).notToHaveKey( "xamz-meta-foo" );
				expect( md ).notToHaveKey( "xamz-meta-bar" );
			});

			it( title="a name that only contains the prefix later on is not changed", body=function() {
				var obj = CustomS3::key( ctx, "inside.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );

				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "my-x-amz-meta-foo": "3", "x-amz-metadata": "4" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				// s3GetMetaData() removes the first "-" of the name
				expect( md[ "myx-amz-meta-foo" ] ).toBe( "3" );
				// "x-amz-metadata" does not start with the prefix "x-amz-meta-"
				expect( md[ "xamz-metadata" ] ).toBe( "4" );
			});

			it( title="a name that is only the prefix is an error", body=function() {
				var obj = CustomS3::key( ctx, "empty.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );

				expect( function() {
					s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "x-amz-meta-": "1" } );
				}).toThrow( message="cannot be empty" );
			});
		});
	}
}
