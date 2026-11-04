/**
 * LDEV-6840: s3GetMetaData() returns the user metadata with the original keys in "metadata" and the
 * response headers in "headers", the flat keys of the struct stay as they are.
 * https://luceeserver.atlassian.net/browse/LDEV-6840
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6840: s3GetMetaData metadata and headers", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6840" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="metadata has the user metadata with the original keys", body=function() {
				var obj = CustomS3::key( ctx, "keys.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "42", "user_name": "alice", "plain": "p", "Mixed-Case": "m" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );

				expect( md ).toHaveKey( "metadata" );
				expect( md.metadata ).toBeStruct();
				// the struct is case insensitive, the servers do not agree on the case of the names
				expect( md.metadata[ "user-id" ] ).toBe( "42" );
				expect( md.metadata[ "user_name" ] ).toBe( "alice" );
				expect( md.metadata[ "plain" ] ).toBe( "p" );
				expect( md.metadata[ "mixed-case" ] ).toBe( "m" );
				expect( structCount( md.metadata ) ).toBe( 4, "only the user metadata belongs to metadata: " & structKeyList( md.metadata ) );
				// "user-id" and "userid" are different keys
				expect( md.metadata ).notToHaveKey( "userid" );
				expect( md.metadata ).notToHaveKey( "x-amz-meta-user-id" );
			});

			it( title="a key with the x-amz-meta- prefix of the header is not changed", body=function() {
				var obj = CustomS3::key( ctx, "prefix.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "other-key": "7" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				expect( md.metadata[ "other-key" ] ).toBe( "7" );
				expect( md.headers[ "x-amz-meta-other-key" ] ).toBe( "7" );
			});

			it( title="a user metadata key that is the name of a field of the struct does not replace the field", body=function() {
				var obj = CustomS3::key( ctx, "collide.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "size": "huge", "exists": "maybe", "bucket": "other", "objectName": "other.txt", "user-id": "42" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );

				expect( md.size ).toBe( 5 );
				expect( md.exists ).toBeTrue();
				expect( md.bucket ).toBeFalse();
				expect( md.objectName ).toBe( obj );
				expect( md.bucketName ).toBe( ctx.bucket );
				// they are all in metadata
				expect( md.metadata[ "size" ] ).toBe( "huge" );
				expect( md.metadata[ "exists" ] ).toBe( "maybe" );
				expect( md.metadata[ "bucket" ] ).toBe( "other" );
				expect( md.metadata[ "objectName" ] ).toBe( "other.txt" );
				expect( md.metadata[ "user-id" ] ).toBe( "42" );
			});

			it( title="headers has the headers of the response with the original names", body=function() {
				var obj = CustomS3::key( ctx, "headers.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "42" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );

				expect( md ).toHaveKey( "headers" );
				expect( md.headers ).toBeStruct();
				expect( md.headers[ "Content-Length" ] ).toBe( 5 );
				expect( md.headers ).toHaveKey( "ETag" );
				expect( md.headers ).toHaveKey( "Last-Modified" );
				expect( md.headers[ "x-amz-meta-user-id" ] ).toBe( "42" );
			});

			it( title="the existing keys are still there", body=function() {
				var obj = CustomS3::key( ctx, "legacy.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "42" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );

				expect( md.bucketName ).toBe( ctx.bucket );
				expect( md.objectName ).toBe( obj );
				expect( md.size ).toBe( 5 );
				expect( md.file ).toBeTrue();
				expect( md.directory ).toBeFalse();
				expect( md.exists ).toBeTrue();
				expect( md ).toHaveKey( "eTag" );
				expect( md ).toHaveKey( "contentType" );
				expect( md ).toHaveKey( "lastModified" );
				// deprecated, kept for backward compatibility
				expect( md.userid ).toBe( "42" );
			});

			it( title="the path form returns metadata and headers", body=function() {
				var obj = CustomS3::key( ctx, "path.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "42" } );

				var md = s3GetMetaData( bucketNameOrPath="s3:///" & ctx.bucket & "/" & obj );

				expect( md.metadata[ "user-id" ] ).toBe( "42" );
				expect( md.headers[ "x-amz-meta-user-id" ] ).toBe( "42" );
			});

			it( title="an object without user metadata has an empty metadata struct", body=function() {
				var obj = CustomS3::key( ctx, "none.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );

				expect( md.metadata ).toBeStruct();
				expect( structIsEmpty( md.metadata ) ).toBeTrue( structKeyList( md.metadata ) );
				expect( md.headers ).toBeStruct();
			});
		});
	}
}
