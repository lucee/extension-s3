/**
 * LDEV-6839: s3SetMetaData() must only change the user metadata, the system metadata
 * (Content-Type, Cache-Control, ...) and the ACL of the object have to survive.
 * https://luceeserver.atlassian.net/browse/LDEV-6839
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6839: s3SetMetaData keeps the system metadata", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6839" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="Content-Type of the object is not reset", body=function() {
				var obj = CustomS3::key( ctx, "type.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj ).contentType ).toInclude( "text/plain" );

				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "42" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				expect( md.contentType ).toInclude( "text/plain" );
				expect( md.userid ?: "" ).toBe( "42" );
				expect( s3Read( bucketName=ctx.bucket, objectName=obj ) ).toBe( "hello" );
			});

			it( title="system metadata set by a previous s3SetMetaData call is kept by the next one", body=function() {
				var obj = CustomS3::key( ctx, "twice.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "content-type": "application/json", "cache-control": "max-age=60", "content-disposition": 'attachment; filename="x.json"', "content-language": "de", "a": "1" } );
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "b": "2" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				expect( md.contentType ).toBe( "application/json" );
				expect( md.cacheControl ).toBe( "max-age=60" );
				expect( md.contentDisposition ).toBe( 'attachment; filename="x.json"' );
				expect( md.contentLanguage ).toBe( "de" );
				expect( md.b ).toBe( "2" );
				// the user metadata is replaced, not merged
				expect( md ).notToHaveKey( "a" );
			});

			it( title="system header names in the struct set the real header", body=function() {
				var obj = CustomS3::key( ctx, "header.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain" );

				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ contentType: "text/csv" } );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj ).contentType ).toBe( "text/csv" );

				// it is the real header and not only a user metadata entry: a user metadata entry would be replaced by the next call
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "1" } );
				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				expect( md.contentType ).toBe( "text/csv" );
				expect( md.userid ).toBe( "1" );
			});

			it( title="the ACL of the object is kept", body=function() {
				var obj = CustomS3::key( ctx, "acl.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/plain", acl="public-read" );
				var before = hasAllUsersGrant( s3GetACL( bucketName=ctx.bucket, objectName=obj ) );
				// not every S3 compatible server stores object ACLs. If this one does not, there is nothing that could get lost.
				if ( !before ) {
					systemOutput( "LDEV-6839: the server does not keep object ACLs, ACL part of the test not applicable", true );
					return;
				}

				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "42" } );

				expect( hasAllUsersGrant( s3GetACL( bucketName=ctx.bucket, objectName=obj ) ) ).toBeTrue( "public-read ACL got lost" );
			});

			it( title="a folder placeholder object ('name/') can still get metadata", body=function() {
				var folder = CustomS3::key( ctx, "folder" );
				directoryCreate( "s3:///" & ctx.bucket & "/" & folder );
				var obj = folder & "/";
				// must not fail (MinIO can't copy a folder placeholder onto itself)
				s3SetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj, metadata={ "user-id": "7" } );
				expect( directoryExists( "s3:///" & ctx.bucket & "/" & folder ) ).toBeTrue( "the folder is gone" );
				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				// MinIO stores folder placeholders as directories and does not keep user metadata for them, so the
				// metadata is only checked where the server returns it
				if ( structKeyExists( md, "userid" ) ) expect( md.userid ).toBe( "7" );
			});
		});
	}

	private boolean function hasAllUsersGrant( required array acl ) {
		for ( var grant in acl ) {
			if ( ( grant.id ?: "" ) contains "AllUsers" ) return true;
		}
		return false;
	}
}
