/**
 * LDEV-6855: specs for s3Delete (bucket, directory, force), the location argument of s3Upload and s3Copy,
 * the credential arguments of s3Delete, and the VFS operations that are not supported or not covered elsewhere
 * (fileSetLastModified, fileSetAccessMode, append, empty directories).
 * https://luceeserver.atlassian.net/browse/LDEV-6855
 *
 * (force=true on a bucket is covered by LDEV-6859, as it needs the version listing to be tolerated by the server.)
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6855: s3Delete", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6855d" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="a bucket with content is not deleted without force", body=function() {
				if ( !ctx.created ) return; // never delete a shared bucket (S3_CUSTOM_BUCKET)
				s3Write( bucketName=ctx.bucket, objectName="a.txt", value="a" );
				expect( function() {
					s3Delete( bucketName=ctx.bucket );
				}).toThrow( message="not empty" );
				expect( s3Exists( bucketName=ctx.bucket, objectName="a.txt" ) ).toBeTrue( "the content must still be there" );
			});

			it( title="an empty bucket is deleted without force", body=function() {
				if ( !ctx.created ) return; // never delete a shared bucket (S3_CUSTOM_BUCKET)
				s3Delete( bucketName=ctx.bucket );
				expect( s3Exists( bucketName=ctx.bucket, objectName="a.txt" ) ).toBeFalse();
				ctx.created = false;
			});

			it( title="a directory with content is only deleted with force", body=function() {
				var dir = CustomS3::key( ctx, "dir" );
				s3Write( bucketName=ctx.bucket, objectName=dir & "/a.txt", value="a" );
				s3Write( bucketName=ctx.bucket, objectName=dir & "/sub/b.txt", value="b" );
				s3Write( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "other.txt" ), value="o" );

				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=dir );
				}).toThrow( message="not empty" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=dir & "/a.txt" ) ).toBeTrue();

				s3Delete( bucketName=ctx.bucket, objectName=dir, force=true );
				expect( s3Exists( bucketName=ctx.bucket, objectName=dir & "/a.txt" ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName=dir & "/sub/b.txt" ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "other.txt" ) ) ).toBeTrue( "the neighbour must stay" );
			});

			it( title="credentials and host as arguments instead of the application settings", body=function() {
				var obj = CustomS3::key( ctx, "direct.txt" );
				var cred = ctx.cred;
				// a local server speaks plain http, the scheme is part of the host
				var host = reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", cred.HOST ) ? "http://" & cred.HOST : cred.HOST;
				s3Write( bucketName=ctx.bucket, objectName=obj, value="direct", accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host=host );
				expect( s3Exists( bucketName=ctx.bucket, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host=host ) ).toBeTrue();
				s3Delete( bucketName=ctx.bucket, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host=host );
				expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeFalse();
			});

		});

		describe( title="LDEV-6855: location argument", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6855l" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="s3Upload and s3Copy with a location", body=function() {
				var src = getTempFile( getTempDirectory(), "ldev6855" );
				fileWrite( src, "location content" );
				try {
					var up = CustomS3::key( ctx, "up.txt" );
					var cp = CustomS3::key( ctx, "cp.txt" );
					var location = len( ctx.cred.REGION ) ? ctx.cred.REGION : "us-east-1";
					s3Upload( bucketName=ctx.bucket, objectName=up, source=src, location=location );
					expect( s3Read( bucketName=ctx.bucket, objectName=up ) ).toBe( "location content" );
					s3Copy( srcBucketName=ctx.bucket, srcObjectName=up, trgBucketName=ctx.bucket, trgObjectName=cp, location=location );
					expect( s3Read( bucketName=ctx.bucket, objectName=cp ) ).toBe( "location content" );
					expect( s3Exists( bucketName=ctx.bucket, objectName=up ) ).toBeTrue( "copy keeps the source" );
				}
				finally {
					if ( fileExists( src ) ) fileDelete( src );
				}
			});

		});

		describe( title="LDEV-6855: VFS operations", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6855v" );
				base = "s3:///" & ctx.bucket & "/" & ctx.prefix;
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="fileSetLastModified and fileSetAccessMode are accepted and leave the object untouched (S3 supports neither)", body=function() {
				var p = base & "attr.txt";
				fileWrite( p, "hello" );
				var before = getFileInfo( p );
				fileSetLastModified( p, createDateTime( 2001, 2, 3, 4, 5, 6 ) );
				fileSetAccessMode( p, "644" );
				var after = getFileInfo( p );
				expect( fileRead( p ) ).toBe( "hello" );
				expect( after.size ).toBe( 5 );
				expect( after.lastmodified ).toBe( before.lastmodified, "S3 can't change the last modified date of an object" );
				expect( after.isModeSupported ).toBeFalse();
			});

			it( title="append adds to the existing content", body=function() {
				var p = base & "append.txt";
				fileWrite( p, "one" );
				var f = fileOpen( p, "append" );
				fileWrite( f, "two" );
				fileClose( f );
				expect( fileRead( p ) ).toBe( "onetwo" );
				// a second time
				f = fileOpen( p, "append" );
				fileWrite( f, "three" );
				fileClose( f );
				expect( fileRead( p ) ).toBe( "onetwothree" );
			});

			it( title="an empty directory exists, lists as empty, shows in its parent and is deleted without recursion", body=function() {
				var d = base & "empty";
				directoryCreate( d );
				expect( directoryExists( d ) ).toBeTrue();
				expect( directoryList( d ) ).toBeEmpty();
				expect( directoryList( base, false, "name" ) ).toInclude( "empty" );
				directoryDelete( d );
				expect( directoryExists( d ) ).toBeFalse();
				expect( directoryList( base, false, "name" ) ).notToInclude( "empty" );
			});

			it( title="a directory that gets content is not empty any more, and is empty again after the content is deleted", body=function() {
				var d = base & "grow";
				directoryCreate( d );
				fileWrite( d & "/a.txt", "a" );
				expect( arrayLen( directoryList( d ) ) ).toBe( 1 );
				fileDelete( d & "/a.txt" );
				expect( directoryList( d ) ).toBeEmpty();
			});

		});
	}
}
