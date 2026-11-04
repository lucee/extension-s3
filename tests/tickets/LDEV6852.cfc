/**
 * LDEV-6852: behaviour specs of the basic S3 functions and the virtual file system that run against the
 * S3 compatible server of the CI (MinIO), so they also run for pull requests from forks (no cloud credentials).
 * Covers read/write, binary, exists, copy, move, upload/download, delete, listing, metadata, ACL and the
 * file/directory functions on s3:/// paths.
 * https://luceeserver.atlassian.net/browse/LDEV-6852
 *
 * Needs the S3_CUSTOM_* environment variables (see CustomS3.cfc), otherwise skipped.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6852: basic functions on an S3 compatible server", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6852" );
				bucket = ctx.bucket;
				// a second bucket for the tests that copy or move between buckets, created when needed
				ctx2 = {};
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
				if ( structCount( ctx2 ) ) CustomS3::cleanup( ctx2 );
			});

			describe( "s3Write, s3Read, s3ReadBinary", function() {

				it( title="a string round trip, including a nested key and non ASCII characters", body=function() {
					var obj = CustomS3::key( ctx, "a/b/c/text.txt" );
					var text = "Susi Sorglos & Co, häuschen, é, ü, 日本語";
					s3Write( bucketName=bucket, objectName=obj, value=text );
					expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( text );
					expect( s3Read( bucketName=bucket, objectName=obj, charset="utf-8" ) ).toBe( text );
				});

				it( title="writing the same key again replaces the content", body=function() {
					var obj = CustomS3::key( ctx, "replace.txt" );
					s3Write( bucketName=bucket, objectName=obj, value="first version, longer" );
					s3Write( bucketName=bucket, objectName=obj, value="second" );
					expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( "second" );
				});

				it( title="binary data survives unchanged", body=function() {
					var obj = CustomS3::key( ctx, "all-bytes.bin" );
					var bytes = [];
					for ( var i = 0; i < 256; i++ ) arrayAppend( bytes, i > 127 ? i - 256 : i );
					var data = javaCast( "byte[]", bytes );
					s3Write( bucketName=bucket, objectName=obj, value=data );
					var back = s3ReadBinary( bucketName=bucket, objectName=obj );
					expect( isBinary( back ) ).toBeTrue();
					expect( arrayLen( back ) ).toBe( 256 );
					expect( toBase64( back ) ).toBe( toBase64( data ) );
				});

				it( title="reading a key that does not exist throws", body=function() {
					expect( function() {
						s3Read( bucketName=bucket, objectName=CustomS3::key( ctx, "does-not-exist.txt" ) );
					}).toThrow();
					expect( function() {
						s3ReadBinary( bucketName=bucket, objectName=CustomS3::key( ctx, "does-not-exist.txt" ) );
					}).toThrow();
				});

				it( title="reading from a bucket that does not exist throws", body=function() {
					expect( function() {
						s3Read( bucketName=bucket & "-nosuchbucket", objectName="x.txt" );
					}).toThrow();
				});

				it( title="the mime type is stored with the object", body=function() {
					var obj = CustomS3::key( ctx, "typed.json" );
					s3Write( bucketName=bucket, objectName=obj, value='{"a":1}', mimeType="application/json" );
					var meta = s3GetMetaData( bucketName=bucket, objectName=obj );
					expect( meta.contentType ).toInclude( "application/json" );
					expect( meta.size ).toBe( 7 );
				});
			});

			describe( "s3Exists and s3Delete", function() {

				it( title="s3Exists for a bucket, an object and things that do not exist", body=function() {
					var obj = CustomS3::key( ctx, "sub/exists.txt" );
					s3Write( bucketName=bucket, objectName=obj, value="x" );
					expect( s3Exists( bucketName=bucket ) ).toBeTrue();
					expect( s3Exists( bucketName=bucket, objectName=obj ) ).toBeTrue();
					expect( s3Exists( bucketName=bucket, objectName=CustomS3::key( ctx, "sub/missing.txt" ) ) ).toBeFalse();
					expect( s3Exists( bucketName=bucket & "-nosuchbucket" ) ).toBeFalse();
					expect( s3Exists( bucketName=bucket & "-nosuchbucket", objectName=obj ) ).toBeFalse();
				});

				it( title="s3Exists is false after the object is deleted and the other objects stay", body=function() {
					var keep = CustomS3::key( ctx, "keep.txt" );
					var gone = CustomS3::key( ctx, "gone.txt" );
					s3Write( bucketName=bucket, objectName=keep, value="keep" );
					s3Write( bucketName=bucket, objectName=gone, value="gone" );

					s3Delete( bucketName=bucket, objectName=gone );

					expect( s3Exists( bucketName=bucket, objectName=gone ) ).toBeFalse();
					expect( s3Exists( bucketName=bucket, objectName=keep ) ).toBeTrue();
					expect( s3Read( bucketName=bucket, objectName=keep ) ).toBe( "keep" );
				});
			});

			describe( "s3Copy and s3Move", function() {

				it( title="s3Copy inside a bucket keeps the source", body=function() {
					var src = CustomS3::key( ctx, "src/original.txt" );
					var trg = CustomS3::key( ctx, "trg/copy.txt" );
					s3Write( bucketName=bucket, objectName=src, value="the content" );

					s3Copy( srcBucketName=bucket, srcObjectName=src, trgBucketName=bucket, trgObjectName=trg );

					expect( s3Read( bucketName=bucket, objectName=src ) ).toBe( "the content" );
					expect( s3Read( bucketName=bucket, objectName=trg ) ).toBe( "the content" );
				});

				it( title="s3Copy replaces an existing target", body=function() {
					var src = CustomS3::key( ctx, "src.txt" );
					var trg = CustomS3::key( ctx, "trg.txt" );
					s3Write( bucketName=bucket, objectName=src, value="new" );
					s3Write( bucketName=bucket, objectName=trg, value="old and longer" );

					s3Copy( srcBucketName=bucket, srcObjectName=src, trgBucketName=bucket, trgObjectName=trg );

					expect( s3Read( bucketName=bucket, objectName=trg ) ).toBe( "new" );
				});

				it( title="s3Copy between two buckets", body=function() {
					ctx2 = CustomS3::prepare( "6852b" );
					var src = CustomS3::key( ctx, "from.txt" );
					var trg = CustomS3::key( ctx2, "to.txt" );
					s3Write( bucketName=bucket, objectName=src, value="between buckets" );

					s3Copy( srcBucketName=bucket, srcObjectName=src, trgBucketName=ctx2.bucket, trgObjectName=trg );

					expect( s3Read( bucketName=ctx2.bucket, objectName=trg ) ).toBe( "between buckets" );
					expect( s3Exists( bucketName=bucket, objectName=src ) ).toBeTrue();
				});

				it( title="s3Copy of an object that does not exist throws and creates nothing", body=function() {
					var trg = CustomS3::key( ctx, "never.txt" );
					expect( function() {
						s3Copy( srcBucketName=bucket, srcObjectName=CustomS3::key( ctx, "missing.txt" ), trgBucketName=bucket, trgObjectName=trg );
					}).toThrow();
					expect( s3Exists( bucketName=bucket, objectName=trg ) ).toBeFalse();
				});

				it( title="s3Move inside a bucket removes the source", body=function() {
					var src = CustomS3::key( ctx, "src/moved.txt" );
					var trg = CustomS3::key( ctx, "trg/moved.txt" );
					s3Write( bucketName=bucket, objectName=src, value="moving" );

					s3Move( srcBucketName=bucket, srcObjectName=src, trgBucketName=bucket, trgObjectName=trg );

					expect( s3Exists( bucketName=bucket, objectName=src ) ).toBeFalse();
					expect( s3Read( bucketName=bucket, objectName=trg ) ).toBe( "moving" );
				});

				it( title="s3Move between two buckets", body=function() {
					ctx2 = CustomS3::prepare( "6852b" );
					var src = CustomS3::key( ctx, "from.txt" );
					var trg = CustomS3::key( ctx2, "to.txt" );
					s3Write( bucketName=bucket, objectName=src, value="moved away" );

					s3Move( srcBucketName=bucket, srcObjectName=src, trgBucketName=ctx2.bucket, trgObjectName=trg );

					expect( s3Read( bucketName=ctx2.bucket, objectName=trg ) ).toBe( "moved away" );
					expect( s3Exists( bucketName=bucket, objectName=src ) ).toBeFalse();
				});

				it( title="s3Move of an object that does not exist throws", body=function() {
					expect( function() {
						s3Move( srcBucketName=bucket, srcObjectName=CustomS3::key( ctx, "missing.txt" ), trgBucketName=bucket, trgObjectName=CustomS3::key( ctx, "never.txt" ) );
					}).toThrow();
					expect( s3Exists( bucketName=bucket, objectName=CustomS3::key( ctx, "never.txt" ) ) ).toBeFalse();
				});
			});

			describe( "s3Upload and s3Download", function() {

				it( title="a file is uploaded and downloaded as string, binary and to a file", body=function() {
					var obj = CustomS3::key( ctx, "upload/file.txt" );
					var source = getTempDirectory() & "ldev6852-" & createUUID() & ".txt";
					var target = getTempDirectory() & "ldev6852-" & createUUID() & ".txt";
					fileWrite( source, "line 1" & chr( 10 ) & "line 2 äö" );
					try {
						s3Upload( source=source, bucketName=bucket, objectName=obj );
						expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( "line 1" & chr( 10 ) & "line 2 äö" );

						var asString = s3Download( bucket=bucket, object=obj, charset="UTF-8" );
						expect( isSimpleValue( asString ) ).toBeTrue();
						expect( asString ).toBe( fileRead( source ) );

						var asBinary = s3Download( bucket=bucket, object=obj );
						expect( isBinary( asBinary ) ).toBeTrue();
						expect( arrayLen( asBinary ) ).toBe( s3GetMetaData( bucketName=bucket, objectName=obj ).size );

						s3Download( bucket=bucket, object=obj, target=target );
						expect( fileRead( target ) ).toBe( fileRead( source ) );
					}
					finally {
						if ( fileExists( source ) ) fileDelete( source );
						if ( fileExists( target ) ) fileDelete( target );
					}
				});
			});

			describe( "s3ListBucket", function() {

				it( title="lists the objects of the bucket, the ones in a folder as well", body=function() {
					var keyOne = CustomS3::key( ctx, "list/one.txt" );
					var keyTwo = CustomS3::key( ctx, "list/two.txt" );
					var keyThree = CustomS3::key( ctx, "other/three.txt" );
					s3Write( bucketName=bucket, objectName=keyOne, value="1" );
					s3Write( bucketName=bucket, objectName=keyTwo, value="22" );
					s3Write( bucketName=bucket, objectName=keyThree, value="333" );

					// this version of the extension has no prefix argument, the whole bucket is listed
					var names = [];
					for ( var row in s3ListBucket( bucketName=bucket ) ) {
						arrayAppend( names, row.objectName );
					}
					expect( names ).toInclude( keyOne );
					expect( names ).toInclude( keyTwo );
					expect( names ).toInclude( keyThree );
				});
			});

			describe( "ACL", function() {

				it( title="an object written with a canned ACL has the owner with full control", body=function() {
					var obj = CustomS3::key( ctx, "acl/private.txt" );
					s3Write( bucketName=bucket, objectName=obj, value="private", acl="private" );
					var acl = s3GetACL( bucketName=bucket, objectName=obj );
					expect( isArray( acl ) ).toBeTrue();
					var permissions = [];
					for ( var grant in acl ) arrayAppend( permissions, grant.permission );
					expect( permissions ).toInclude( "FULL_CONTROL" );
				});

				it( title="an invalid canned ACL is rejected before anything is written", body=function() {
					var obj = CustomS3::key( ctx, "acl/invalid.txt" );
					expect( function() {
						s3Write( bucketName=bucket, objectName=obj, value="x", acl="no-such-acl" );
					}).toThrow();
					expect( s3Exists( bucketName=bucket, objectName=obj ) ).toBeFalse();
				});
			});

			describe( "file and directory functions on s3:/// paths", function() {

				it( title="fileWrite, fileRead, fileExists, fileInfo and fileDelete", body=function() {
					var p = s3path( "vfs/file.txt" );
					fileWrite( p, "via the file system" );
					expect( fileExists( p ) ).toBeTrue();
					expect( fileRead( p ) ).toBe( "via the file system" );
					expect( fileInfo( p ).size ).toBe( len( "via the file system" ) );
					// the same object is visible with the functions of the extension
					expect( s3Read( bucketName=bucket, objectName=CustomS3::key( ctx, "vfs/file.txt" ) ) ).toBe( "via the file system" );

					fileDelete( p );
					expect( fileExists( p ) ).toBeFalse();
					expect( s3Exists( bucketName=bucket, objectName=CustomS3::key( ctx, "vfs/file.txt" ) ) ).toBeFalse();
				});

				it( title="fileCopy and fileMove", body=function() {
					var src = s3path( "vfs/a.txt" );
					fileWrite( src, "copy me" );

					fileCopy( src, s3path( "vfs/b.txt" ) );
					expect( fileRead( s3path( "vfs/b.txt" ) ) ).toBe( "copy me" );
					expect( fileExists( src ) ).toBeTrue();

					fileMove( s3path( "vfs/b.txt" ), s3path( "vfs/c.txt" ) );
					expect( fileRead( s3path( "vfs/c.txt" ) ) ).toBe( "copy me" );
					expect( fileExists( s3path( "vfs/b.txt" ) ) ).toBeFalse();
				});

				it( title="directoryCreate, directoryExists, directoryList and non recursive directoryDelete of an empty directory", body=function() {
					var dir = s3path( "dirs/empty" );
					directoryCreate( dir );
					expect( directoryExists( dir ) ).toBeTrue();
					expect( fileExists( dir ) ).toBeFalse();
					expect( directoryList( dir ) ).toBeEmpty();

					directoryDelete( dir );
					expect( directoryExists( dir ) ).toBeFalse();
				});

				it( title="directoryList of files and sub directories, recursive and with a filter", body=function() {
					fileWrite( s3path( "tree/a.txt" ), "a" );
					fileWrite( s3path( "tree/b.log" ), "b" );
					fileWrite( s3path( "tree/sub/c.txt" ), "c" );

					var flat = directoryList( s3path( "tree" ), false, "name" );
					expect( flat ).toInclude( "a.txt" );
					expect( flat ).toInclude( "b.log" );
					expect( flat ).toInclude( "sub" );
					expect( flat ).notToInclude( "c.txt" );

					var deep = directoryList( s3path( "tree" ), true, "name" );
					expect( deep ).toInclude( "c.txt" );

					var txt = directoryList( path=s3path( "tree" ), recurse=true, listInfo="name", filter="*.txt" );
					expect( txt ).toInclude( "a.txt" );
					expect( txt ).toInclude( "c.txt" );
					expect( txt ).notToInclude( "b.log" );

					var q = directoryList( path=s3path( "tree" ), recurse=false, listInfo="query" );
					expect( q.recordCount ).toBe( 3 );
				});

				it( title="a recursive directoryDelete removes the content of a directory and leaves the rest", body=function() {
					fileWrite( s3path( "del/a.txt" ), "a" );
					fileWrite( s3path( "del/sub/b.txt" ), "b" );
					fileWrite( s3path( "stay/c.txt" ), "c" );

					directoryDelete( s3path( "del" ), true );

					expect( fileExists( s3path( "del/a.txt" ) ) ).toBeFalse();
					expect( fileExists( s3path( "del/sub/b.txt" ) ) ).toBeFalse();
					expect( directoryExists( s3path( "del" ) ) ).toBeFalse();
					expect( fileRead( s3path( "stay/c.txt" ) ) ).toBe( "c" );
				});

				it( title="a non recursive directoryDelete of a directory with content fails and keeps the content", body=function() {
					fileWrite( s3path( "full/a.txt" ), "a" );
					expect( function() {
						directoryDelete( s3path( "full" ), false );
					}).toThrow();
					expect( fileRead( s3path( "full/a.txt" ) ) ).toBe( "a" );
				});
			});
		});
	}

	private string function s3path( required string relative ) {
		// no credentials in the path, they come from the application settings set by CustomS3::prepare()
		return "s3:///" & bucket & "/" & ctx.prefix & relative;
	}
}
