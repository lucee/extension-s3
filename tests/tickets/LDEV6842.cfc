/**
 * LDEV-6842: s3Write(), s3Upload(), s3Copy() and s3Move() can store custom metadata, the standard HTTP
 * headers (Cache-Control, Content-Type, ...) and a storage class with the object.
 * https://luceeserver.atlassian.net/browse/LDEV-6842
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6842: metadata, headers and storage class when writing and copying", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6842" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="s3Write stores metadata and headers with a text value", body=function() {
				var obj = CustomS3::key( ctx, "text.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello",
					metadata={ "foo": "1", "bar-baz": "two" },
					headers={ "Cache-Control": "max-age=60", "Content-Disposition": "attachment", "Content-Language": "de", "Content-Encoding": "gzip",
						"Expires": createDateTime( 2030, 1, 1, 12, 0, 0, 0, "UTC" ) } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
				// s3GetMetaData() removes the first "-" of the names
				expect( md.foo ).toBe( "1" );
				expect( md.barbaz ).toBe( "two" );
				expect( md.cachecontrol ).toBe( "max-age=60" );
				expect( md.contentdisposition ).toBe( "attachment" );
				expect( md.contentlanguage ).toBe( "de" );
				expect( md.contentencoding ).toBe( "gzip" );
				expect( md.expires ).toInclude( "01 Jan 2030 12:00:00" );
			});

			it( title="the content type of a text value is the mime type or the Content-Type header, with the charset", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				var b = CustomS3::key( ctx, "b.txt" );
				var c = CustomS3::key( ctx, "c.txt" );
				var d = CustomS3::key( ctx, "d.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello", mimeType="text/html" );
				s3Write( bucketName=ctx.bucket, objectName=b, value="hello", headers={ "Content-Type": "text/css" } );
				s3Write( bucketName=ctx.bucket, objectName=c, value="hello", charset="iso-8859-1" );
				// a Content-Type that has a charset is stored as it is
				s3Write( bucketName=ctx.bucket, objectName=d, value="hello", headers={ "Content-Type": "text/csv; charset=utf-16" } );

				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=a ).contenttype ).toBe( "text/html; charset=UTF-8" );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=b ).contenttype ).toBe( "text/css; charset=UTF-8" );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=c ).contenttype ).toBe( "text/plain; charset=ISO-8859-1" );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=d ).contenttype ).toBe( "text/csv; charset=utf-16" );
			});

			it( title="s3Write stores metadata, headers and the mime type with binary data", body=function() {
				var a = CustomS3::key( ctx, "a.bin" );
				var b = CustomS3::key( ctx, "b.bin" );
				var bin = toBinary( toBase64( "hello" ) );
				s3Write( bucketName=ctx.bucket, objectName=a, value=bin, mimeType="application/x-ldev6842", metadata={ "foo": "1" }, headers={ "Cache-Control": "no-cache" } );
				s3Write( bucketName=ctx.bucket, objectName=b, value=bin, headers={ "Content-Type": "application/x-other" } );

				var mda = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=a );
				expect( mda.contenttype ).toBe( "application/x-ldev6842" );
				expect( mda.foo ).toBe( "1" );
				expect( mda.cachecontrol ).toBe( "no-cache" );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=b ).contenttype ).toBe( "application/x-other" );
				expect( s3ReadBinary( bucketName=ctx.bucket, objectName=a ) ).toBe( bin );
			});

			it( title="s3Write and s3Upload store metadata, headers and the mime type with a file", body=function() {
				var file = getTempDirectory() & "ldev6842-" & createUUID() & ".txt";
				fileWrite( file, "hello file" );
				try {
					var a = CustomS3::key( ctx, "file-write.txt" );
					var b = CustomS3::key( ctx, "file-upload.txt" );
					s3Write( bucketName=ctx.bucket, objectName=a, value=fileOpen( file ), mimeType="application/x-ldev6842", metadata={ "foo": "1" } );
					s3Upload( bucketName=ctx.bucket, objectName=b, source=file, mimeType="application/x-ldev6842", metadata={ "foo": "2" }, headers={ "Cache-Control": "max-age=5" } );

					var mda = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=a );
					expect( mda.contenttype ).toBe( "application/x-ldev6842" );
					expect( mda.foo ).toBe( "1" );
					var mdb = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=b );
					expect( mdb.contenttype ).toBe( "application/x-ldev6842" );
					expect( mdb.foo ).toBe( "2" );
					expect( mdb.cachecontrol ).toBe( "max-age=5" );
					expect( s3Read( bucketName=ctx.bucket, objectName=b ) ).toBe( "hello file" );
				}
				finally {
					if ( fileExists( file ) ) fileDelete( file );
				}
			});

			it( title="without the new arguments nothing changes: no metadata, the type is detected", body=function() {
				var obj = CustomS3::key( ctx, "plain.html" );
				var file = getTempDirectory() & "ldev6842-" & createUUID() & ".html";
				fileWrite( file, "<html></html>" );
				try {
					s3Upload( bucketName=ctx.bucket, objectName=obj, source=file );
					var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj );
					expect( md.contenttype ).toInclude( "text/html" );
					expect( md ).notToHaveKey( "cachecontrol" );
				}
				finally {
					if ( fileExists( file ) ) fileDelete( file );
				}
			});

			it( title="a copy without the new arguments keeps metadata and headers of the source", body=function() {
				var src = CustomS3::key( ctx, "src.txt" );
				var trg = CustomS3::key( ctx, "trg.txt" );
				s3Write( bucketName=ctx.bucket, objectName=src, value="hello", mimeType="text/html", metadata={ "foo": "1" }, headers={ "Cache-Control": "max-age=60" } );

				s3Copy( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=trg );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=trg );
				expect( md.foo ).toBe( "1" );
				expect( md.cachecontrol ).toBe( "max-age=60" );
				expect( md.contenttype ).toBe( "text/html; charset=UTF-8" );
			});

			it( title="a copy with metadata replaces the user metadata but keeps the headers of the source", body=function() {
				var src = CustomS3::key( ctx, "src.txt" );
				var trg = CustomS3::key( ctx, "trg.txt" );
				s3Write( bucketName=ctx.bucket, objectName=src, value="hello", mimeType="text/html", metadata={ "foo": "1", "old": "x" }, headers={ "Cache-Control": "max-age=60" } );

				s3Copy( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=trg, metadata={ "foo": "2", "added": "y" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=trg );
				expect( md.foo ).toBe( "2" );
				expect( md.added ).toBe( "y" );
				expect( md ).notToHaveKey( "old" );
				expect( md.cachecontrol ).toBe( "max-age=60" );
				expect( md.contenttype ).toBe( "text/html; charset=UTF-8" );
				// the source is not touched
				var mds = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=src );
				expect( mds.foo ).toBe( "1" );
				expect( mds.old ).toBe( "x" );
			});

			it( title="a copy with an empty metadata struct removes the user metadata", body=function() {
				var src = CustomS3::key( ctx, "src.txt" );
				var trg = CustomS3::key( ctx, "trg.txt" );
				s3Write( bucketName=ctx.bucket, objectName=src, value="hello", metadata={ "foo": "1" }, headers={ "Cache-Control": "max-age=60" } );

				s3Copy( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=trg, metadata={} );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=trg );
				expect( md ).notToHaveKey( "foo" );
				expect( md.cachecontrol ).toBe( "max-age=60" );
			});

			it( title="a copy with headers changes only these headers", body=function() {
				var src = CustomS3::key( ctx, "src.txt" );
				var trg = CustomS3::key( ctx, "trg.txt" );
				s3Write( bucketName=ctx.bucket, objectName=src, value="hello", mimeType="text/html", metadata={ "foo": "1" },
					headers={ "Cache-Control": "max-age=60", "Content-Language": "de" } );

				s3Copy( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=trg, headers={ "Cache-Control": "no-store" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=trg );
				expect( md.cachecontrol ).toBe( "no-store" );
				expect( md.contentlanguage ).toBe( "de" );
				expect( md.foo ).toBe( "1" );
				expect( md.contenttype ).toBe( "text/html; charset=UTF-8" );
			});

			it( title="s3Move can define metadata and headers of the target", body=function() {
				var src = CustomS3::key( ctx, "src.txt" );
				var trg = CustomS3::key( ctx, "trg.txt" );
				s3Write( bucketName=ctx.bucket, objectName=src, value="hello", metadata={ "foo": "1" } );

				s3Move( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=trg, metadata={ "foo": "moved" }, headers={ "Cache-Control": "max-age=1" } );

				var md = s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=trg );
				expect( md.foo ).toBe( "moved" );
				expect( md.cachecontrol ).toBe( "max-age=1" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=src ) ).toBeFalse();
			});

			it( title="the storage class is sent to the server", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				var b = CustomS3::key( ctx, "b.txt" );
				var c = CustomS3::key( ctx, "c.txt" );

				// STANDARD is known by every server
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello", storageClass="standard" );
				s3Copy( srcBucketName=ctx.bucket, srcObjectName=a, trgBucketName=ctx.bucket, trgObjectName=b, storageClass="STANDARD" );
				expect( s3Read( bucketName=ctx.bucket, objectName=b ) ).toBe( "hello" );

				// a class that does not exist is rejected by the server (MinIO, AWS), some servers (Wasabi) ignore the storage class
				var rejected = false;
				try {
					s3Write( bucketName=ctx.bucket, objectName=c, value="hello", storageClass="NOT_A_STORAGE_CLASS" );
				}
				catch ( any e ) {
					rejected = true;
					expect( e.message ).toMatch( "(?i)storage ?class" );
				}
				if ( rejected ) {
					expect( s3Exists( bucketName=ctx.bucket, objectName=c ) ).toBeFalse();
					expect( function() {
						s3Copy( srcBucketName=ctx.bucket, srcObjectName=a, trgBucketName=ctx.bucket, trgObjectName=c, storageClass="NOT_A_STORAGE_CLASS" );
					}).toThrow( message="could not copy" );
					expect( s3Exists( bucketName=ctx.bucket, objectName=c ) ).toBeFalse();
				}
			});

			it( title="invalid values are an error", body=function() {
				var obj = CustomS3::key( ctx, "invalid.txt" );

				expect( function() {
					s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", headers={ "X-Unknown": "1" } );
				}).toThrow( message="not supported" );
				expect( function() {
					s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", headers={ "Expires": "not a date" } );
				}).toThrow( message="not a valid date" );
				expect( function() {
					s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", metadata="foo=1" );
				}).toThrow( message="must be a struct" );
				expect( function() {
					s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/html", headers={ "Content-Type": "text/css" } );
				}).toThrow( message="different content types" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeFalse();

				// the same type in both places is fine
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello", mimeType="text/html", headers={ "Content-Type": "text/html" } );
				expect( s3GetMetaData( bucketNameOrPath=ctx.bucket, objectName=obj ).contenttype ).toBe( "text/html; charset=UTF-8" );
			});
		});
	}
}
