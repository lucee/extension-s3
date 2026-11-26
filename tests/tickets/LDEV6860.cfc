/**
 * LDEV-6860: s3Write treated a string value as a path when it resolved to an existing resource. An empty string
 * resolved to the current directory (FileNotFoundException), a string naming a directory failed the same way.
 * A string value is text, only a string naming an existing file is read as a file (the documented behaviour).
 * https://luceeserver.atlassian.net/browse/LDEV-6860
 *
 * Needs the S3_CUSTOM_* environment variables (see CustomS3.cfc), otherwise skipped.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6860: s3Write with a string that looks like a path", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6860" );
				bucket = ctx.bucket;
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="an empty string creates an empty object", body=function() {
				var obj = CustomS3::key( ctx, "empty.txt" );
				s3Write( bucketName=bucket, objectName=obj, value="" );
				expect( s3Exists( bucketName=bucket, objectName=obj ) ).toBeTrue();
				expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( "" );
				expect( s3GetMetaData( bucketName=bucket, objectName=obj ).size ).toBe( 0 );
			});

			it( title="a string naming an existing directory is written as text", body=function() {
				var obj = CustomS3::key( ctx, "dir.txt" );
				var dir = getTempDirectory();
				expect( directoryExists( dir ) ).toBeTrue();
				s3Write( bucketName=bucket, objectName=obj, value=dir );
				expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( dir );
			});

			it( title="a string naming an existing file still uploads the file", body=function() {
				var obj = CustomS3::key( ctx, "file.txt" );
				var file = getTempFile( getTempDirectory(), "ldev6860", "txt" );
				try {
					fileWrite( file, "content of the file" );
					s3Write( bucketName=bucket, objectName=obj, value=file );
					expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( "content of the file" );
				}
				finally {
					if ( fileExists( file ) ) fileDelete( file );
				}
			});

			it( title="a string that is not a path is written as text", body=function() {
				var obj = CustomS3::key( ctx, "text.txt" );
				s3Write( bucketName=bucket, objectName=obj, value="just some text" );
				expect( s3Read( bucketName=bucket, objectName=obj ) ).toBe( "just some text" );
			});

		});
	}
}
