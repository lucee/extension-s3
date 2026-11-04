/**
 * LDEV-6865: opening a file that does not exist in append mode must create it (like a local file).
 * https://luceeserver.atlassian.net/browse/LDEV-6865
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6865: append to an S3 file", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6865" );
				base = "s3:///" & ctx.bucket & "/" & ctx.prefix;
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="append to a file that does not exist creates it", body=function() {
				var p = base & "new.txt";
				var f = fileOpen( p, "append" );
				fileWrite( f, "new" );
				fileClose( f );
				expect( fileExists( p ) ).toBeTrue();
				expect( fileRead( p ) ).toBe( "new" );
			});

			it( title="append to a file that does not exist, then append again", body=function() {
				var p = base & "twice.txt";
				var f = fileOpen( p, "append" );
				fileWrite( f, "one" );
				fileClose( f );
				f = fileOpen( p, "append" );
				fileWrite( f, "two" );
				fileClose( f );
				expect( fileRead( p ) ).toBe( "onetwo" );
			});

			it( title="append to an existing file keeps the content", body=function() {
				var p = base & "existing.txt";
				fileWrite( p, "start-" );
				var f = fileOpen( p, "append" );
				fileWrite( f, "end" );
				fileClose( f );
				expect( fileRead( p ) ).toBe( "start-end" );
			});

		});
	}
}
