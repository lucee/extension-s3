/**
 * S3GetApplicationSettings(): returns the S3 related settings of the current application (this.s3 and
 * the other custom settings of Application.cfc). No S3 server is needed, the Application.cfc in
 * S3GetApplicationSettings/ only holds fake values and nothing connects anywhere.
 * https://luceeserver.atlassian.net/browse/LDEV-6851
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="Test suite for S3GetApplicationSettings()", body=function() {

			it( title="returns a struct", body=function() {
				expect( S3GetApplicationSettings() ).toBeStruct();
			});

			it( title="returns the settings of this.s3", body=function() {
				var data = settings( "s3" );
				expect( data ).toHaveKey( "s3" );
				var s3 = data.s3;
				expect( s3.accessKeyId ).toBe( "fakeAccessKeyId" );
				expect( s3.awsSecretKey ).toBe( "fakeSecretKey" );
				expect( s3.host ).toBe( "s3.example.org" );
				expect( s3.ssl ).toBe( false );
				expect( s3.pathStyleAccess ).toBe( true );
				expect( s3.defaultLocation ).toBe( "eu-west-1" );
			});

			it( title="has no s3 key when the application defines no this.s3", body=function() {
				var data = settings( "none" );
				expect( data ).notToHaveKey( "s3" );
			});

			it( title="returns the other settings of the Application.cfc (this.vfs, custom keys), but no functions", body=function() {
				var data = settings( "custom" );
				expect( data ).toHaveKey( "s3" );
				expect( data.s3.accessKeyId ).toBe( "fakeAccessKeyId" );

				expect( data ).toHaveKey( "myCustomSetting" );
				expect( data.myCustomSetting ).toBe( "hello" );

				// the settings of a s3 mapping (this.vfs.s3.<name>)
				expect( data ).toHaveKey( "vfs" );
				expect( data.vfs.s3.mymapping.accessKeyId ).toBe( "mappingKeyId" );
				expect( data.vfs.s3.mymapping.host ).toBe( "mapping.example.org" );

				// the UDF of the Application.cfc must not show up
				expect( data ).notToHaveKey( "helperFunction" );
			});

			it( title="every application gets its own settings", body=function() {
				expect( settings( "s3" ).s3.host ).toBe( "s3.example.org" );
				expect( settings( "custom" ).s3 ).notToHaveKey( "host" );
				expect( settings( "s3" ).s3.host ).toBe( "s3.example.org" );
			});

		});
	}

	// calls the function in the application defined by S3GetApplicationSettings/Application.cfc
	private struct function settings( required string mode ) {
		var result = internalRequest( template=baseURI() & "S3GetApplicationSettings/index.cfm", urls={ mode: arguments.mode } );
		expect( result.status ).toBe( 200, result.filecontent );
		return deserializeJSON( result.filecontent );
	}

	private string function baseURI() {
		var uri = contractPath( getCurrentTemplatePath() );
		return left( uri, len( uri ) - len( "S3GetApplicationSettings.cfc" ) );
	}

}
