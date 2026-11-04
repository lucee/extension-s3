/**
 * LDEV-6848: ssl and pathStyleAccess of this.s3 are dropped by the functions when the credentials
 * come from elsewhere, and the ssl / pathStyleAccess of a mapping (this.vfs.s3.<name>) are ignored.
 * https://luceeserver.atlassian.net/browse/LDEV-6848
 *
 * Needs a S3 compatible server on plain http (MinIO in the CI, S3_CUSTOM_* environment variables).
 * The scenarios run in the application defined by LDEV6848/Application.cfc, the "cfg" url variable
 * selects the configuration. The bucket and the object are always created and removed with the
 * "full" configuration, which has all settings in this.s3.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.FULL = { mode: "full" };
	variables.ENV = { mode: "env", ssl: false, pathStyleAccess: true };
	variables.MAPPING = { mode: "mapping", ssl: false, pathStyleAccess: true };

	function run( testResults, testBox ) {

		describe( title="LDEV-6848: this.s3 with ssl, the credentials come from the system properties", skip=isNotSupported(), body=function() {

			it( title="the functions use ssl=false from this.s3 (they reach the plain http server)", body=function() {
				var bucket = bucketName( "env" );
				try {
					call( FULL, "create", { bucket: bucket } );
					call( FULL, "write", { bucket: bucket, object: "a.txt", value: "hello env" } );

					expect( call( ENV, "exists", { bucket: bucket, object: "a.txt" } ) ).toBe( true );
					expect( call( ENV, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "hello env" );

					// and they write to the same place the reference setup reads from
					call( ENV, "write", { bucket: bucket, object: "b.txt", value: "written by env" } );
					expect( call( FULL, "read", { bucket: bucket, object: "b.txt" } ) ).toBe( "written by env" );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="the functions and the s3:// paths reach the same server with the same settings", body=function() {
				var bucket = bucketName( "same" );
				try {
					call( FULL, "create", { bucket: bucket } );
					call( ENV, "write", { bucket: bucket, object: "a.txt", value: "from the function" } );
					expect( call( ENV, "vfsRead", { path: "s3:///#bucket#/a.txt" } ) ).toBe( "from the function" );

					call( ENV, "vfsWrite", { path: "s3:///#bucket#/b.txt", value: "from the path" } );
					expect( call( ENV, "read", { bucket: bucket, object: "b.txt" } ) ).toBe( "from the path" );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="the presigned URL of a function uses the scheme of ssl=false from this.s3", body=function() {
				var bucket = bucketName( "url" );
				try {
					call( FULL, "create", { bucket: bucket } );
					call( FULL, "write", { bucket: bucket, object: "a.txt", value: "x" } );

					var u = call( ENV, "url", { bucket: bucket, object: "a.txt" } );
					expect( u ).toMatch( "^http://[^/]*" & reEscape( hostOf() ) & "/" );
				}
				finally {
					cleanup( bucket );
				}
			});

		});

		describe( title="LDEV-6848: ssl and pathStyleAccess of a mapping (this.vfs.s3.<name>)", skip=isNotSupported(), body=function() {

			it( title="the mapping uses its own ssl and pathStyleAccess, not the ones of this.s3", body=function() {
				var bucket = bucketName( "map" );
				try {
					call( FULL, "create", { bucket: bucket } );
					call( FULL, "write", { bucket: bucket, object: "a.txt", value: "hello mapping" } );

					// this.s3 of that application has ssl=true, the mapping has ssl=false
					expect( call( MAPPING, "vfsExists", { path: "s3://ldev6848@/#bucket#/a.txt" } ) ).toBe( true );
					expect( call( MAPPING, "vfsRead", { path: "s3://ldev6848@/#bucket#/a.txt" } ) ).toBe( "hello mapping" );

					call( MAPPING, "vfsWrite", { path: "s3://ldev6848@/#bucket#/b.txt", value: "written by mapping" } );
					expect( call( FULL, "read", { bucket: bucket, object: "b.txt" } ) ).toBe( "written by mapping" );

					var u = call( MAPPING, "urlOfPath", { path: "s3://ldev6848@/#bucket#/a.txt" } );
					expect( u ).toMatch( "^http://[^/]*" & reEscape( hostOf() ) & "/" );
				}
				finally {
					cleanup( bucket );
				}
			});

		});
	}

	private string function bucketName( required string name ) {
		return "ldev6848-" & lcase( arguments.name ) & "-" & lcase( left( createUUID(), 8 ) );
	}

	/**
	 * runs an operation in the application of the given configuration, returns the value
	 */
	private any function call( required struct cfg, required string op, struct args = {} ) {
		var u = duplicate( arguments.args );
		u.op = arguments.op;
		u.cfg = serializeJSON( arguments.cfg );
		var res = internalRequest( template: baseURI() & "index.cfm", url: u );
		expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
		var data = deserializeJSON( res.filecontent );
		if ( structKeyExists( data, "error" ) ) throw( message="[#arguments.op#] failed: " & data.error );
		return data.value;
	}

	private void function cleanup( required string bucket ) {
		try {
			call( FULL, "delete", { bucket: arguments.bucket } );
		}
		catch ( any e ) {
			systemOutput( "LDEV-6848 cleanup of [#arguments.bucket#] failed: " & e.message, true );
		}
	}

	private string function hostOf() {
		var host = server.system.environment.S3_CUSTOM_HOST;
		return reReplace( host, "^https?://", "" );
	}

	private string function reEscape( required string str ) {
		return reReplace( arguments.str, "([\.\?\+\*\[\]\(\)\{\}\^\$\|\\])", "\\\1", "all" );
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6848/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}

	private boolean function isNotSupported() {
		var env = server.system.environment;
		return !( len( env.S3_CUSTOM_HOST ?: "" ) && len( env.S3_CUSTOM_ACCESS_KEY_ID ?: "" ) && len( env.S3_CUSTOM_SECRET_KEY ?: "" ) );
	}
}
