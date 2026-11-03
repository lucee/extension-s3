/**
 * LDEV-6838: per-region bucket name suffix (this.s3.bucketSuffixes) for DR failover.
 * https://luceeserver.atlassian.net/browse/LDEV-6838
 *
 * The scenarios run in the application defined by LDEV6838/Application.cfc, the "cfg" url
 * variable selects the configuration. The bucket that really exists on the server is looked at
 * through a second application that has no suffix at all.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.RAW = {};
	variables.DR = { suffixes: { "us-east-1": "-dr" } };

	function run( testResults, testBox ) {

		describe( title="LDEV-6838: bucket suffix with a default region that has an entry", skip=isNotSupported(), body=function() {

			it( title="functions and s3:// paths read and write the suffixed bucket", body=function() {
				var bucket = bucketName( "dflt" );
				try {
					expect( call( DR, "create", { bucket: bucket } ) ).toBe( true );
					expect( call( DR, "write", { bucket: bucket, object: "a.txt", value: "hello dr" } ) ).toBe( true );

					// the bucket on the server has the suffix, the one without does not exist
					expect( call( RAW, "exists", { bucket: bucket & "-dr" } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( false );
					expect( call( RAW, "read", { bucket: bucket & "-dr", object: "a.txt" } ) ).toBe( "hello dr" );

					// the code keeps using the logical name
					expect( call( DR, "exists", { bucket: bucket } ) ).toBe( true );
					expect( call( DR, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "hello dr" );

					// virtual file system
					expect( call( DR, "vfsExists", { path: "s3:///#bucket#/a.txt" } ) ).toBe( true );
					expect( call( DR, "vfsRead", { path: "s3:///#bucket#/a.txt" } ) ).toBe( "hello dr" );
					expect( call( DR, "vfsWrite", { path: "s3:///#bucket#/b.txt", value: "from vfs" } ) ).toBe( true );
					expect( call( RAW, "read", { bucket: bucket & "-dr", object: "b.txt" } ) ).toBe( "from vfs" );
					expect( call( DR, "vfsDirExists", { path: "s3:///#bucket#/" } ) ).toBe( true );
				}
				finally {
					cleanup( bucket & "-dr" );
				}
			});

			it( title="the suffix is not added twice and bucket listings show the logical name", body=function() {
				var bucket = bucketName( "list" );
				try {
					call( DR, "create", { bucket: bucket } );

					var logical = call( DR, "buckets" );
					expect( logical ).toInclude( bucket );
					expect( logical ).notToInclude( bucket & "-dr" );

					var real = call( RAW, "buckets" );
					expect( real ).toInclude( bucket & "-dr" );
					expect( real ).notToInclude( bucket );

					var vfsLogical = call( DR, "vfsBuckets" );
					expect( vfsLogical ).toInclude( bucket );
					expect( vfsLogical ).notToInclude( bucket & "-dr" );
				}
				finally {
					cleanup( bucket & "-dr" );
				}
			});

			it( title="a bucket without the suffix is not visible", body=function() {
				var bucket = bucketName( "hidden" );
				try {
					call( RAW, "create", { bucket: bucket } );
					expect( call( DR, "buckets" ) ).notToInclude( bucket );
					expect( call( RAW, "buckets" ) ).toInclude( bucket );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="the presigned URL points to the suffixed bucket", body=function() {
				var bucket = bucketName( "presign" );
				try {
					call( DR, "create", { bucket: bucket } );
					call( DR, "write", { bucket: bucket, object: "a.txt", value: "x" } );
					var presigned = call( DR, "url", { bucket: bucket, object: "a.txt" } );
					expect( presigned ).toInclude( bucket & "-dr" );
				}
				finally {
					cleanup( bucket & "-dr" );
				}
			});
		});

		describe( title="LDEV-6838: regions and opt-out", skip=isNotSupported(), body=function() {

			it( title="a default region without an entry gets no suffix", body=function() {
				var bucket = bucketName( "noentry" );
				var cfg = { suffixes: { "eu-west-1": "-dr" } }; // default region is us-east-1
				try {
					call( cfg, "create", { bucket: bucket } );
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: bucket & "-dr" } ) ).toBe( false );
					expect( call( cfg, "vfsDirExists", { path: "s3:///#bucket#/" } ) ).toBe( true );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="an explicit region argument selects the suffix of that region", body=function() {
				var bucket = bucketName( "region" );
				var cfg = { suffixes: { "eu-west-1": "-dr" } }; // default region is us-east-1
				try {
					call( RAW, "create", { bucket: bucket & "-dr" } );
					call( RAW, "create", { bucket: bucket } );
					call( cfg, "write", { bucket: bucket, object: "a.txt", value: "eu", region: "eu-west-1" } );
					expect( call( RAW, "read", { bucket: bucket & "-dr", object: "a.txt" } ) ).toBe( "eu" );
					expect( call( RAW, "exists", { bucket: bucket, object: "a.txt" } ) ).toBe( false );

					// without the region argument the default region (no entry) is used
					call( cfg, "write", { bucket: bucket, object: "b.txt", value: "default" } );
					expect( call( RAW, "read", { bucket: bucket, object: "b.txt" } ) ).toBe( "default" );
					expect( call( RAW, "exists", { bucket: bucket & "-dr", object: "b.txt" } ) ).toBe( false );
				}
				finally {
					cleanup( bucket & "-dr" );
					cleanup( bucket );
				}
			});

			it( title="an explicit region without an entry gets no suffix", body=function() {
				var bucket = bucketName( "regionno" );
				try {
					call( RAW, "create", { bucket: bucket } );
					call( RAW, "create", { bucket: bucket & "-dr" } );
					// DR has an entry for us-east-1 only
					call( DR, "write", { bucket: bucket, object: "a.txt", value: "eu", region: "eu-west-1" } );
					expect( call( RAW, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "eu" );
					expect( call( RAW, "exists", { bucket: bucket & "-dr", object: "a.txt" } ) ).toBe( false );
				}
				finally {
					cleanup( bucket );
					cleanup( bucket & "-dr" );
				}
			});

			it( title="the region of a s3:// mapping selects the suffix", body=function() {
				var bucket = bucketName( "mapregion" );
				// default region is us-east-1, the mapping lives in eu-west-1 and inherits the suffixes
				var cfg = { suffixes: { "eu-west-1": "-eu" }, mapping: { key: "eumap", defaultLocation: "eu-west-1" } };
				try {
					call( RAW, "create", { bucket: bucket & "-eu" } );
					call( RAW, "write", { bucket: bucket & "-eu", object: "a.txt", value: "eu" } );

					// the default region has no entry, so the logical name is the real name and does not exist
					expect( call( cfg, "vfsExists", { path: "s3:///#bucket#/a.txt" } ) ).toBe( false );
					// the mapping uses the suffix of its region
					expect( call( cfg, "vfsExists", { path: "s3://eumap@/#bucket#/a.txt" } ) ).toBe( true );
					expect( call( cfg, "vfsRead", { path: "s3://eumap@/#bucket#/a.txt" } ) ).toBe( "eu" );
				}
				finally {
					cleanup( bucket & "-eu" );
				}
			});

			it( title="buckets in bucketSuffixExclude do not get the suffix", body=function() {
				var shared = bucketName( "shared" );
				var plain = bucketName( "plain" );
				var cfg = { suffixes: { "us-east-1": "-dr" }, exclude: [ shared, "ldev6838-prefix-*" ] };
				try {
					call( cfg, "create", { bucket: shared } );
					call( cfg, "create", { bucket: "ldev6838-prefix-" & plain } );
					call( cfg, "create", { bucket: plain } );

					expect( call( RAW, "exists", { bucket: shared } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "ldev6838-prefix-" & plain } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: plain } ) ).toBe( false );
					expect( call( RAW, "exists", { bucket: plain & "-dr" } ) ).toBe( true );

					expect( call( cfg, "exists", { bucket: shared } ) ).toBe( true );
					expect( call( cfg, "vfsDirExists", { path: "s3:///#shared#/" } ) ).toBe( true );
				}
				finally {
					cleanup( shared );
					cleanup( "ldev6838-prefix-" & plain );
					cleanup( plain & "-dr" );
				}
			});

			it( title="a mapping can override the suffixes", body=function() {
				var bucket = bucketName( "mapping" );
				var cfg = { suffixes: { "us-east-1": "-dr" }, mapping: { key: "mapsuffix", suffixes: { "us-east-1": "-map" } } };
				try {
					call( RAW, "create", { bucket: bucket & "-map" } );
					expect( call( cfg, "vfsWrite", { path: "s3://mapsuffix@/#bucket#/a.txt", value: "x" } ) ).toBe( true );
					expect( call( RAW, "read", { bucket: bucket & "-map", object: "a.txt" } ) ).toBe( "x" );
					expect( call( RAW, "exists", { bucket: bucket & "-dr" } ) ).toBe( false );
				}
				finally {
					cleanup( bucket & "-map" );
				}
			});

			it( title="a mapping with an empty map is the opt-out for that mapping", body=function() {
				var bucket = bucketName( "mapping" );
				var cfg = { suffixes: { "us-east-1": "-dr" }, mapping: { key: "plainmapping", suffixes: {} } };
				try {
					call( RAW, "create", { bucket: bucket } );
					expect( call( cfg, "vfsWrite", { path: "s3://plainmapping@/#bucket#/a.txt", value: "x" } ) ).toBe( true );
					expect( call( RAW, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "x" );
					expect( call( RAW, "exists", { bucket: bucket & "-dr" } ) ).toBe( false );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="without any configuration nothing changes", body=function() {
				var bucket = bucketName( "none" );
				try {
					call( RAW, "create", { bucket: bucket } );
					call( RAW, "write", { bucket: bucket, object: "a.txt", value: "plain" } );
					expect( call( RAW, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "plain" );
					expect( call( RAW, "vfsRead", { path: "s3:///#bucket#/a.txt" } ) ).toBe( "plain" );
					expect( call( RAW, "buckets" ) ).toInclude( bucket );
				}
				finally {
					cleanup( bucket );
				}
			});
		});

		describe( title="LDEV-6838: invalid configuration", skip=isNotSupported(), body=function() {

			it( title="an invalid suffix is rejected", body=function() {
				var result = call( { suffixes: { "us-east-1": "_DR/" } }, "exists", { bucket: bucketName( "bad" ) }, true );
				expect( result ).toHaveKey( "error" );
				expect( result.error ).toInclude( "suffix" );
			});

			it( title="an invalid suffix map is rejected", body=function() {
				var result = call( { suffixes: "{not json" }, "exists", { bucket: bucketName( "bad" ) }, true );
				expect( result ).toHaveKey( "error" );
			});
		});
	}

	// ---- helpers ----

	private string function bucketName( required string name ) {
		return "ldev6838-" & lcase( arguments.name ) & "-" & lcase( left( createUUID(), 8 ) );
	}

	/**
	 * runs an operation in the application of the given configuration, returns the value
	 * (or the whole result when "full" is true)
	 */
	private any function call( required struct cfg, required string op, struct args = {}, boolean full = false ) {
		var u = duplicate( arguments.args );
		u.op = arguments.op;
		u.cfg = serializeJSON( arguments.cfg );
		var res = internalRequest( template: baseURI() & "index.cfm", url: u );
		expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
		var data = deserializeJSON( res.filecontent );
		if ( arguments.full ) return data;
		if ( structKeyExists( data, "error" ) ) throw( message="[#arguments.op#] failed: " & data.error );
		return data.value;
	}

	private void function cleanup( required string bucket ) {
		try {
			if ( call( RAW, "exists", { bucket: arguments.bucket } ) ) call( RAW, "delete", { bucket: arguments.bucket } );
		}
		catch ( any e ) {
			systemOutput( "LDEV-6838 cleanup of [#arguments.bucket#] failed: " & e.message, true );
		}
	}

	private string function baseURI() {
		return "/test/#listLast( getDirectoryFromPath( getCurrentTemplatePath() ), "\/" )#/LDEV6838/";
	}

	private boolean function isNotSupported() {
		var env = server.system.environment;
		return !( len( env.S3_CUSTOM_HOST ?: "" ) && len( env.S3_CUSTOM_ACCESS_KEY_ID ?: "" ) && len( env.S3_CUSTOM_SECRET_KEY ?: "" ) );
	}
}
