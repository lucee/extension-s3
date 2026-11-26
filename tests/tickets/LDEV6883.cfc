/**
 * LDEV-6883: per-region bucket name prefix (this.s3.bucketPrefixes), the counterpart of the bucket suffix (LDEV-6838).
 * https://luceeserver.atlassian.net/browse/LDEV-6883
 *
 * The scenarios run in the application defined by LDEV6883/Application.cfc, the "cfg" url
 * variable selects the configuration. The bucket that really exists on the server is looked at
 * through a second application that has no prefix at all.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.RAW = {};
	variables.PFX = { prefixes: { "us-east-1": "livestock-" } };

	function run( testResults, testBox ) {

		describe( title="LDEV-6883: bucket prefix with a default region that has an entry", skip=isNotSupported(), body=function() {

			it( title="functions and s3:// paths read and write the prefixed bucket", body=function() {
				var bucket = bucketName( "dflt" );
				try {
					expect( call( PFX, "create", { bucket: bucket } ) ).toBe( true );
					expect( call( PFX, "write", { bucket: bucket, object: "a.txt", value: "hello prefix" } ) ).toBe( true );

					// the bucket on the server has the prefix, the one without does not exist
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( false );
					expect( call( RAW, "read", { bucket: "livestock-" & bucket, object: "a.txt" } ) ).toBe( "hello prefix" );

					// the code keeps using the logical name
					expect( call( PFX, "exists", { bucket: bucket } ) ).toBe( true );
					expect( call( PFX, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "hello prefix" );

					// virtual file system
					expect( call( PFX, "vfsExists", { path: "s3:///#bucket#/a.txt" } ) ).toBe( true );
					expect( call( PFX, "vfsRead", { path: "s3:///#bucket#/a.txt" } ) ).toBe( "hello prefix" );
					expect( call( PFX, "vfsWrite", { path: "s3:///#bucket#/b.txt", value: "from vfs" } ) ).toBe( true );
					expect( call( RAW, "read", { bucket: "livestock-" & bucket, object: "b.txt" } ) ).toBe( "from vfs" );
					expect( call( PFX, "vfsDirExists", { path: "s3:///#bucket#/" } ) ).toBe( true );
				}
				finally {
					cleanup( "livestock-" & bucket );
				}
			});

			it( title="the prefix is not added twice and bucket listings show the logical name", body=function() {
				var bucket = bucketName( "list" );
				try {
					call( PFX, "create", { bucket: bucket } );
					// every further call still resolves to the same bucket
					call( PFX, "write", { bucket: bucket, object: "a.txt", value: "x" } );
					call( PFX, "vfsWrite", { path: "s3:///#bucket#/b.txt", value: "y" } );
					expect( call( RAW, "exists", { bucket: "livestock-livestock-" & bucket } ) ).toBe( false );

					var logical = call( PFX, "buckets" );
					expect( logical ).toInclude( bucket );
					expect( logical ).notToInclude( "livestock-" & bucket );

					var real = call( RAW, "buckets" );
					expect( real ).toInclude( "livestock-" & bucket );
					expect( real ).notToInclude( bucket );

					var vfsLogical = call( PFX, "vfsBuckets" );
					expect( vfsLogical ).toInclude( bucket );
					expect( vfsLogical ).notToInclude( "livestock-" & bucket );
				}
				finally {
					cleanup( "livestock-" & bucket );
				}
			});

			it( title="a bucket without the prefix is not visible", body=function() {
				var bucket = bucketName( "hidden" );
				try {
					call( RAW, "create", { bucket: bucket } );
					expect( call( PFX, "buckets" ) ).notToInclude( bucket );
					expect( call( RAW, "buckets" ) ).toInclude( bucket );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="the presigned URL points to the prefixed bucket", body=function() {
				var bucket = bucketName( "presign" );
				try {
					call( PFX, "create", { bucket: bucket } );
					call( PFX, "write", { bucket: bucket, object: "a.txt", value: "x" } );
					var presigned = call( PFX, "url", { bucket: bucket, object: "a.txt" } );
					expect( presigned ).toInclude( "livestock-" & bucket );
				}
				finally {
					cleanup( "livestock-" & bucket );
				}
			});
		});

		describe( title="LDEV-6883: prefix and suffix together", skip=isNotSupported(), body=function() {

			it( title="the bucket name is prefix + name + suffix", body=function() {
				var bucket = bucketName( "both" );
				var cfg = { prefixes: { "us-east-1": "livestock-" }, suffixes: { "us-east-1": "-dr" } };
				try {
					call( cfg, "create", { bucket: bucket } );
					call( cfg, "write", { bucket: bucket, object: "a.txt", value: "both" } );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket & "-dr" } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket } ) ).toBe( false );
					expect( call( RAW, "exists", { bucket: bucket & "-dr" } ) ).toBe( false );
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( false );
					expect( call( RAW, "read", { bucket: "livestock-" & bucket & "-dr", object: "a.txt" } ) ).toBe( "both" );
					expect( call( cfg, "vfsRead", { path: "s3:///#bucket#/a.txt" } ) ).toBe( "both" );

					// the listings show the logical name only for the buckets that have the prefix and the suffix
					var logical = call( cfg, "buckets" );
					expect( logical ).toInclude( bucket );
					expect( logical ).notToInclude( "livestock-" & bucket & "-dr" );
					expect( call( cfg, "vfsBuckets" ) ).toInclude( bucket );
				}
				finally {
					cleanup( "livestock-" & bucket & "-dr" );
				}
			});

			it( title="a bucket with only the prefix or only the suffix is not visible", body=function() {
				var onlyPrefix = bucketName( "onlyp" );
				var onlySuffix = bucketName( "onlys" );
				var cfg = { prefixes: { "us-east-1": "livestock-" }, suffixes: { "us-east-1": "-dr" } };
				try {
					call( RAW, "create", { bucket: "livestock-" & onlyPrefix } );
					call( RAW, "create", { bucket: onlySuffix & "-dr" } );
					var logical = call( cfg, "buckets" );
					expect( logical ).notToInclude( onlyPrefix );
					expect( logical ).notToInclude( onlySuffix );
					expect( logical ).notToInclude( "livestock-" & onlyPrefix );
					expect( logical ).notToInclude( onlySuffix & "-dr" );
				}
				finally {
					cleanup( "livestock-" & onlyPrefix );
					cleanup( onlySuffix & "-dr" );
				}
			});

			it( title="the prefix and the suffix have an own opt-out list", body=function() {
				var noPrefix = bucketName( "nopfx" );
				var noSuffix = bucketName( "nosfx" );
				var neither = bucketName( "neither" );
				var cfg = {
					prefixes: { "us-east-1": "livestock-" }, prefixExclude: [ noPrefix, neither ],
					suffixes: { "us-east-1": "-dr" }, exclude: [ noSuffix, neither ]
				};
				try {
					call( cfg, "create", { bucket: noPrefix } );
					call( cfg, "create", { bucket: noSuffix } );
					call( cfg, "create", { bucket: neither } );

					expect( call( RAW, "exists", { bucket: noPrefix & "-dr" } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "livestock-" & noSuffix } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: neither } ) ).toBe( true );

					// the listing shows all of them with the logical name
					var logical = call( cfg, "buckets" );
					expect( logical ).toInclude( noPrefix );
					expect( logical ).toInclude( noSuffix );
					expect( logical ).toInclude( neither );
					expect( logical ).notToInclude( noPrefix & "-dr" );
					expect( logical ).notToInclude( "livestock-" & noSuffix );

					expect( call( cfg, "vfsDirExists", { path: "s3:///#noPrefix#/" } ) ).toBe( true );
					expect( call( cfg, "vfsDirExists", { path: "s3:///#noSuffix#/" } ) ).toBe( true );
					expect( call( cfg, "vfsDirExists", { path: "s3:///#neither#/" } ) ).toBe( true );
				}
				finally {
					cleanup( noPrefix & "-dr" );
					cleanup( "livestock-" & noSuffix );
					cleanup( neither );
				}
			});
		});

		describe( title="LDEV-6883: regions and opt-out", skip=isNotSupported(), body=function() {

			it( title="a default region without an entry gets no prefix", body=function() {
				var bucket = bucketName( "noentry" );
				var cfg = { prefixes: { "eu-west-1": "livestock-" } }; // default region is us-east-1
				try {
					call( cfg, "create", { bucket: bucket } );
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket } ) ).toBe( false );
					expect( call( cfg, "vfsDirExists", { path: "s3:///#bucket#/" } ) ).toBe( true );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="an explicit region argument selects the prefix of that region", body=function() {
				var bucket = bucketName( "region" );
				var cfg = { prefixes: { "eu-west-1": "livestock-" } }; // default region is us-east-1
				try {
					call( RAW, "create", { bucket: "livestock-" & bucket } );
					call( RAW, "create", { bucket: bucket } );
					call( cfg, "write", { bucket: bucket, object: "a.txt", value: "eu", region: "eu-west-1" } );
					expect( call( RAW, "read", { bucket: "livestock-" & bucket, object: "a.txt" } ) ).toBe( "eu" );
					expect( call( RAW, "exists", { bucket: bucket, object: "a.txt" } ) ).toBe( false );

					// without the region argument the default region (no entry) is used
					call( cfg, "write", { bucket: bucket, object: "b.txt", value: "default" } );
					expect( call( RAW, "read", { bucket: bucket, object: "b.txt" } ) ).toBe( "default" );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket, object: "b.txt" } ) ).toBe( false );
				}
				finally {
					cleanup( "livestock-" & bucket );
					cleanup( bucket );
				}
			});

			it( title="an explicit region without an entry gets no prefix", body=function() {
				var bucket = bucketName( "regionno" );
				try {
					call( RAW, "create", { bucket: bucket } );
					call( RAW, "create", { bucket: "livestock-" & bucket } );
					// PFX has an entry for us-east-1 only
					call( PFX, "write", { bucket: bucket, object: "a.txt", value: "eu", region: "eu-west-1" } );
					expect( call( RAW, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "eu" );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket, object: "a.txt" } ) ).toBe( false );
				}
				finally {
					cleanup( bucket );
					cleanup( "livestock-" & bucket );
				}
			});

			it( title="the region of a s3:// mapping selects the prefix", body=function() {
				var bucket = bucketName( "mapregion" );
				// default region is us-east-1, the mapping lives in eu-west-1 and inherits the prefixes
				var cfg = { prefixes: { "eu-west-1": "eu-" }, mapping: { key: "eumap", defaultLocation: "eu-west-1" } };
				try {
					call( RAW, "create", { bucket: "eu-" & bucket } );
					call( RAW, "write", { bucket: "eu-" & bucket, object: "a.txt", value: "eu" } );

					// the default region has no entry, so the logical name is the real name and does not exist
					expect( call( cfg, "vfsExists", { path: "s3:///#bucket#/a.txt" } ) ).toBe( false );
					// the mapping uses the prefix of its region
					expect( call( cfg, "vfsExists", { path: "s3://eumap@/#bucket#/a.txt" } ) ).toBe( true );
					expect( call( cfg, "vfsRead", { path: "s3://eumap@/#bucket#/a.txt" } ) ).toBe( "eu" );
				}
				finally {
					cleanup( "eu-" & bucket );
				}
			});

			it( title="buckets in bucketPrefixExclude do not get the prefix", body=function() {
				var shared = bucketName( "shared" );
				var plain = bucketName( "plain" );
				var cfg = { prefixes: { "us-east-1": "livestock-" }, prefixExclude: [ shared, "ldev6883-wild-*" ] };
				try {
					call( cfg, "create", { bucket: shared } );
					call( cfg, "create", { bucket: "ldev6883-wild-" & plain } );
					call( cfg, "create", { bucket: plain } );

					expect( call( RAW, "exists", { bucket: shared } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "ldev6883-wild-" & plain } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: plain } ) ).toBe( false );
					expect( call( RAW, "exists", { bucket: "livestock-" & plain } ) ).toBe( true );

					expect( call( cfg, "exists", { bucket: shared } ) ).toBe( true );
					expect( call( cfg, "vfsDirExists", { path: "s3:///#shared#/" } ) ).toBe( true );

					// the excluded buckets are listed with their name
					var logical = call( cfg, "buckets" );
					expect( logical ).toInclude( shared );
					expect( logical ).toInclude( "ldev6883-wild-" & plain );
					expect( logical ).toInclude( plain );
				}
				finally {
					cleanup( shared );
					cleanup( "ldev6883-wild-" & plain );
					cleanup( "livestock-" & plain );
				}
			});

			it( title="a mapping can override the prefixes", body=function() {
				var bucket = bucketName( "mapping" );
				var cfg = { prefixes: { "us-east-1": "livestock-" }, mapping: { key: "mapprefix", prefixes: { "us-east-1": "map-" } } };
				try {
					call( RAW, "create", { bucket: "map-" & bucket } );
					expect( call( cfg, "vfsWrite", { path: "s3://mapprefix@/#bucket#/a.txt", value: "x" } ) ).toBe( true );
					expect( call( RAW, "read", { bucket: "map-" & bucket, object: "a.txt" } ) ).toBe( "x" );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket } ) ).toBe( false );
				}
				finally {
					cleanup( "map-" & bucket );
				}
			});

			it( title="a mapping with an empty map is the opt-out for that mapping", body=function() {
				var bucket = bucketName( "mapping" );
				var cfg = { prefixes: { "us-east-1": "livestock-" }, mapping: { key: "plainmapping", prefixes: {} } };
				try {
					call( RAW, "create", { bucket: bucket } );
					expect( call( cfg, "vfsWrite", { path: "s3://plainmapping@/#bucket#/a.txt", value: "x" } ) ).toBe( true );
					expect( call( RAW, "read", { bucket: bucket, object: "a.txt" } ) ).toBe( "x" );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket } ) ).toBe( false );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="an empty prefix for a region means no prefix", body=function() {
				var bucket = bucketName( "emptyentry" );
				var cfg = { prefixes: { "us-east-1": "" } };
				try {
					call( cfg, "create", { bucket: bucket } );
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( true );
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

		describe( title="LDEV-6883: directoryList and directoryCopy between buckets with different prefixes", skip=isNotSupported(), body=function() {

			it( title="each region has its own prefix", body=function() {
				var bucketA = bucketName( "copya" );
				var bucketB = bucketName( "copyb" );
				// default region us-east-1 has the prefix "us-", the mapping "eumap" is eu-west-1 with the prefix "eu-"
				var cfg = { prefixes: { "us-east-1": "us-", "eu-west-1": "eu-" }, mapping: { key: "eumap", defaultLocation: "eu-west-1" } };
				try {
					call( RAW, "create", { bucket: "us-" & bucketA } );
					call( RAW, "create", { bucket: "eu-" & bucketB } );

					call( cfg, "vfsWrite", { path: "s3:///#bucketA#/dir/one.txt", value: "1" } );
					call( cfg, "vfsWrite", { path: "s3:///#bucketA#/dir/sub/two.txt", value: "2" } );
					call( cfg, "vfsWrite", { path: "s3://eumap@/#bucketB#/dir/three.txt", value: "3" } );

					var a = call( cfg, "vfsList", { path: "s3:///#bucketA#/dir" } );
					expect( a ).toInclude( "one.txt" );
					expect( a ).toInclude( "two.txt" );
					expect( a ).notToInclude( "three.txt" );
					var b = call( cfg, "vfsList", { path: "s3://eumap@/#bucketB#/dir" } );
					expect( b ).toInclude( "three.txt" );
					expect( b ).notToInclude( "one.txt" );

					expect( call( RAW, "read", { bucket: "us-" & bucketA, object: "dir/one.txt" } ) ).toBe( "1" );
					expect( call( RAW, "read", { bucket: "eu-" & bucketB, object: "dir/three.txt" } ) ).toBe( "3" );

					// copy there and back
					call( cfg, "vfsCopy", { source: "s3:///#bucketA#/dir", destination: "s3://eumap@/#bucketB#/copy" } );
					expect( call( RAW, "read", { bucket: "eu-" & bucketB, object: "copy/one.txt" } ) ).toBe( "1" );
					expect( call( RAW, "read", { bucket: "eu-" & bucketB, object: "copy/sub/two.txt" } ) ).toBe( "2" );
					expect( call( RAW, "read", { bucket: "us-" & bucketA, object: "dir/one.txt" } ) ).toBe( "1" );

					call( cfg, "vfsCopy", { source: "s3://eumap@/#bucketB#/copy", destination: "s3:///#bucketA#/back" } );
					expect( call( RAW, "read", { bucket: "us-" & bucketA, object: "back/sub/two.txt" } ) ).toBe( "2" );
				}
				finally {
					cleanup( "us-" & bucketA );
					cleanup( "eu-" & bucketB );
				}
			});
		});

		describe( title="LDEV-6883: environment variable / system property", skip=isNotSupported(), body=function() {

			it( title="lucee.s3.bucketprefixes is used, the application replaces it, the opt-out wins over it", body=function() {
				var bucketEnv = bucketName( "env" );
				var bucketApp = bucketName( "envapp" );
				var bucketOut = bucketName( "envout" );
				var props = createObject( "java", "java.lang.System" );
				try {
					props.setProperty( "lucee.s3.bucketprefixes", '{"us-east-1":"livestock-"}' );

					// no definition in the application, the property is used (RAW has no definition of its own)
					call( RAW, "create", { bucket: bucketEnv } );
					expect( call( RAW, "exists", { bucket: bucketEnv } ) ).toBe( true );
					var logical = call( RAW, "buckets" );
					expect( logical ).toInclude( bucketEnv );
					expect( logical ).notToInclude( "livestock-" & bucketEnv );

					// the application replaces the property
					call( { prefixes: { "us-east-1": "app-" } }, "create", { bucket: bucketApp } );
					// an empty definition in the application is the opt-out
					call( { prefixes: {} }, "create", { bucket: bucketOut } );
				}
				finally {
					props.clearProperty( "lucee.s3.bucketprefixes" );
				}
				// the property is gone, now RAW shows what really exists
				try {
					expect( call( RAW, "exists", { bucket: "livestock-" & bucketEnv } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: bucketEnv } ) ).toBe( false );
					expect( call( RAW, "exists", { bucket: "app-" & bucketApp } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucketApp } ) ).toBe( false );
					expect( call( RAW, "exists", { bucket: bucketOut } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucketOut } ) ).toBe( false );
				}
				finally {
					cleanup( "livestock-" & bucketEnv );
					cleanup( "app-" & bucketApp );
					cleanup( bucketOut );
				}
			});

			it( title="lucee.s3.bucketprefixexclude is used", body=function() {
				var bucket = bucketName( "envex" );
				var props = createObject( "java", "java.lang.System" );
				try {
					props.setProperty( "lucee.s3.bucketprefixes", '{"us-east-1":"livestock-"}' );
					props.setProperty( "lucee.s3.bucketprefixexclude", bucket );
					call( RAW, "create", { bucket: bucket } );
				}
				finally {
					props.clearProperty( "lucee.s3.bucketprefixes" );
					props.clearProperty( "lucee.s3.bucketprefixexclude" );
				}
				try {
					expect( call( RAW, "exists", { bucket: bucket } ) ).toBe( true );
					expect( call( RAW, "exists", { bucket: "livestock-" & bucket } ) ).toBe( false );
				}
				finally {
					cleanup( bucket );
				}
			});

			it( title="an invalid lucee.s3.bucketprefixes is rejected", body=function() {
				var props = createObject( "java", "java.lang.System" );
				var result = "";
				try {
					props.setProperty( "lucee.s3.bucketprefixes", "{not json" );
					result = call( RAW, "exists", { bucket: bucketName( "badenv" ) }, true );
				}
				finally {
					props.clearProperty( "lucee.s3.bucketprefixes" );
				}
				expect( result ).toHaveKey( "error" );
				expect( result.error ).toInclude( "LUCEE_S3_BUCKETPREFIXES" );
			});
		});

		describe( title="LDEV-6883: invalid configuration", skip=isNotSupported(), body=function() {

			it( title="an invalid prefix is rejected", body=function() {
				var result = call( { prefixes: { "us-east-1": "Live_/" } }, "exists", { bucket: bucketName( "bad" ) }, true );
				expect( result ).toHaveKey( "error" );
				expect( result.error ).toInclude( "prefix" );
			});

			it( title="a prefix that does not start with a letter or a digit is rejected", body=function() {
				var result = call( { prefixes: { "us-east-1": "-livestock" } }, "exists", { bucket: bucketName( "bad" ) }, true );
				expect( result ).toHaveKey( "error" );
				expect( result.error ).toInclude( "prefix" );
			});

			it( title="an invalid prefix map is rejected", body=function() {
				var result = call( { prefixes: "{not json" }, "exists", { bucket: bucketName( "bad" ) }, true );
				expect( result ).toHaveKey( "error" );
			});
		});
	}

	// ---- helpers ----

	private string function bucketName( required string name ) {
		return "ldev6883-" & lcase( arguments.name ) & "-" & lcase( left( createUUID(), 8 ) );
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
			systemOutput( "LDEV-6883 cleanup of [#arguments.bucket#] failed: " & e.message, true );
		}
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6883/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}

	private boolean function isNotSupported() {
		var env = server.system.environment;
		return !( len( env.S3_CUSTOM_HOST ?: "" ) && len( env.S3_CUSTOM_ACCESS_KEY_ID ?: "" ) && len( env.S3_CUSTOM_SECRET_KEY ?: "" ) );
	}
}
