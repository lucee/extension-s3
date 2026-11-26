/**
 * LDEV-6883: bucket prefix (this.s3.bucketPrefixes, the counterpart of the bucket suffix LDEV-6838) on Amazon with two
 * buckets in different regions, and directoryList / directoryCopy of the virtual file system across buckets with
 * different prefixes.
 * https://luceeserver.atlassian.net/browse/LDEV-6883
 *
 * The prefix is the region (ldev6883-use1- for us-east-1, ldev6883-euw1- for eu-west-1, the "ldev" start is what the
 * credentials of the CI are allowed to create), the code only uses the logical name.
 * Documented skip: a bucket in a second region is a cloud feature, the specs are skipped without the Amazon
 * credentials (the prefix itself is tested on MinIO by LDEV6883).
 * The scenarios run in the application defined by LDEV6883Amazon/Application.cfc, the "cfg" url variable selects the
 * configuration. What really exists on Amazon is looked at through applications without any prefix.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.US = "us-east-1";
	variables.EU = "eu-west-1";
	variables.PREFIXES = { "us-east-1": "ldev6883-use1-", "eu-west-1": "ldev6883-euw1-" };
	variables.SUFFIXES = { "us-east-1": "-use1", "eu-west-1": "-euw1" };
	variables.PUS = "ldev6883-use1-";
	variables.PEU = "ldev6883-euw1-";

	function run( testResults, testBox ) {

		describe( title="LDEV-6883: two buckets in different regions with a prefix each (Amazon)", skip=Util::isAWSNotSupported(), body=function() {

			beforeEach( function() {
				bucketUS = Util::createBucketName( "6883a" );
				bucketEU = Util::createBucketName( "6883b" );
				// the application of each region and the same without prefixes
				appUS = { prefixes: PREFIXES, defaultLocation: US };
				appEU = { prefixes: PREFIXES, defaultLocation: EU };
				rawUS = { defaultLocation: US };
				rawEU = { defaultLocation: EU };
				// the default region is us-east-1, the mapping "eumap" is eu-west-1
				appVFS = { prefixes: PREFIXES, defaultLocation: US, mapping: { key: "eumap", defaultLocation: EU } };
				call( appUS, "create", { bucket: bucketUS } );
				call( appEU, "create", { bucket: bucketEU } );
			});

			afterEach( function() {
				cleanup( rawUS, PUS & bucketUS );
				cleanup( rawEU, PEU & bucketEU );
			});

			it( title="each bucket gets the prefix of its region and the code uses the logical names", body=function() {
				expect( call( rawUS, "exists", { bucket: PUS & bucketUS } ) ).toBe( true );
				expect( call( rawUS, "exists", { bucket: bucketUS } ) ).toBe( false );
				expect( call( rawEU, "exists", { bucket: PEU & bucketEU } ) ).toBe( true );
				expect( call( rawEU, "exists", { bucket: bucketEU } ) ).toBe( false );

				call( appUS, "write", { bucket: bucketUS, object: "a.txt", value: "us" } );
				call( appEU, "write", { bucket: bucketEU, object: "a.txt", value: "eu" } );
				expect( call( rawUS, "read", { bucket: PUS & bucketUS, object: "a.txt" } ) ).toBe( "us" );
				expect( call( rawEU, "read", { bucket: PEU & bucketEU, object: "a.txt" } ) ).toBe( "eu" );
				expect( call( appUS, "read", { bucket: bucketUS, object: "a.txt" } ) ).toBe( "us" );
				expect( call( appEU, "read", { bucket: bucketEU, object: "a.txt" } ) ).toBe( "eu" );
			});

			it( title="the prefix is not added twice, the bucket list of a region shows the logical names of its buckets, the account has the prefixed ones", body=function() {
				expect( call( rawUS, "exists", { bucket: PUS & PUS & bucketUS } ) ).toBe( false );
				// every region sees the buckets of its own prefix only
				var logicalUS = call( appUS, "buckets" );
				expect( logicalUS ).toInclude( bucketUS );
				expect( logicalUS ).notToInclude( bucketEU );
				expect( logicalUS ).notToInclude( PUS & bucketUS );
				var logicalEU = call( appEU, "buckets" );
				expect( logicalEU ).toInclude( bucketEU );
				expect( logicalEU ).notToInclude( bucketUS );
				expect( logicalEU ).notToInclude( PEU & bucketEU );

				var real = call( rawUS, "buckets" );
				expect( real ).toInclude( PUS & bucketUS );
				expect( real ).toInclude( PEU & bucketEU );
			});

			it( title="the bucket of the other region is not found with the prefix of the own region", body=function() {
				// the logical name of the EU bucket with the US prefix does not exist
				expect( call( appUS, "exists", { bucket: bucketEU } ) ).toBe( false );
				expect( call( appEU, "exists", { bucket: bucketUS } ) ).toBe( false );
			});

			it( title="directoryList of buckets with different prefixes", body=function() {
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/dir/one.txt", value: "1" } );
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/dir/sub/two.txt", value: "2" } );
				call( appVFS, "vfsWrite", { path: "s3://eumap@/#bucketEU#/dir/three.txt", value: "3" } );

				var us = call( appVFS, "vfsList", { path: "s3:///#bucketUS#/dir" } );
				expect( us ).toInclude( "one.txt" );
				expect( us ).toInclude( "two.txt" );
				expect( us ).notToInclude( "three.txt" );

				var eu = call( appVFS, "vfsList", { path: "s3://eumap@/#bucketEU#/dir" } );
				expect( eu ).toInclude( "three.txt" );
				expect( eu ).notToInclude( "one.txt" );

				// the objects are in the buckets with the prefix
				expect( call( rawUS, "read", { bucket: PUS & bucketUS, object: "dir/one.txt" } ) ).toBe( "1" );
				expect( call( rawEU, "read", { bucket: PEU & bucketEU, object: "dir/three.txt" } ) ).toBe( "3" );
			});

			it( title="directoryCopy from a bucket to a bucket in another region", body=function() {
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/src/one.txt", value: "1" } );
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/src/sub/two.txt", value: "2" } );

				call( appVFS, "vfsCopy", { source: "s3:///#bucketUS#/src", destination: "s3://eumap@/#bucketEU#/copy" } );

				// the copy is in the EU bucket, the source is untouched
				expect( call( rawEU, "read", { bucket: PEU & bucketEU, object: "copy/one.txt" } ) ).toBe( "1" );
				expect( call( rawEU, "read", { bucket: PEU & bucketEU, object: "copy/sub/two.txt" } ) ).toBe( "2" );
				expect( call( rawUS, "read", { bucket: PUS & bucketUS, object: "src/one.txt" } ) ).toBe( "1" );
				expect( call( appVFS, "vfsRead", { path: "s3://eumap@/#bucketEU#/copy/sub/two.txt" } ) ).toBe( "2" );

				// and back
				call( appVFS, "vfsCopy", { source: "s3://eumap@/#bucketEU#/copy", destination: "s3:///#bucketUS#/back" } );
				expect( call( rawUS, "read", { bucket: PUS & bucketUS, object: "back/sub/two.txt" } ) ).toBe( "2" );
			});
		});

		describe( title="LDEV-6883: prefix and suffix together, two regions (Amazon)", skip=Util::isAWSNotSupported(), body=function() {

			beforeEach( function() {
				bucketUS = Util::createBucketName( "6883c" );
				bucketEU = Util::createBucketName( "6883d" );
				appUS = { prefixes: PREFIXES, suffixes: SUFFIXES, defaultLocation: US };
				appEU = { prefixes: PREFIXES, suffixes: SUFFIXES, defaultLocation: EU };
				rawUS = { defaultLocation: US };
				rawEU = { defaultLocation: EU };
				appVFS = { prefixes: PREFIXES, suffixes: SUFFIXES, defaultLocation: US, mapping: { key: "eumap", defaultLocation: EU } };
				call( appUS, "create", { bucket: bucketUS } );
				call( appEU, "create", { bucket: bucketEU } );
			});

			afterEach( function() {
				cleanup( rawUS, PUS & bucketUS & "-use1" );
				cleanup( rawEU, PEU & bucketEU & "-euw1" );
			});

			it( title="the bucket on Amazon is prefix + name + suffix of the region", body=function() {
				expect( call( rawUS, "exists", { bucket: PUS & bucketUS & "-use1" } ) ).toBe( true );
				expect( call( rawUS, "exists", { bucket: PUS & bucketUS } ) ).toBe( false );
				expect( call( rawUS, "exists", { bucket: bucketUS & "-use1" } ) ).toBe( false );
				expect( call( rawEU, "exists", { bucket: PEU & bucketEU & "-euw1" } ) ).toBe( true );
				expect( call( rawEU, "exists", { bucket: bucketEU } ) ).toBe( false );

				call( appUS, "write", { bucket: bucketUS, object: "a.txt", value: "us" } );
				call( appEU, "write", { bucket: bucketEU, object: "a.txt", value: "eu" } );
				expect( call( rawUS, "read", { bucket: PUS & bucketUS & "-use1", object: "a.txt" } ) ).toBe( "us" );
				expect( call( rawEU, "read", { bucket: PEU & bucketEU & "-euw1", object: "a.txt" } ) ).toBe( "eu" );
				expect( call( appUS, "read", { bucket: bucketUS, object: "a.txt" } ) ).toBe( "us" );
				expect( call( appEU, "read", { bucket: bucketEU, object: "a.txt" } ) ).toBe( "eu" );
			});

			it( title="the bucket list of a region shows the logical names, the prefix is not added twice", body=function() {
				expect( call( rawUS, "exists", { bucket: PUS & PUS & bucketUS & "-use1" } ) ).toBe( false );
				var logicalUS = call( appUS, "buckets" );
				expect( logicalUS ).toInclude( bucketUS );
				expect( logicalUS ).notToInclude( bucketEU );
				expect( logicalUS ).notToInclude( PUS & bucketUS & "-use1" );
				var logicalEU = call( appEU, "buckets" );
				expect( logicalEU ).toInclude( bucketEU );
				expect( logicalEU ).notToInclude( bucketUS );

				var real = call( rawUS, "buckets" );
				expect( real ).toInclude( PUS & bucketUS & "-use1" );
				expect( real ).toInclude( PEU & bucketEU & "-euw1" );
			});

			it( title="directoryList and directoryCopy across regions", body=function() {
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/src/one.txt", value: "1" } );
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/src/sub/two.txt", value: "2" } );
				call( appVFS, "vfsWrite", { path: "s3://eumap@/#bucketEU#/dir/three.txt", value: "3" } );

				var us = call( appVFS, "vfsList", { path: "s3:///#bucketUS#/src" } );
				expect( us ).toInclude( "one.txt" );
				expect( us ).toInclude( "two.txt" );
				expect( us ).notToInclude( "three.txt" );
				var eu = call( appVFS, "vfsList", { path: "s3://eumap@/#bucketEU#/dir" } );
				expect( eu ).toInclude( "three.txt" );

				call( appVFS, "vfsCopy", { source: "s3:///#bucketUS#/src", destination: "s3://eumap@/#bucketEU#/copy" } );
				expect( call( rawEU, "read", { bucket: PEU & bucketEU & "-euw1", object: "copy/sub/two.txt" } ) ).toBe( "2" );
				expect( call( rawUS, "read", { bucket: PUS & bucketUS & "-use1", object: "src/one.txt" } ) ).toBe( "1" );
			});
		});

		describe( title="LDEV-6883: opt-out (Amazon)", skip=Util::isAWSNotSupported(), body=function() {

			it( title="a region without an entry and a bucket in the opt-out list get no prefix", body=function() {
				var bucketUS = Util::createBucketName( "6883e" );
				var bucketEU = Util::createBucketName( "6883f" );
				var cfgUS = { prefixes: { "eu-west-1": PEU }, prefixExclude: [ bucketEU ], defaultLocation: US };
				var cfgEU = { prefixes: { "eu-west-1": PEU }, prefixExclude: [ bucketEU ], defaultLocation: EU };
				var rawUS = { defaultLocation: US };
				var rawEU = { defaultLocation: EU };
				try {
					// us-east-1 has no entry
					call( cfgUS, "create", { bucket: bucketUS } );
					expect( call( rawUS, "exists", { bucket: bucketUS } ) ).toBe( true );
					expect( call( rawUS, "exists", { bucket: PEU & bucketUS } ) ).toBe( false );
					// eu-west-1 has an entry, but the bucket is excluded
					call( cfgEU, "create", { bucket: bucketEU } );
					expect( call( rawEU, "exists", { bucket: bucketEU } ) ).toBe( true );
					expect( call( rawEU, "exists", { bucket: PEU & bucketEU } ) ).toBe( false );
					expect( call( cfgEU, "buckets" ) ).toInclude( bucketEU );
				}
				finally {
					cleanup( rawUS, bucketUS );
					cleanup( rawEU, bucketEU );
				}
			});
		});
	}

	// ---- helpers ----

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

	private void function cleanup( required struct cfg, required string bucket ) {
		try {
			if ( call( arguments.cfg, "exists", { bucket: arguments.bucket } ) ) call( arguments.cfg, "delete", { bucket: arguments.bucket } );
		}
		catch ( any e ) {
			systemOutput( "LDEV-6883 cleanup of [#arguments.bucket#] failed: " & e.message, true );
		}
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6883Amazon/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}
}
