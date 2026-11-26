/**
 * LDEV-6857: bucket suffix (this.s3.bucketSuffixes, LDEV-6838) on Amazon with two buckets in different regions,
 * and directoryList / directoryCopy of the virtual file system across buckets with different suffixes.
 * https://luceeserver.atlassian.net/browse/LDEV-6857
 *
 * The suffixes are the region (-use1 for us-east-1, -euw1 for eu-west-1), the code only uses the logical name.
 * Documented skip: a bucket in a second region is a cloud feature, the specs are skipped without the Amazon
 * credentials (the suffix itself is tested on MinIO by LDEV6838).
 * The scenarios run in the application defined by LDEV6857/Application.cfc, the "cfg" url variable selects the
 * configuration. What really exists on Amazon is looked at through applications without any suffix.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.US = "us-east-1";
	variables.EU = "eu-west-1";
	variables.SUFFIXES = { "us-east-1": "-use1", "eu-west-1": "-euw1" };

	function run( testResults, testBox ) {

		describe( title="LDEV-6857: two buckets in different regions with a suffix each (Amazon)", skip=Util::isAWSNotSupported(), body=function() {

			beforeEach( function() {
				bucketUS = Util::createBucketName( "6857a" );
				bucketEU = Util::createBucketName( "6857b" );
				// the application of each region and the same without suffixes
				appUS = { suffixes: SUFFIXES, defaultLocation: US };
				appEU = { suffixes: SUFFIXES, defaultLocation: EU };
				rawUS = { defaultLocation: US };
				rawEU = { defaultLocation: EU };
				// the default region is us-east-1, the mapping "eumap" is eu-west-1
				appVFS = { suffixes: SUFFIXES, defaultLocation: US, mapping: { key: "eumap", defaultLocation: EU } };
				call( appUS, "create", { bucket: bucketUS } );
				call( appEU, "create", { bucket: bucketEU } );
			});

			afterEach( function() {
				cleanup( rawUS, bucketUS & "-use1" );
				cleanup( rawEU, bucketEU & "-euw1" );
			});

			it( title="each bucket gets the suffix of its region and the code uses the logical names", body=function() {
				expect( call( rawUS, "exists", { bucket: bucketUS & "-use1" } ) ).toBe( true );
				expect( call( rawUS, "exists", { bucket: bucketUS } ) ).toBe( false );
				expect( call( rawEU, "exists", { bucket: bucketEU & "-euw1" } ) ).toBe( true );
				expect( call( rawEU, "exists", { bucket: bucketEU } ) ).toBe( false );

				call( appUS, "write", { bucket: bucketUS, object: "a.txt", value: "us" } );
				call( appEU, "write", { bucket: bucketEU, object: "a.txt", value: "eu" } );
				expect( call( rawUS, "read", { bucket: bucketUS & "-use1", object: "a.txt" } ) ).toBe( "us" );
				expect( call( rawEU, "read", { bucket: bucketEU & "-euw1", object: "a.txt" } ) ).toBe( "eu" );
				expect( call( appUS, "read", { bucket: bucketUS, object: "a.txt" } ) ).toBe( "us" );
				expect( call( appEU, "read", { bucket: bucketEU, object: "a.txt" } ) ).toBe( "eu" );
			});

			it( title="the bucket list of a region shows the logical names of its buckets, the account has the suffixed ones", body=function() {
				// every region sees the buckets of its own suffix only
				var logicalUS = call( appUS, "buckets" );
				expect( logicalUS ).toInclude( bucketUS );
				expect( logicalUS ).notToInclude( bucketEU );
				expect( logicalUS ).notToInclude( bucketUS & "-use1" );
				var logicalEU = call( appEU, "buckets" );
				expect( logicalEU ).toInclude( bucketEU );
				expect( logicalEU ).notToInclude( bucketUS );
				expect( logicalEU ).notToInclude( bucketEU & "-euw1" );

				var real = call( rawUS, "buckets" );
				expect( real ).toInclude( bucketUS & "-use1" );
				expect( real ).toInclude( bucketEU & "-euw1" );
			});

			it( title="the bucket of the other region is not found with the suffix of the own region", body=function() {
				// the logical name of the EU bucket with the US suffix does not exist
				expect( call( appUS, "exists", { bucket: bucketEU } ) ).toBe( false );
				expect( call( appEU, "exists", { bucket: bucketUS } ) ).toBe( false );
			});

			it( title="directoryList of buckets with different suffixes", body=function() {
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

				// the objects are in the buckets with the suffix
				expect( call( rawUS, "read", { bucket: bucketUS & "-use1", object: "dir/one.txt" } ) ).toBe( "1" );
				expect( call( rawEU, "read", { bucket: bucketEU & "-euw1", object: "dir/three.txt" } ) ).toBe( "3" );
			});

			it( title="directoryCopy from a bucket to a bucket in another region", body=function() {
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/src/one.txt", value: "1" } );
				call( appVFS, "vfsWrite", { path: "s3:///#bucketUS#/src/sub/two.txt", value: "2" } );

				call( appVFS, "vfsCopy", { source: "s3:///#bucketUS#/src", destination: "s3://eumap@/#bucketEU#/copy" } );

				// the copy is in the EU bucket, the source is untouched
				expect( call( rawEU, "read", { bucket: bucketEU & "-euw1", object: "copy/one.txt" } ) ).toBe( "1" );
				expect( call( rawEU, "read", { bucket: bucketEU & "-euw1", object: "copy/sub/two.txt" } ) ).toBe( "2" );
				expect( call( rawUS, "read", { bucket: bucketUS & "-use1", object: "src/one.txt" } ) ).toBe( "1" );
				expect( call( appVFS, "vfsRead", { path: "s3://eumap@/#bucketEU#/copy/sub/two.txt" } ) ).toBe( "2" );

				// and back
				call( appVFS, "vfsCopy", { source: "s3://eumap@/#bucketEU#/copy", destination: "s3:///#bucketUS#/back" } );
				expect( call( rawUS, "read", { bucket: bucketUS & "-use1", object: "back/sub/two.txt" } ) ).toBe( "2" );
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
			systemOutput( "LDEV-6857 cleanup of [#arguments.bucket#] failed: " & e.message, true );
		}
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6857/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}
}
