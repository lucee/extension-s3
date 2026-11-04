/**
 * LDEV-6862: credentials passed directly to an S3 function (accessKeyId, secretAccessKey, host) skipped the
 * ssl and pathStyleAccess settings of Application.cfc, so a call against an S3 compatible server (MinIO) behaved
 * differently from the same call with the credentials in this.s3.
 * https://luceeserver.atlassian.net/browse/LDEV-6862
 *
 * The calls run in the application defined by LDEV6862/Application.cfc: this.s3 only defines ssl=false and
 * pathStyleAccess (and wrong credentials), the endpoint is passed as host argument without a scheme. (The
 * settings of "application action=update" are no option, Lucee does not pass ssl and pathStyleAccess on.)
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI), only when it
 * is a local server speaking plain HTTP.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6862: ssl and pathStyleAccess of the application with credentials as arguments", skip=!isLocalServer(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6862" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="s3Write and s3Read", body=function() {
				var obj = CustomS3::key( ctx, "direct.txt" );
				call( "write", { bucket: ctx.bucket, object: obj, value: "direct credentials" } );
				expect( call( "read", { bucket: ctx.bucket, object: obj } ) ).toBe( "direct credentials" );
			});

			it( title="s3Exists", body=function() {
				// the object, not only the bucket: the list of buckets is cached by the extension and can be stale
				var obj = CustomS3::key( ctx, "exists.txt" );
				call( "write", { bucket: ctx.bucket, object: obj, value: "x" } );
				expect( call( "exists", { bucket: ctx.bucket, object: obj } ) ).toBeTrue();
				expect( call( "exists", { bucket: ctx.bucket, object: CustomS3::key( ctx, "missing.txt" ) } ) ).toBeFalse();
			});

			it( title="s3ListBucket", body=function() {
				call( "write", { bucket: ctx.bucket, object: CustomS3::key( ctx, "list.txt" ), value: "x" } );
				expect( call( "list", { bucket: ctx.bucket, prefix: ctx.prefix } ) ).toBe( 1 );
			});

		});
	}

	private any function call( required string op, struct args = {} ) {
		var u = duplicate( arguments.args );
		u.op = arguments.op;
		var res = internalRequest( template: baseURI() & "index.cfm", url: u );
		expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
		var data = deserializeJSON( res.filecontent );
		if ( structKeyExists( data, "error" ) ) throw( message="[#arguments.op#] failed: " & data.error );
		return data.value;
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6862/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}

	// the server only speaks plain http, the host of the tests has no scheme
	private boolean function isLocalServer() {
		if ( !CustomS3::isSupported() ) return false;
		return reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", CustomS3::getCredentials().HOST ) > 0;
	}
}
