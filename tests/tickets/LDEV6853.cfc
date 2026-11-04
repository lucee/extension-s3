/**
 * LDEV-6853: S3GeneratePresignedURL() ignored the pathStyleAccess setting (it always generated virtual host style
 * URLs for a bucket without a dot in its name, even for servers like MinIO that only support path style), and had no
 * specs for most of its arguments.
 * https://luceeserver.atlassian.net/browse/LDEV-6853
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI), the settings
 * of the application (pathStyleAccess) come from the Application.cfc in the LDEV6853 folder.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		// the server only speaks plain http, and the URLs are fetched
		describe( title="LDEV-6853: S3GeneratePresignedURL", skip=!isLocalServer(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6853" );
				obj = CustomS3::key( ctx, "presign.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="presigned content" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			describe( "pathStyleAccess", function() {

				it( title="credentials as arguments: the URL is path style", body=function() {
					var cred = CustomS3::getCredentials();
					var presigned = S3GeneratePresignedURL(
						bucketNameOrPath=ctx.bucket, objectName=obj, expireDate=dateAdd( "n", 5, now() ),
						accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host="http://" & plainHost() );
					expectPathStyle( presigned, ctx.bucket, obj );
				});

				it( title="pathStyleAccess not set: the URL of a local server is path style and can be fetched", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj }, {}, "GET" );
					expectPathStyle( rsp.url, ctx.bucket, obj );
					expect( rsp.status ).toBe( 200 );
					expect( rsp.content ).toBe( "presigned content" );
				});

				it( title="pathStyleAccess=true: the URL is path style and can be fetched", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj }, { pathStyleAccess: true }, "GET" );
					expectPathStyle( rsp.url, ctx.bucket, obj );
					expect( rsp.status ).toBe( 200 );
					expect( rsp.content ).toBe( "presigned content" );
				});

				it( title="pathStyleAccess=false: the URL is virtual host style", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj }, { pathStyleAccess: false } );
					expect( rsp.url.startsWith( "http://" & ctx.bucket & "." & plainHost() & "/" ) ).toBeTrue( "not a virtual host style URL: " & rsp.url );
				});

				it( title="a dot in the bucket name always results in a path style URL", body=function() {
					// nothing is requested from the server, the bucket does not need to exist
					for ( var cfg in [ {}, { pathStyleAccess: true }, { pathStyleAccess: false } ] ) {
						var rsp = presign( { bucketNameOrPath: "ldev6853.with.dots", objectName: "a/b.txt" }, cfg );
						expect( rsp.url.startsWith( "http://" & plainHost() & "/ldev6853.with.dots/a/b.txt?" ) ).toBeTrue( "not a path style URL: " & rsp.url );
					}
				});

			});

			describe( "arguments", function() {

				it( title="expireDate is part of the URL", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, expireDate: 5 }, {} );
					expect( rsp.url ).toMatch( "[?&](X-Amz-Expires|Expires)=" );
				});

				it( title="expireDate in the past is an error", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, expireDate: -5 }, {} );
					expect( rsp ).toHaveKey( "error" );
					expect( rsp.error ).toInclude( "future" );
				});

				it( title="httpMethod PUT creates a URL to upload an object", body=function() {
					var target = CustomS3::key( ctx, "uploaded.txt" );
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: target, httpMethod: "put" }, {}, "PUT", "uploaded with a presigned URL" );
					expect( rsp.status ).toBe( 200 );
					expect( s3Read( bucketName=ctx.bucket, objectName=target ) ).toBe( "uploaded with a presigned URL" );
				});

				it( title="httpMethod HEAD", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, httpMethod: "HEAD" }, {}, "HEAD" );
					expect( rsp.status ).toBe( 200 );
				});

				it( title="httpMethod DELETE creates a URL to delete an object", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, httpMethod: "DELETE" }, {}, "DELETE" );
					expect( rsp.status ).toBe( 204 );
					expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeFalse();
				});

				it( title="an invalid httpMethod is an error", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, httpMethod: "FETCH" }, {} );
					expect( rsp ).toHaveKey( "error" );
					expect( rsp.error ).toInclude( "invalid http method" );
				});

				it( title="an invalid sseAlgorithm is an error", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, sseAlgorithm: "ROT13" }, {} );
					expect( rsp ).toHaveKey( "error" );
					expect( rsp.error ).toInclude( "SSE" );
				});

				it( title="version is part of the URL", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, version: "abc123" }, {} );
					expect( rsp.url ).toMatch( "[?&]versionId=abc123" );
				});

				it( title="disposition, type and encoding override the response headers", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, disposition: "attachment; filename=""x.txt""", type: "text/plain", encoding: "gzip" }, {} );
					expect( rsp.url ).toMatch( "[?&]response-content-disposition=" );
					expect( rsp.url ).toMatch( "[?&]response-content-type=text%2Fplain" );
					expect( rsp.url ).toMatch( "[?&]response-content-encoding=gzip" );
				});

				it( title="responseHeaders are added as x-amz-meta- parameters", body=function() {
					var rsp = presign( { bucketNameOrPath: ctx.bucket, objectName: obj, responseHeaders: { "foo": "bar", "x-amz-meta-baz": "qux" } }, {} );
					expect( rsp.url ).toMatch( "[?&]x-amz-meta-foo=bar" );
					expect( rsp.url ).toMatch( "[?&]x-amz-meta-baz=qux" );
				});

			});

		});
	}

	private void function expectPathStyle( required string presigned, required string bucket, required string object ) {
		var noScheme = reReplace( presigned, "^https?://", "" );
		expect( noScheme.startsWith( plainHost() & "/" & bucket & "/" & object & "?" ) ).toBeTrue( "not a path style URL: " & presigned );
	}

	/**
	 * calls S3GeneratePresignedURL() in the application of the Application.cfc of the LDEV6853 folder, optional the
	 * URL is requested with the given http method
	 * @return struct with url (or error) and, when requested, status and content
	 */
	private struct function presign( required struct args, required struct cfg, string fetch = "", string body = "" ) {
		var u = { cfg: serializeJSON( cfg ), args: serializeJSON( args ) };
		if ( len( fetch ) ) u.fetch = fetch;
		if ( len( body ) ) u.body = body;
		var res = internalRequest( template: baseURI() & "index.cfm", url: u );
		expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
		return deserializeJSON( res.filecontent );
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6853/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}

	private string function plainHost() {
		return reReplaceNoCase( CustomS3::getCredentials().HOST, "^https?://", "" );
	}

	// the server only speaks plain http, the host of the tests has no scheme
	private boolean function isLocalServer() {
		if ( !CustomS3::isSupported() ) return false;
		return reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", CustomS3::getCredentials().HOST ) > 0;
	}
}
