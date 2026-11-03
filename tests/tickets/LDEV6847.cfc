/**
 * LDEV-6847: S3Function.isEmpty() did not return the result for a string, so an empty string was
 * never treated as "not set" for the optional arguments (timeout, zeroByte, responseHeaders,
 * location, range, merge, maxResults ...) of the functions: the empty string was handed to the cast to
 * number / boolean / struct and the function failed.
 * https://luceeserver.atlassian.net/browse/LDEV-6847
 *
 * Lucee casts arguments declared as number, boolean or struct in the function definition before
 * the function is called, so with named arguments an empty string never reaches isEmpty(). To test
 * the function itself, the functions (BIF) are called directly with the argument array, which is
 * what a dynamic call (for example from a tag, evaluate, or an extension) does.
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6847: an empty string for an optional argument means not set", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6847" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="S3CreateBucket with empty location and timeout", body=function() {
				var bucket = "ldev-6847-cb-" & lcase( left( createUUID(), 8 ) );
				try {
					callBif( "S3CreateBucket", [ bucket, "NIL", "", "NIL", "NIL", "NIL", "" ] );
					expect( s3Exists( bucketName=bucket ) ).toBeTrue();
				}
				finally {
					if ( s3Exists( bucketName=bucket ) ) s3Delete( bucketName=bucket, force=true );
				}
			});

			it( title="S3Download with empty timeout and range returns the whole object", body=function() {
				var obj = CustomS3::key( ctx, "download.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello download" );
				var data = callBif( "S3Download", [ ctx.bucket, obj, "NIL", "UTF-8", "NIL", "NIL", "NIL", "", "" ] );
				expect( data ).toBe( "hello download" );
			});

			it( title="S3ListBucket with empty timeout, delimiter, startAfter, maxResults and prefix lists everything", body=function() {
				s3Write( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "a.txt" ), value="a" );
				s3Write( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, "dir/b.txt" ), value="b" );

				var qry = callBif( "S3ListBucket", [ ctx.bucket, "NIL", "NIL", "NIL", "NIL", "NIL", "", "", "", "", ctx.prefix ] );
				// no delimiter, both objects, the one in the folder as well
				expect( qry.recordCount ).toBe( 2 );
			});

			it( title="S3GetMetaData with empty objectName and timeout", body=function() {
				var obj = CustomS3::key( ctx, "meta.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="12345" );
				var md = callBif( "S3GetMetaData", [ ctx.bucket, obj, "NIL", "NIL", "NIL", "" ] );
				expect( md ).toBeStruct();
				expect( md ).toHaveKey( "contentLength" );
				expect( md.contentLength ).toBe( 5 );

				var md = callBif( "S3GetMetaData", [ ctx.bucket, "", "NIL", "NIL", "NIL", "" ] );
				expect( md ).toBeStruct();
				expect( md ).notToHaveKey( "contentLength" );
			});

			it( title="S3GeneratePresignedURL with empty zeroByte, responseHeaders and timeout", body=function() {
				var obj = CustomS3::key( ctx, "presign.txt" );
				var presigned = callBif( "S3GeneratePresignedURL", [ ctx.bucket, obj, "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "", "", "NIL", "NIL", "NIL", "" ] );
				expect( presigned ).toBeString();
				expect( presigned ).toInclude( "X-Amz-Signature" );
				expect( presigned ).toInclude( ctx.bucket );
			});

			it( title="S3SetMetaData with empty timeout and merge", body=function() {
				var obj = CustomS3::key( ctx, "setmeta.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="x" );
				callBif( "S3SetMetaData", [ ctx.bucket, obj, { "x-ldev": "6847" }, "NIL", "NIL", "NIL", "", "" ] );
				var md = callBif( "S3GetMetaData", [ ctx.bucket, obj ] );
				expect( md.metadata ).toHaveKey( "x-ldev" );
			});

		});
	}

	private function callBif( required string name, required array args ) {
		// the classes of the extension are loaded by the class loader of its resource provider
		var cl = "";
		loop array=getPageContext().getConfig().getResourceProviders() item="local.provider" {
			if ( provider.getScheme() == "s3" ) {
				cl = provider.getClass().getClassLoader();
				break;
			}
		}
		expect( isSimpleValue( cl ) ).toBeFalse( "the resource provider for s3 is not available" );
		var bif = cl.loadClass( "org.lucee.extension.resource.s3.function.#arguments.name#" ).newInstance();
		// NIL stands for an argument that is not set (null)
		var list = createObject( "java", "java.util.ArrayList" );
		loop array=arguments.args item="local.arg" {
			if ( isSimpleValue( arg ) && arg == "NIL" ) list.add( javaCast( "null", "" ) );
			else list.add( arg );
		}
		return bif.invoke( getPageContext(), list.toArray() );
	}
}
