/**
 * LDEV-6866: s3GeneratePresignedURL sseCustomerKey (SSE-C) is ignored without sseAlgorithm and fails with it.
 * https://luceeserver.atlassian.net/browse/LDEV-6866
 * Only the URL is generated, so this runs against any S3 compatible server (no request is sent).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6866: sseCustomerKey of s3GeneratePresignedURL", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6866" );
				obj = CustomS3::key( ctx, "ssec.txt" );
				// a 256 bit key, base64 encoded
				sseKey = toBase64( binaryDecode( hash( "ldev-6866", "sha-256" ), "hex" ) );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="sseCustomerKey signs the SSE-C headers", body=function() {
				for ( var method in [ "PUT", "GET" ] ) {
					var u = s3GeneratePresignedURL( bucketNameOrPath=ctx.bucket, objectName=obj, httpMethod=method, sseCustomerKey=sseKey );
					expect( signedHeaders( u ) ).toBe(
						"host,x-amz-server-side-encryption-customer-algorithm,x-amz-server-side-encryption-customer-key,x-amz-server-side-encryption-customer-key-md5", method );
				}
			});

			it( title="without sseCustomerKey no SSE-C header is signed", body=function() {
				var u = s3GeneratePresignedURL( bucketNameOrPath=ctx.bucket, objectName=obj, httpMethod="PUT" );
				expect( signedHeaders( u ) ).toBe( "host" );
			});

			it( title="sseCustomerKey together with sseAlgorithm is a clear error", body=function() {
				expect( function() {
					s3GeneratePresignedURL( bucketNameOrPath=ctx.bucket, objectName=obj, httpMethod="PUT", sseAlgorithm="AES256", sseCustomerKey=sseKey );
				}).toThrow( message="can not be used together" );
			});

		});
	}

	/** the signed headers of a presigned URL, sorted, as a comma separated list */
	private string function signedHeaders( required string presignedUrl ) {
		var q = listLast( arguments.presignedUrl, "?" );
		for ( var pair in listToArray( q, "&" ) ) {
			if ( lcase( listFirst( pair, "=" ) ) == "x-amz-signedheaders" ) {
				return listSort( replace( urlDecode( listRest( pair, "=" ) ), ";", ",", "all" ), "text", "asc" );
			}
		}
		throw( message="no X-Amz-SignedHeaders in [#arguments.presignedUrl#]" );
	}
}
