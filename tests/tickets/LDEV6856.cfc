/**
 * LDEV-6856: server side encryption (sseAlgorithm) and checksum of s3GeneratePresignedURL.
 * https://luceeserver.atlassian.net/browse/LDEV-6856
 *
 * - the signed headers of the presigned URL run against the S3 compatible server of the CI (MinIO) and need no
 *   encryption support of the server.
 * - the storageClass specs of the original are not part of this version, as it has no storageClass argument.
 * - what the server does with a presigned upload (SSE-S3 header stored, SSE-KMS, Content-MD5 enforced) is only
 *   tested on Amazon: SSE-S3 and SSE-KMS need a server with encryption configured (MinIO refuses it without TLS/KMS),
 *   and a local server needs the path style URL.
 *   Documented skip: these specs are skipped when there are no Amazon credentials.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6856: sseAlgorithm and checksum are signed headers of the presigned URL", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6856p" );
				obj = CustomS3::key( ctx, "presign.txt" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="no sseAlgorithm and no checksum: only the host is signed", body=function() {
				expect( signedHeaders( presign( {} ) ) ).toBe( "host" );
			});

			it( title="sseAlgorithm AES256 or KMS (any case, trimmed) signs the x-amz-server-side-encryption header", body=function() {
				for ( var alg in [ "AES256", "aes256", " KMS ", "kms" ] ) {
					expect( signedHeaders( presign( { sseAlgorithm: alg } ) ) ).toBe( "host,x-amz-server-side-encryption", "sseAlgorithm [#alg#]" );
				}
			});

			it( title="checksum signs the content-md5 header", body=function() {
				var md5 = toBase64( binaryDecode( hash( "hello", "md5" ), "hex" ) );
				expect( signedHeaders( presign( { checksum: md5 } ) ) ).toBe( "content-md5,host" );
				expect( signedHeaders( presign( { checksum: md5, sseAlgorithm: "AES256" } ) ) ).toBe( "content-md5,host,x-amz-server-side-encryption" );
			});

			it( title="an invalid sseAlgorithm is an error", body=function() {
				expect( function() {
					presign( { sseAlgorithm: "ROT13" } );
				}).toThrow( message="SSE" );
			});

		});

		describe( title="LDEV-6856: upload with a presigned URL on Amazon (SSE-S3, SSE-KMS, checksum)", skip=Util::isAWSNotSupported(), body=function() {

			beforeEach( function() {
				cred = Util::getAWSCredentials();
				bucketName = Util::createBucketName( "6856" );
				obj = "presigned.txt";
				s3CreateBucket( bucketName=bucketName, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY );
			});

			afterEach( function() {
				Util::deleteBucketEL( cred, bucketName );
			});

			it( title="SSE-S3: the object is stored with AES256 when the header of the presigned URL is sent", body=function() {
				var rsp = put( url( { sseAlgorithm: "AES256" } ), "sse-s3", { "x-amz-server-side-encryption": "AES256" } );
				expect( val( rsp.statusCode ) ).toBe( 200, rsp.statusCode & " " & rsp.fileContent );
				Util::waitUntilReadable( cred, bucketName, obj );
				expect( header( meta(), "x-amz-server-side-encryption" ) ).toBe( "AES256" );
				expect( s3Read( bucketName=bucketName, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY ) ).toBe( "sse-s3" );
			});

			it( title="SSE-S3: the upload is refused without the signed header", body=function() {
				var rsp = put( url( { sseAlgorithm: "AES256" } ), "no-header", {} );
				expect( val( rsp.statusCode ) ).notToBe( 200 );
				expect( s3Exists( bucketName=bucketName, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY ) ).toBeFalse();
			});

			it( title="SSE-KMS: the object is stored with aws:kms", body=function() {
				var rsp = put( url( { sseAlgorithm: "KMS" } ), "sse-kms", { "x-amz-server-side-encryption": "aws:kms" } );
				expect( val( rsp.statusCode ) ).toBe( 200, rsp.statusCode & " " & rsp.fileContent );
				Util::waitUntilReadable( cred, bucketName, obj );
				expect( header( meta(), "x-amz-server-side-encryption" ) ).toBe( "aws:kms" );
			});

			it( title="checksum: the matching Content-MD5 is accepted, a different one is refused and stores nothing", body=function() {
				var good = toBase64( binaryDecode( hash( "checksum content", "md5" ), "hex" ) );
				var bad = toBase64( binaryDecode( hash( "something else", "md5" ), "hex" ) );

				var rsp = put( url( { checksum: bad } ), "checksum content", { "Content-MD5": bad } );
				expect( val( rsp.statusCode ) ).toBe( 400, "the content does not match the checksum: " & rsp.statusCode & " " & rsp.fileContent );
				expect( rsp.fileContent ).toInclude( "BadDigest" );
				expect( s3Exists( bucketName=bucketName, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY ) ).toBeFalse();

				rsp = put( url( { checksum: good } ), "checksum content", { "Content-MD5": good } );
				expect( val( rsp.statusCode ) ).toBe( 200, rsp.statusCode & " " & rsp.fileContent );
				Util::waitUntilReadable( cred, bucketName, obj );
				expect( s3Read( bucketName=bucketName, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY ) ).toBe( "checksum content" );
			});

		});
	}

	// ---- helpers ----

	private string function presign( required struct args ) {
		return s3GeneratePresignedURL( bucketNameOrPath=ctx.bucket, objectName=obj, httpMethod="PUT", sseAlgorithm=arguments.args.sseAlgorithm ?: "", checksum=arguments.args.checksum ?: "" );
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

	private string function url( required struct args ) {
		return s3GeneratePresignedURL( bucketNameOrPath=bucketName, objectName=obj, httpMethod="PUT", accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, sseAlgorithm=arguments.args.sseAlgorithm ?: "", checksum=arguments.args.checksum ?: "" );
	}

	private struct function put( required string presignedUrl, required string body, required struct headers ) {
		http url=arguments.presignedUrl method="put" result="local.rsp" {
			for ( var h in arguments.headers ) {
				httpparam type="header" name=h value=arguments.headers[ h ];
			}
			httpparam type="body" value=arguments.body;
		}
		return local.rsp;
	}

	private struct function meta() {
		return s3GetMetaData( bucketName=bucketName, objectName=obj, accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY );
	}

	private string function header( required struct meta, required string name ) {
		for ( var k in arguments.meta.headers ) {
			if ( lcase( k ) == lcase( arguments.name ) ) return arguments.meta.headers[ k ];
		}
		return "";
	}
}
