/**
 * LDEV-6856: storageClass, server side encryption (sseAlgorithm) and checksum of s3GeneratePresignedURL.
 * https://luceeserver.atlassian.net/browse/LDEV-6856
 *
 * - storageClass (write, upload, copy, move) and the signed headers of the presigned URL run against the S3 compatible
 *   server of the CI (MinIO) and need no encryption support of the server.
 * - what the server does with a presigned upload (SSE-S3 header stored, SSE-KMS, Content-MD5 enforced) is only
 *   tested on Amazon: SSE-S3 and SSE-KMS need a server with encryption configured (MinIO refuses it without TLS/KMS),
 *   and a local server needs the path style URL.
 *   Documented skip: these specs are skipped when there are no Amazon credentials.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6856: storageClass", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6856s" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="s3Write with storageClass REDUCED_REDUNDANCY is stored with that class, any case", body=function() {
				var a = CustomS3::key( ctx, "write-a.txt" );
				var b = CustomS3::key( ctx, "write-b.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="a", storageClass="REDUCED_REDUNDANCY" );
				s3Write( bucketName=ctx.bucket, objectName=b, value="b", storageClass="reduced_redundancy" );
				expect( classOf( a ) ).toBe( "REDUCED_REDUNDANCY" );
				expect( classOf( b ) ).toBe( "REDUCED_REDUNDANCY" );
				expect( s3Read( bucketName=ctx.bucket, objectName=a ) ).toBe( "a" );
				expect( s3GetMetaData( bucketName=ctx.bucket, objectName=a ).headers[ "x-amz-storage-class" ] ?: "" ).toBe( "REDUCED_REDUNDANCY" );
			});

			it( title="without storageClass the object is STANDARD", body=function() {
				var a = CustomS3::key( ctx, "plain.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="a" );
				expect( classOf( a ) ).toBe( "STANDARD" );
			});

			it( title="s3Upload with storageClass", body=function() {
				var src = getTempFile( getTempDirectory(), "ldev6856" );
				fileWrite( src, "upload" );
				try {
					var a = CustomS3::key( ctx, "upload.txt" );
					s3Upload( bucketName=ctx.bucket, objectName=a, source=src, storageClass="REDUCED_REDUNDANCY" );
					expect( classOf( a ) ).toBe( "REDUCED_REDUNDANCY" );
					expect( s3Read( bucketName=ctx.bucket, objectName=a ) ).toBe( "upload" );
				}
				finally {
					if ( fileExists( src ) ) fileDelete( src );
				}
			});

			it( title="s3Copy and s3Move with storageClass", body=function() {
				var src = CustomS3::key( ctx, "src.txt" );
				var cp = CustomS3::key( ctx, "copy.txt" );
				var mv = CustomS3::key( ctx, "move.txt" );
				s3Write( bucketName=ctx.bucket, objectName=src, value="content" );
				expect( classOf( src ) ).toBe( "STANDARD" );

				s3Copy( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=cp, storageClass="REDUCED_REDUNDANCY" );
				expect( classOf( cp ) ).toBe( "REDUCED_REDUNDANCY" );
				expect( classOf( src ) ).toBe( "STANDARD", "the source is not touched by a copy" );
				expect( s3Read( bucketName=ctx.bucket, objectName=cp ) ).toBe( "content" );

				s3Move( srcBucketName=ctx.bucket, srcObjectName=src, trgBucketName=ctx.bucket, trgObjectName=mv, storageClass="REDUCED_REDUNDANCY" );
				expect( classOf( mv ) ).toBe( "REDUCED_REDUNDANCY" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=src ) ).toBeFalse();
				expect( s3Read( bucketName=ctx.bucket, objectName=mv ) ).toBe( "content" );
			});

			it( title="an invalid storageClass is an error for upload and move as well and writes nothing", body=function() {
				var src = getTempFile( getTempDirectory(), "ldev6856" );
				fileWrite( src, "x" );
				try {
					var a = CustomS3::key( ctx, "bad-upload.txt" );
					expect( function() {
						s3Upload( bucketName=ctx.bucket, objectName=a, source=src, storageClass="NOT_A_STORAGE_CLASS" );
					}).toThrow();
					expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeFalse();

					var m = CustomS3::key( ctx, "bad-move-src.txt" );
					var t = CustomS3::key( ctx, "bad-move-trg.txt" );
					s3Write( bucketName=ctx.bucket, objectName=m, value="m" );
					expect( function() {
						s3Move( srcBucketName=ctx.bucket, srcObjectName=m, trgBucketName=ctx.bucket, trgObjectName=t, storageClass="NOT_A_STORAGE_CLASS" );
					}).toThrow();
					expect( s3Exists( bucketName=ctx.bucket, objectName=m ) ).toBeTrue( "a failed move must keep the source" );
					expect( s3Exists( bucketName=ctx.bucket, objectName=t ) ).toBeFalse();
				}
				finally {
					if ( fileExists( src ) ) fileDelete( src );
				}
			});

		});

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

	/** the storage class of the object in the listing of the bucket */
	private string function classOf( required string objectName ) {
		var qry = s3ListBucket( bucketName=ctx.bucket );
		for ( var r = 1; r <= qry.recordCount; r++ ) {
			if ( qry.objectName[ r ] == arguments.objectName ) return qry.storageClass[ r ];
		}
		throw( message="object [#arguments.objectName#] not found in the listing" );
	}

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
