/**
 * Helper for the ticket tests that run against a self hosted S3 compatible server (MinIO in the CI,
 * see S3_CUSTOM_* in .github/workflows/main.yml). Uses the application settings, so the same
 * code works with plain HTTP and a custom host.
 *
 * Environment:
 *   S3_CUSTOM_ACCESS_KEY_ID, S3_CUSTOM_SECRET_KEY, S3_CUSTOM_HOST   required, otherwise the tests are skipped
 *   S3_CUSTOM_BUCKET                                                optional, use this existing bucket (with a unique key prefix) instead of creating one
 *   S3_CUSTOM_REGION                                                optional, region of the endpoint
 */
component {

	static boolean function isSupported() {
		return structCount( getCredentials() ) > 0;
	}

	static struct function getCredentials() {
		var env = server.system.environment;
		var id = env.S3_CUSTOM_ACCESS_KEY_ID ?: "";
		var secret = env.S3_CUSTOM_SECRET_KEY ?: "";
		var host = env.S3_CUSTOM_HOST ?: "";
		if ( !len( id ) || !len( secret ) || !len( host ) ) return {};
		return {
			ACCESS_KEY_ID: id,
			SECRET_KEY: secret,
			HOST: host,
			BUCKET: env.S3_CUSTOM_BUCKET ?: "",
			REGION: env.S3_CUSTOM_REGION ?: ""
		};
	}

	/**
	 * The application settings for the custom endpoint.
	 * @extra additional settings (like pool), they overrule the defaults
	 */
	static struct function settings( struct extra = {} ) {
		var cred = getCredentials();
		var result = {
			accessKeyId: cred.ACCESS_KEY_ID,
			awsSecretKey: cred.SECRET_KEY,
			host: cred.HOST,
			pathStyleAccess: true
		};
		if ( len( cred.REGION ) ) result.defaultLocation = cred.REGION;
		// a local server (MinIO) speaks plain HTTP: this version of the extension has no "ssl" / "pathStyleAccess"
		// setting, a scheme in the host is used instead. The AWS SDK addresses the buckets virtual hosted
		// (bucket.localhost) except for an IP address, MinIO does not serve that, so the IP is used.
		if ( reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", cred.HOST ) ) {
			result.host = "http://" & reReplaceNoCase( cred.HOST, "^localhost", "127.0.0.1" );
		}
		structAppend( result, arguments.extra );
		return result;
	}

	/**
	 * Sets the application settings for the custom endpoint and prepares a bucket (or, if S3_CUSTOM_BUCKET is set,
	 * a unique key prefix in that bucket).
	 * @return struct with bucket, prefix (empty or "xyz/"), created (true if the bucket was created), and cred
	 */
	static struct function prepare( required string name ) {
		var cred = getCredentials();
		application action="update" s3=settings();

		var ctx = { cred: cred, created: false, bucket: cred.BUCKET, prefix: "" };
		var unique = "ldev-" & lcase( arguments.name ) & "-" & lcase( left( createUUID(), 8 ) );
		if ( len( ctx.bucket ) ) {
			ctx.prefix = unique & "/";
		}
		else {
			ctx.bucket = unique & "-" & listFirst( replace( server.lucee.version, ".", "", "all" ), "-" );
			s3CreateBucket( bucketName: ctx.bucket );
			ctx.created = true;
		}
		return ctx;
	}

	/** path of an object for the functions that take an object name */
	static string function key( required struct ctx, required string name ) {
		return ctx.prefix & name;
	}

	/** removes everything the test created: the bucket, or the objects under the unique prefix */
	static function cleanup( required struct ctx ) {
		try {
			if ( ctx.created ) {
				directoryDelete( "s3:///" & ctx.bucket, true );
			}
			else if ( len( ctx.prefix ) ) {
				var p = "s3:///" & ctx.bucket & "/" & left( ctx.prefix, len( ctx.prefix ) - 1 );
				if ( directoryExists( p ) ) directoryDelete( p, true );
			}
		}
		catch ( any e ) {
			// cleanup must never hide the result of the test, but leave a trace
			systemOutput( "CustomS3 cleanup failed: " & e.message, true );
		}
	}

	/**
	 * enables the versioning of the bucket of the context (the extension has no function to change it)
	 * @return the http status code, 200 when the server supports versioning (MinIO only does in some setups)
	 */
	static numeric function enableVersioning( required struct ctx ) {
		var body = '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>';
		return CustomS3::signedRequest( ctx, "PUT", "/" & ctx.bucket, "versioning=", body ).status_code;
	}

	// ---- the extension has no function to change the versioning of a bucket, so the (few) requests
	// needed for the tests are sent signed (AWS Signature Version 4) with cfhttp

	static struct function signedRequest( required struct ctx, required string method, required string path, string query="", string body="" ) {
		var cred = ctx.cred;
		var local_ = reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", cred.HOST ) > 0;
		var host = reReplaceNoCase( cred.HOST, "^https?://", "" );
		var scheme = local_ || left( cred.HOST, 7 ) == "http://" ? "http" : "https";
		var region = len( cred.REGION ) ? cred.REGION : "us-east-1";
		if ( !len( cred.REGION ) && reFindNoCase( "^s3\.([a-z0-9-]+)\.wasabisys\.com$", host ) ) {
			region = reReplaceNoCase( host, "^s3\.([a-z0-9-]+)\.wasabisys\.com$", "\1" );
		}

		var now_ = dateConvert( "local2Utc", now() );
		var amzDate = dateTimeFormat( now_, "yyyyMMdd'T'HHnnss'Z'" );
		var day = dateTimeFormat( now_, "yyyyMMdd" );
		var payloadHash = lcase( hash( body, "SHA-256", "utf-8" ) );

		var canonicalHeaders = "host:" & host & chr( 10 ) & "x-amz-content-sha256:" & payloadHash & chr( 10 ) & "x-amz-date:" & amzDate & chr( 10 );
		var signedHeaders = "host;x-amz-content-sha256;x-amz-date";
		var canonical = method & chr( 10 ) & path & chr( 10 ) & query & chr( 10 ) & canonicalHeaders & chr( 10 ) & signedHeaders & chr( 10 ) & payloadHash;
		var scope = day & "/" & region & "/s3/aws4_request";
		var toSign = "AWS4-HMAC-SHA256" & chr( 10 ) & amzDate & chr( 10 ) & scope & chr( 10 ) & lcase( hash( canonical, "SHA-256", "utf-8" ) );

		var kDate = CustomS3::hmacBinary( "AWS4" & cred.SECRET_KEY, day );
		var kRegion = CustomS3::hmacBinary( kDate, region );
		var kService = CustomS3::hmacBinary( kRegion, "s3" );
		var kSigning = CustomS3::hmacBinary( kService, "aws4_request" );
		var signature = lcase( binaryEncode( CustomS3::hmacBinary( kSigning, toSign ), "hex" ) );

		var auth = "AWS4-HMAC-SHA256 Credential=" & cred.ACCESS_KEY_ID & "/" & scope & ", SignedHeaders=" & signedHeaders & ", Signature=" & signature;
		var reqUrl = scheme & "://" & host & path & ( len( query ) ? "?" & query : "" );
		http method=method url=reqUrl result="local.rsp" timeout=30 {
			httpparam type="header" name="x-amz-date" value=amzDate;
			httpparam type="header" name="x-amz-content-sha256" value=payloadHash;
			httpparam type="header" name="Authorization" value=auth;
			if ( len( body ) ) httpparam type="body" value=body;
		}
		return rsp;
	}

	static binary function hmacBinary( required any key, required string message ) {
		return binaryDecode( hmac( message, key, "HmacSHA256", "utf-8" ), "hex" );
	}

}
