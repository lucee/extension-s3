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
	 * Sets the application settings for the custom endpoint and prepares a bucket (or, if S3_CUSTOM_BUCKET is set,
	 * a unique key prefix in that bucket).
	 * @return struct with bucket, prefix (empty or "xyz/"), created (true if the bucket was created), and cred
	 */
	static struct function prepare( required string name ) {
		var cred = getCredentials();
		var settings = {
			accessKeyId: cred.ACCESS_KEY_ID,
			awsSecretKey: cred.SECRET_KEY,
			host: cred.HOST,
			pathStyleAccess: true
		};
		if ( len( cred.REGION ) ) settings.defaultLocation = cred.REGION;
		// a local server (MinIO) speaks plain HTTP: the application setting "ssl" is not passed on to the extension
		// by every Lucee version, a scheme in the host is
		if ( reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", cred.HOST ) ) settings.host = "http://" & cred.HOST;
		application action="update" s3=settings;

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
}
