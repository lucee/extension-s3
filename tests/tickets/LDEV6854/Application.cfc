/**
 * LDEV-6854: s3 settings of the application with the "pool" settings (url variable "pool", JSON).
 * The endpoint is the self hosted server of the tests (S3_CUSTOM_* environment) when the url variable "custom" is
 * true, otherwise 127.0.0.1 with the url variable "port" and made up credentials.
 * Every scenario runs in its own application.
 */
component {

	this.name = "ldev6854-" & hash( ( url.port ?: "" ) & ( url.pool ?: "" ) & ( url.custom ?: "" ) );

	env = server.system.environment;

	// this version of the extension has no "ssl" / "pathStyleAccess" setting: a local server (plain http) is addressed with
	// the scheme in the host. The AWS SDK addresses the buckets virtual hosted (bucket.localhost) except for an IP
	// address, the self hosted server does not serve that (the bucket is not found, exists() answers false),
	// so "localhost" is replaced with the IP (same as CustomS3::settings)
	customHost = env.S3_CUSTOM_HOST ?: "";
	if ( reFindNoCase( "^(localhost|127\.0\.0\.1)(:|$)", customHost ) ) customHost = "http://" & reReplaceNoCase( customHost, "^localhost", "127.0.0.1" );

	if ( ( url.custom ?: false ) == true ) {
		this.s3 = {
			accessKeyId: env.S3_CUSTOM_ACCESS_KEY_ID ?: "",
			awsSecretKey: env.S3_CUSTOM_SECRET_KEY ?: "",
			host: customHost,
			defaultLocation: ( len( env.S3_CUSTOM_REGION ?: "" ) ? env.S3_CUSTOM_REGION : "us-east-1" )
		};
	}
	else {
		this.s3 = {
			accessKeyId: "ldev6854key",
			awsSecretKey: "ldev6854secret",
			host: "http://127.0.0.1:" & ( url.port ?: "1" ), // plain http, an IP address is always addressed path style
			defaultLocation: "us-east-1"
		};
	}
	if ( isJSON( url.pool ?: "" ) ) this.s3.pool = deserializeJSON( url.pool );
}
