/**
 * LDEV-6853: the S3 settings of the application, "pathStyleAccess" is only set when the "cfg" url variable (JSON)
 * has it, every scenario runs in its own application.
 */
component {

	cfg = isJSON( url.cfg ?: "" ) ? deserializeJSON( url.cfg ) : {};

	this.name = "ldev6853-" & hash( url.cfg ?: "" );

	env = server.system.environment;

	this.s3 = {
		accessKeyId: env.S3_CUSTOM_ACCESS_KEY_ID ?: "",
		awsSecretKey: env.S3_CUSTOM_SECRET_KEY ?: "",
		host: env.S3_CUSTOM_HOST ?: "",
		ssl: false, // the server of the tests listens on plain http
		defaultLocation: "us-east-1"
	};
	if ( structKeyExists( cfg, "pathStyleAccess" ) ) this.s3.pathStyleAccess = cfg.pathStyleAccess;
}
