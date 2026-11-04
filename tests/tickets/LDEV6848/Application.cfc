/**
 * LDEV-6848: the S3 configuration comes from the "cfg" url variable (JSON), so every test
 * scenario runs in its own application (name derived from the configuration).
 *
 * mode "env":     this.s3 only has ssl and pathStyleAccess, the credentials and the host come from the
 *                 system properties lucee.s3.* (set by index.cfm for the request)
 * mode "mapping": this.s3 defines a different endpoint setup (ssl true, path style off), the mapping
 *                 "ldev6848" (this.vfs.s3.ldev6848) has its own credentials, host, ssl and path style
 * mode "full":    complete configuration in this.s3, the reference setup
 */
component {

	cfg = isJSON( url.cfg ?: "" ) ? deserializeJSON( url.cfg ) : {};

	this.name = "ldev6848-" & hash( url.cfg ?: "" );

	env = server.system.environment;
	id = env.S3_CUSTOM_ACCESS_KEY_ID ?: "";
	secret = env.S3_CUSTOM_SECRET_KEY ?: "";
	host = env.S3_CUSTOM_HOST ?: "";

	mode = cfg.mode ?: "full";

	if ( mode == "env" ) {
		this.s3 = {
			ssl: cfg.ssl ?: false,
			pathStyleAccess: cfg.pathStyleAccess ?: true
		};
	}
	else if ( mode == "mapping" ) {
		// the top level settings are not the ones of the mapping
		this.s3 = {
			ssl: true,
			pathStyleAccess: false
		};
		this.vfs.s3.ldev6848 = {
			accessKeyId: id,
			awsSecretKey: secret,
			host: host,
			ssl: cfg.ssl ?: false,
			pathStyleAccess: cfg.pathStyleAccess ?: true
		};
	}
	else {
		this.s3 = {
			accessKeyId: id,
			awsSecretKey: secret,
			host: host,
			ssl: false,
			pathStyleAccess: true
		};
	}
}
