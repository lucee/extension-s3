/**
 * LDEV-6838: the S3 configuration comes from the "cfg" url variable (JSON), so every test
 * scenario runs in its own application (name derived from the configuration).
 */
component {

	cfg = isJSON( url.cfg ?: "" ) ? deserializeJSON( url.cfg ) : {};

	this.name = "ldev6838-" & hash( url.cfg ?: "" );

	env = server.system.environment;
	host = env.S3_CUSTOM_HOST ?: "";

	this.s3 = {
		accessKeyId: env.S3_CUSTOM_ACCESS_KEY_ID ?: "",
		awsSecretKey: env.S3_CUSTOM_SECRET_KEY ?: "",
		host: host,
		ssl: false, // MinIO in CI listens on plain http
		defaultLocation: cfg.defaultLocation ?: "us-east-1"
	};
	if ( structKeyExists( cfg, "suffixes" ) ) this.s3.bucketSuffixes = cfg.suffixes;
	if ( structKeyExists( cfg, "exclude" ) ) this.s3.bucketSuffixExclude = cfg.exclude;

	// optional mapping specific override of the suffixes (this.vfs.s3.<key>)
	if ( structKeyExists( cfg, "mapping" ) ) {
		this.vfs.s3[ cfg.mapping.key ] = {
			accessKeyId: this.s3.accessKeyId,
			awsSecretKey: this.s3.awsSecretKey,
			host: this.s3.host,
			ssl: false,
			defaultLocation: cfg.mapping.defaultLocation ?: this.s3.defaultLocation
		};
		if ( structKeyExists( cfg.mapping, "suffixes" ) ) this.vfs.s3[ cfg.mapping.key ].bucketSuffixes = cfg.mapping.suffixes;
	}
}
