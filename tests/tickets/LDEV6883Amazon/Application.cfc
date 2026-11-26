/**
 * LDEV-6883: Amazon only. The S3 configuration comes from the "cfg" url variable (JSON), so every scenario
 * runs in its own application: cfg.defaultLocation (region), cfg.prefixes (this.s3.bucketPrefixes), cfg.suffixes
 * (this.s3.bucketSuffixes), cfg.prefixExclude / cfg.exclude and an optional cfg.mapping (this.vfs.s3.<key>, a mapping
 * in another region that inherits the prefixes and suffixes).
 */
component {

	cfg = isJSON( url.cfg ?: "" ) ? deserializeJSON( url.cfg ) : {};

	this.name = "ldev6883a-" & hash( url.cfg ?: "" );

	env = server.system.environment;

	this.s3 = {
		accessKeyId: env.S3_AMAZON_ACCESS_KEY_ID ?: "",
		awsSecretKey: env.S3_AMAZON_SECRET_KEY ?: "",
		defaultLocation: cfg.defaultLocation ?: "us-east-1"
	};
	if ( structKeyExists( cfg, "prefixes" ) ) this.s3.bucketPrefixes = cfg.prefixes;
	if ( structKeyExists( cfg, "prefixExclude" ) ) this.s3.bucketPrefixExclude = cfg.prefixExclude;
	if ( structKeyExists( cfg, "suffixes" ) ) this.s3.bucketSuffixes = cfg.suffixes;
	if ( structKeyExists( cfg, "exclude" ) ) this.s3.bucketSuffixExclude = cfg.exclude;

	if ( structKeyExists( cfg, "mapping" ) ) {
		this.vfs.s3[ cfg.mapping.key ] = {
			accessKeyId: this.s3.accessKeyId,
			awsSecretKey: this.s3.awsSecretKey,
			defaultLocation: cfg.mapping.defaultLocation
		};
	}
}
