/**
 * LDEV-6862: only ssl and pathStyleAccess are defined here (the MinIO of the CI speaks plain http), the
 * credentials are wrong on purpose and the host is not defined, index.cfm passes credentials and host as arguments.
 */
component {

	this.name = "ldev6862";

	this.s3 = {
		accessKeyId: "wrong",
		awsSecretKey: "wrong",
		ssl: false,
		pathStyleAccess: true
	};
}
