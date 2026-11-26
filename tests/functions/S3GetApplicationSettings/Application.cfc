/**
 * Application for the S3GetApplicationSettings() spec, the scenario is selected by the "mode" url variable.
 * Only fake values, nothing in here connects to a S3 server.
 */
component {

	this.name = "s3GetApplicationSettings-" & ( url.mode ?: "none" );

	mode = url.mode ?: "none";

	if ( mode == "s3" ) {
		this.s3 = {
			accessKeyId: "fakeAccessKeyId",
			awsSecretKey: "fakeSecretKey",
			host: "s3.example.org",
			ssl: false,
			pathStyleAccess: true,
			defaultLocation: "eu-west-1"
		};
	}
	else if ( mode == "custom" ) {
		this.s3 = { accessKeyId: "fakeAccessKeyId", awsSecretKey: "fakeSecretKey" };
		this.myCustomSetting = "hello";
		this.vfs.s3.mymapping = {
			accessKeyId: "mappingKeyId",
			awsSecretKey: "mappingSecret",
			host: "mapping.example.org"
		};
	}

	// a function must never show up in the result
	function helperFunction() {
		return "ignored";
	}
}
