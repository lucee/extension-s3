<cfscript>
	// executes one S3 operation with the credentials of the S3_CUSTOM_* environment variables passed as arguments,
	// returns the result as JSON, errors are returned as "error"
	result = {};
	try {
		env = server.system.environment;
		cred = {
			accessKeyId: env.S3_CUSTOM_ACCESS_KEY_ID,
			secretAccessKey: env.S3_CUSTOM_SECRET_KEY,
			host: env.S3_CUSTOM_HOST
		};
		switch ( url.op ) {
			case "write":
				S3Write( value=url.value, bucketName=url.bucket, objectName=url.object, accessKeyId=cred.accessKeyId, secretAccessKey=cred.secretAccessKey, host=cred.host );
				result.value = true;
			break;
			case "read":
				result.value = S3Read( bucketName=url.bucket, objectName=url.object, accessKeyId=cred.accessKeyId, secretAccessKey=cred.secretAccessKey, host=cred.host );
			break;
			case "exists":
				result.value = S3Exists( bucketName=url.bucket, accessKeyId=cred.accessKeyId, secretAccessKey=cred.secretAccessKey, host=cred.host );
			break;
			case "list":
				result.value = S3ListBucket( bucketName=url.bucket, prefix=url.prefix, accessKeyId=cred.accessKeyId, secretAccessKey=cred.secretAccessKey, host=cred.host ).recordCount;
			break;
			default:
				result.error = "unknown operation [#url.op#]";
		}
	}
	catch ( any e ) {
		result.error = e.message;
	}
	content type="application/json";
	echo( serializeJSON( result ) );
</cfscript>
