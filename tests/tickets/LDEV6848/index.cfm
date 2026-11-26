<cfscript>
	// executes one S3 operation and returns the result as JSON, errors are returned as "error"
	result = {};
	sys = createObject( "java", "java.lang.System" );
	props = [ "lucee.s3.accesskeyid", "lucee.s3.secretaccesskey", "lucee.s3.host" ];
	try {
		// the credentials and the host of the "env" mode come from the system properties,
		// they only exist while this request runs
		cfg = isJSON( url.cfg ?: "" ) ? deserializeJSON( url.cfg ) : {};
		if ( ( cfg.mode ?: "" ) == "env" ) {
			env = server.system.environment;
			sys.setProperty( props[ 1 ], env.S3_CUSTOM_ACCESS_KEY_ID );
			sys.setProperty( props[ 2 ], env.S3_CUSTOM_SECRET_KEY );
			sys.setProperty( props[ 3 ], env.S3_CUSTOM_HOST );
		}

		switch ( url.op ) {
			case "create":
				S3CreateBucket( bucketName=url.bucket );
				result.value = true;
			break;
			case "write":
				S3Write( value=url.value, bucketName=url.bucket, objectName=url.object );
				result.value = true;
			break;
			case "read":
				result.value = S3Read( bucketName=url.bucket, objectName=url.object );
			break;
			case "exists":
				result.value = S3Exists( bucketName=url.bucket, objectName=url.object );
			break;
			case "delete":
				S3Delete( bucketName=url.bucket, force=true );
				result.value = true;
			break;
			case "url":
				result.value = S3GeneratePresignedURL( bucketNameOrPath=url.bucket, objectName=url.object );
			break;
			case "urlOfPath":
				result.value = S3GeneratePresignedURL( bucketNameOrPath=url.path );
			break;
			case "vfsWrite":
				fileWrite( url.path, url.value );
				result.value = true;
			break;
			case "vfsRead":
				result.value = fileRead( url.path );
			break;
			case "vfsExists":
				result.value = fileExists( url.path );
			break;
			default:
				throw( message="unknown operation [#url.op#]" );
		}
	}
	catch ( any e ) {
		result.error = e.message;
	}
	// not in a finally block (that combination does not compile with the loop on every Lucee version)
	sys.clearProperty( props[ 1 ] );
	sys.clearProperty( props[ 2 ] );
	sys.clearProperty( props[ 3 ] );
	echo( serializeJSON( result ) );
</cfscript>
