<cfscript>
	// creates a presigned URL with the settings of the Application.cfc next to this file, the arguments of the function
	// are the JSON "args" url variable, when "fetch" is set the URL is requested and the response returned as well,
	// errors are returned as "error"
	function arg( struct args, string name, any fallback ) {
		if ( structKeyExists( args, name ) ) return args[ name ];
		if ( !isNull( arguments.fallback ) ) return arguments.fallback;
		return nullValue();
	}

	result = {};
	try {
		// serializeJSON upper cases the keys, the function needs the names of its arguments
		names = "bucketNameOrPath,objectName,expireDate,httpMethod,sseAlgorithm,sseCustomerKey,checksum,type,disposition,encoding,version,zeroByte,responseHeaders,accessKeyId,secretAccessKey,host,timeout";
		args = {};
		loop struct=deserializeJSON( url.args ) index="k" item="v" {
			args[ listFindNoCase( names, k ) ? listGetAt( names, listFindNoCase( names, k ) ) : k ] = v;
		}
		if ( structKeyExists( args, "expireDate" ) ) args.expireDate = dateAdd( "n", args.expireDate, now() );
		// the function is called with all arguments by name (a struct as argumentCollection is checked at compile time),
		// an expireDate is always passed, 5 minutes when the test does not set one
		if ( !structKeyExists( args, "expireDate" ) ) args.expireDate = dateAdd( "n", 5, now() );
		result.url = S3GeneratePresignedURL(
			bucketNameOrPath=arg( args, "bucketNameOrPath" ), objectName=arg( args, "objectName" ), expireDate=args.expireDate,
			httpMethod=arg( args, "httpMethod" ), sseAlgorithm=arg( args, "sseAlgorithm" ), sseCustomerKey=arg( args, "sseCustomerKey" ),
			checksum=arg( args, "checksum" ), type=arg( args, "type" ), disposition=arg( args, "disposition" ),
			encoding=arg( args, "encoding" ), version=arg( args, "version" ), zeroByte=arg( args, "zeroByte", false ),
			responseHeaders=arg( args, "responseHeaders", {} ), accessKeyId=arg( args, "accessKeyId" ),
			secretAccessKey=arg( args, "secretAccessKey" ), host=arg( args, "host" ) );
		if ( structKeyExists( url, "fetch" ) ) {
			http method=url.fetch url=result.url result="rsp" timeout=30 {
				if ( structKeyExists( url, "body" ) ) httpparam type="body" value=url.body;
			}
			result.status = rsp.status_code;
			result.content = rsp.filecontent;
		}
	}
	catch ( any e ) {
		result.error = e.message;
	}
	content type="application/json";
	echo( serializeJSON( result ) );
</cfscript>
