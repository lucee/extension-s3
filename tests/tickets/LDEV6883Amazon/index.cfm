<cfscript>
	// executes one S3 operation and returns the result as JSON, errors are returned as "error"
	result = {};
	try {
		hasRegion = structKeyExists( url, "region" ) && len( url.region );

		switch ( url.op ) {
			case "create":
				if ( hasRegion ) S3CreateBucket( bucketName=url.bucket, location=url.region );
				else S3CreateBucket( bucketName=url.bucket );
				result.value = true;
			break;
			case "write":
				if ( hasRegion ) S3Write( value=url.value, bucketName=url.bucket, objectName=url.object, location=url.region );
				else S3Write( value=url.value, bucketName=url.bucket, objectName=url.object );
				result.value = true;
			break;
			case "read":
				result.value = S3Read( bucketName=url.bucket, objectName=url.object );
			break;
			case "exists":
				if ( structKeyExists( url, "object" ) ) result.value = S3Exists( bucketName=url.bucket, objectName=url.object );
				else result.value = S3Exists( bucketName=url.bucket );
			break;
			case "delete":
				S3Delete( bucketName=url.bucket, force=true );
				result.value = true;
			break;
			case "buckets":
				result.value = valueArray( S3ListBuckets(), "name" );
			break;
			case "vfsExists":
				result.value = fileExists( url.path );
			break;
			case "vfsRead":
				result.value = fileRead( url.path );
			break;
			case "vfsWrite":
				fileWrite( url.path, url.value );
				result.value = true;
			break;
			case "vfsBuckets":
				result.value = directoryList( path="s3:///", listInfo="name" );
			break;
			case "vfsDirExists":
				result.value = directoryExists( url.path );
			break;
			case "url":
				result.value = S3GeneratePresignedURL( bucketNameOrPath=url.bucket, objectName=url.object );
			break;
			case "vfsList":
				// the names of the direct and nested entries, relative to the path
				result.value = directoryList( path=url.path, recurse=true, listInfo="path" ).map( function( p ) {
					return listLast( replace( p, "\", "/", "all" ), "/" );
				});
			break;
			case "vfsCopy":
				directoryCopy( url.source, url.destination, true );
				result.value = true;
			break;
			default:
				throw( message="unknown operation [#url.op#]" );
		}
	}
	catch ( any e ) {
		result.error = e.message;
	}
	echo( serializeJSON( result ) );
</cfscript>
