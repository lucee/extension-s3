<cfscript>
	// exists() of the same object "count" times in parallel, returns what every call answered
	path = "s3:///" & url.bucket & "/" & url.key;
	results = [];
	arraySet( results, 1, val( url.count ), "" );
	results = results.map( function( p ) {
		try {
			var ex = fileExists( path );
			if ( !ex ) {
				// DIAG6854 temporary
				var d = [];
				try { d.append( "s3 settings: " & serializeJSON( getApplicationSettings().s3 ?: "none" ) ); } catch ( any e1 ) { d.append( "settings err " & e1.message ); }
				try { d.append( "fileExists again: " & fileExists( path ) ); } catch ( any e2 ) { d.append( "fileExists again threw: " & e2.type & " " & e2.message ); }
				try { d.append( "s3Exists: " & s3Exists( bucketName=url.bucket, objectName=url.key ) ); } catch ( any e3 ) { d.append( "s3Exists threw: " & e3.type & " " & e3.message ); }
				try { d.append( "listBucket rows: " & s3ListBucket( bucketName=url.bucket ).recordCount ); } catch ( any e4 ) { d.append( "listBucket threw: " & e4.type & " " & e4.message ); }
				try { d.append( "getBucketLocation-ish dirExists: " & directoryExists( "s3:///" & url.bucket & "/" ) ); } catch ( any e5 ) { d.append( "dirExists threw: " & e5.type & " " & e5.message ); }
				systemOutput( "DIAG6854 fileExists=false path=#path# :: " & arrayToList( d, " || " ), true, true );
			}
			return ex ? "true" : "false";
		}
		catch ( any e ) {
			if ( findNoCase( "does not exist", e.message ) ) return "missing: " & e.message;
			return "error: " & e.message;
		}
	}, true, val( url.count ) );
	cfcontent( reset=true, type="application/json" );
	writeOutput( serializeJSON( results ) );
</cfscript>
