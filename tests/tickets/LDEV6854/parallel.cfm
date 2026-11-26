<cfscript>
	// exists() of the same object "count" times in parallel, returns what every call answered
	path = "s3:///" & url.bucket & "/" & url.key;
	results = [];
	arraySet( results, 1, val( url.count ), "" );
	results = results.map( function( p ) {
		try {
			return fileExists( path ) ? "true" : "false";
		}
		catch ( any e ) {
			if ( findNoCase( "does not exist", e.message ) ) return "missing: " & e.message;
			return "error: " & e.message;
		}
	}, true, val( url.count ) );
	cfcontent( reset=true, type="application/json" );
	writeOutput( serializeJSON( results ) );
</cfscript>
