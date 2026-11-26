<cfscript>
	// calls an endpoint and returns whether it failed and how long it took
	start = getTickCount();
	failed = false;
	message = "";
	try {
		fileExists( "s3:///ldev6854-bucket/some/file.txt" );
	}
	catch ( any e ) {
		failed = true;
		message = e.message;
	}
	cfcontent( reset=true, type="application/json" );
	writeOutput( serializeJSON( { "failed": failed, "message": message, "duration": getTickCount() - start } ) );
</cfscript>
