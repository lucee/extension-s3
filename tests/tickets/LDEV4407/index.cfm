<cfscript>
	private struct function getCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_ACCESS_ID_TEST?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		var SECRET_KEY=server.system.environment.S3_SECRET_KEY_TEST?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY};
	}


	function test() {
		var props  = getCredentials();	
		var id="b"&lcase(left(replace(createUUID(),"-","","all"),10));
		var  dir="s3://#props.ACCESS_KEY_ID#:#props.SECRET_KEY#@/"&id&"/";
		var  file=dir&"test.txt";
		var hasAllRead=false;
		try {    
			directoryCreate(dir);
			fileWrite(file, "Susi Sorglos!");
			var res=storeGetACL(file);
			if(!isNull(res)) {
				loop array=res item="local.data" {
					dump(data);
					if((data.group?:"")=="all" && (data.permission?:"")=="READ") {
						hasAllRead=true;
						break;
					}
				}
			}
		}
		finally {
			if(directoryExists(dir)) directoryDelete(dir, true);
		}
		return hasAllRead;
	}
	echo(test());
	</cfscript>