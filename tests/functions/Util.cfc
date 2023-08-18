component  {
	
	public static boolean function isAWSNotSupported() {
		res= getAWSCredentials();
		return isNull(res) || len(res)==0;
	}
	public static struct function getAWSCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var PREFIX=server.system.environment.S3_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,PREFIX:PREFIX};
	}
	
	public static boolean function isBackBlazeNotSupported() {
		res= getBackBlazeCredentials();
		return isNull(res) || len(res)==0;
	}
	public static struct function getBackBlazeCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_BACKBLAZE_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_BACKBLAZE_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var HOST=server.system.environment.S3_BACKBLAZE_HOST?:nullValue();
		if(isNull(HOST) || isEmpty(HOST)) return {};
		
		var PREFIX=server.system.environment.S3_BACKBLAZE_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,HOST:HOST,PREFIX:PREFIX};
	}

	public static boolean function isWasabiNotSupported() {
		res= getWasabiCredentials();
		return isNull(res) || len(res)==0;
	}
	public static struct function getWasabiCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_WASABI_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_WASABI_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var HOST=server.system.environment.S3_WASABI_HOST?:nullValue();
		if(isNull(HOST) || isEmpty(HOST)) return {};
		
		var PREFIX=server.system.environment.S3_WASABI_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,HOST:HOST,PREFIX:PREFIX};
	}
	public static boolean function isWasabiNotSupported() {
		res= getWasabiCredentials();
		return isNull(res) || len(res)==0;
	}
	public static struct function getWasabiCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_WASABI_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_WASABI_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var HOST=server.system.environment.S3_WASABI_HOST?:nullValue();
		if(isNull(HOST) || isEmpty(HOST)) return {};
		
		var PREFIX=server.system.environment.S3_WASABI_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,HOST:HOST,PREFIX:PREFIX};
	}
	public static boolean function isGoogleNotSupported() {
		res= getGoogleCredentials();
		return true; // disbled because current account is invalid
		return isNull(res) || len(res)==0;
	}
	public static struct function getGoogleCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_GOOGLE_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_GOOGLE_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var HOST=server.system.environment.S3_GOOGLE_HOST?:nullValue();
		if(isNull(HOST) || isEmpty(HOST)) return {};
		
		var PREFIX=server.system.environment.S3_GOOGLE_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,HOST:HOST,PREFIX:PREFIX};
	}

}