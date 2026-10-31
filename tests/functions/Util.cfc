component  {
	
	
	static function createBucketName(name) {
		return "ldev-"&lcase(name)&"-"&listFirst(replace(server.lucee.version,".","","all"),"-")&"-"&lcase(left(createUUID(),5));
	}

	/**
	 * A freshly created bucket/object is not always visible right away (AWS answers NoSuchBucket/NoSuchKey
	 * for a short time after the bucket was created by S3Write). Polls with a short backoff until the object
	 * can be read, gives up after timeoutMs and rethrows the last error. Any other error is rethrown at once.
	 */
	static function waitUntilReadable(cred,bucketName,objectName,timeoutMs=30000) {
		var start=getTickCount();
		var pause=100;
		while(true) {
			try {
				S3ReadBinary( 
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				return getTickCount()-start;
			}
			catch(e) {
				var msg=(e.message?:"")&" "&(e.detail?:"");
				var notYetVisible=findNoCase("NoSuchBucket",msg) || findNoCase("NoSuchKey",msg);
				if(!notYetVisible || (getTickCount()-start+pause)>timeoutMs) rethrow;
			}
			sleep(pause);
			pause=min(pause*2,2000);
		}
	}

	/**
	 * true when the ACL (array of structs as returned by S3GetACL) contains a grant of the given
	 * permission to the given group, for example hasGrant(acl,"authenticated","READ")
	 */
	static function hasGrant(required array acl, required string group, required string permission) {
		loop array=arguments.acl item="local.grant" {
			if((grant.group?:"")==arguments.group && (grant.permission?:"")==arguments.permission) return true;
		}
		return false;
	}

	/**
	 * Calls fn, which has to fail, and returns the error message. Fails the test when fn does not throw.
	 */
	static function messageOfFailure(required function fn) {
		var failed=false;
		var msg="";
		try {
			arguments.fn();
		}
		catch(e) {
			failed=true;
			msg=e.message?:"";
		}
		if(!failed) throw "expected the call to throw an exception, but it did not";
		return msg;
	}

	/**
	 * Calls fn, which has to fail, and returns the type of the error. Fails the test when fn does not throw.
	 */
	static function typeOfFailure(required function fn) {
		var failed=false;
		var type="";
		try {
			arguments.fn();
		}
		catch(e) {
			failed=true;
			type=e.type?:"";
		}
		if(!failed) throw "expected the call to throw an exception, but it did not";
		return type;
	}

	static function deleteBucketEL(cred,bucketName,objectName) {
		// callers pass "s3://name" or "name"; S3Exists/S3Delete need the plain bucket name
		if(left(bucketName,5)=="s3://") arguments.bucketName=mid(bucketName,6);
		try {
			if(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)))
				S3Delete(bucketName:bucketName,objectName:objectName?:nullValue(),force:true, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY,host:(isNull(cred.HOST)?nullvalue():cred.HOST));
		}
		catch(e) {
			// Backblaze will fail, because it no allows to delete newely created buckets
		}
		finally {

		}
	}

	static function deleteIfExists(cred,bucketName,objectName) {
		if(S3Exists( 
			bucketName:bucketName,  objectName:objectName?:nullValue(), 
			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)))
			S3Delete(bucketName:bucketName,objectName:objectName?:nullValue(),force:true, accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY,host:(isNull(cred.HOST)?nullvalue():cred.HOST));
	}


	public static boolean function isAWSNotSupported() {
		var res= getAWSCredentials();
		return isNull(res) || len(res)<2;
	}
	public static struct function getAWSCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_AMAZON_ACCESS_KEY_ID?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		
		var SECRET_KEY=server.system.environment.S3_AMAZON_SECRET_KEY?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};
		
		var PREFIX=server.system.environment.S3_AMAZON_BUCKET_PREFIX?:nullValue();
		if(isNull(PREFIX) || isEmpty(PREFIX)) local.prefix="ldev";

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY,PREFIX:PREFIX};
	}
	
	public static boolean function isBackBlazeNotSupported() {
		return true;// temp disabled becuae account reached cap
		var cred= getBackBlazeCredentials();
		if(isNull(cred) || len(cred)==0) return true;
		return false;
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