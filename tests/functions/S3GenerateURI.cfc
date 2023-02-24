component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GenerateURI()", body=function() {
			it(title="checking function with type virtualhost",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GenerateURI(
					path:"s3:///bundle-download/sentry-log4j-1.7.22.jar"
					,type:"virtualhost"
					,secured:false
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("http://bundle-download.s3.eu-west-1.amazonaws.com/sentry-log4j-1.7.22.jar", res);
			});			
			it(title="checking function with type path",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GenerateURI(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"path"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("https://s3.eu-west-1.amazonaws.com/bundle-download/sentry-log4j-1.7.22.jar", res);
			});				
			it(title="checking function with type arn",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GenerateURI(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"arn"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("arn:aws:s3:::bundle-download/sentry-log4j-1.7.22.jar", res);
			});					
			it(title="checking function with type arn",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GenerateURI(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"s3"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("s3://bundle-download/sentry-log4j-1.7.22.jar", res);
			});			
		});
	}

	private function doFind(value){
		return value EQ "world";
	}
	
	private boolean function isNotSupported() {
		res= getCredentials();
		return isNull(res) || len(res)==0;
	}
	private struct function getCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_ACCESS_ID_TEST?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		var SECRET_KEY=server.system.environment.S3_SECRET_KEY_TEST?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY};
	}
}