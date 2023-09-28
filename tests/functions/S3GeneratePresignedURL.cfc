component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GeneratePresignedURL()",skip=isNotSupported(), body=function() {
			it(title="checking function with a path", skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GeneratePresignedURL(
					path:"s3:///bundle-download/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(200, res.status_code);
			});
			it(title="checking function with a path containing dot notation bucket (not existing)", skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GeneratePresignedURL(
					path:"s3:///bundle.download/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(404, res.status_code);
			});

			it(title="checking function with a bucketname/objectname",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(200, res.status_code);
			});

			it(title="checking function with a bucketname/objectname with dot in bucketname",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				var  res=S3GeneratePresignedURL(
					bucketName:"bundle.download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(404, res.status_code);
			});

			
			it( title="should handle dots in bucket names ",skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle.download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);

				// if it has a dot it should not start with http://bundle.download.s3.amazonaws.com

				assert(res.startsWith("http://s3.amazonaws.com/bundle.download"));
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