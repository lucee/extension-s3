component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GeneratePresignedURL()",skip=isNotSupported(), body=function() {
			it(title="checking function with a path", skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GeneratePresignedURL(
					path:"s3:///bundle-downloadx/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(200, res.status_code);
			});
			it(title="checking function with a path containing dot notation bucket (not existing)", skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GeneratePresignedURL(
					path:"s3:///bundle.downloadx/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(404, res.status_code);
			});

			it(title="checking function with a bucketname/objectname",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GeneratePresignedURL(
					bucketName:"bundle-downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(200, res.status_code);
			});

			it(title="checking function with a bucketname/objectname with dot in bucketname",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GeneratePresignedURL(
					bucketName:"bundle.downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				assertEquals(404, res.status_code);
			});

			
			it( title="should handle dots in bucket names ",skip=isNotSupported(), body=function(currentSpec){
				var cred=Util::getAWSCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle.downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);

				// if it has a dot it should not start with http://bundle.downloadx.s3.amazonaws.com

				assert(res.startsWith("https://s3.amazonaws.com/bundle.downloadx/sentry-log4j-1.7.22.jar"));
			});

			it( title="LDEV-6885 cacheControl, expires and contentLanguage are set as response header overrides", skip=isNotSupported(), body=function(currentSpec){
				var cred=Util::getAWSCredentials();
				var presignedUrl = S3GeneratePresignedURL(
					bucketName:"bundle-downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,cacheControl:"public, max-age=86400"
					,expires:"Thu, 01 Dec 2033 16:00:00 GMT"
					,contentLanguage:"en-US"
				);
				expect( presignedUrl ).toInclude( "response-cache-control=" );
				expect( presignedUrl ).toInclude( "response-expires=" );
				expect( presignedUrl ).toInclude( "response-content-language=" );
				expect( presignedUrl ).notToInclude( "x-amz-meta-response-" );

				http url=presignedUrl result="local.res";
				expect( res.status_code ).toBe( 200 );
				expect( res.responseheader["Cache-Control"] ).toBe( "public, max-age=86400" );
				expect( res.responseheader["Expires"] ).toBe( "Thu, 01 Dec 2033 16:00:00 GMT" );
				expect( res.responseheader["Content-Language"] ).toBe( "en-US" );
			});

			it( title="LDEV-6885 response-* keys in responseHeaders are not prefixed with x-amz-meta-", skip=isNotSupported(), body=function(currentSpec){
				var cred=Util::getAWSCredentials();
				var presignedUrl = S3GeneratePresignedURL(
					bucketName:"bundle-downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,responseHeaders:{ "response-cache-control":"no-cache", "Response-Content-Type":"application/java-archive" }
				);
				expect( presignedUrl ).toInclude( "response-cache-control=no-cache" );
				expect( presignedUrl ).toInclude( "response-content-type=" );
				expect( presignedUrl ).notToInclude( "x-amz-meta-response-" );

				http url=presignedUrl result="local.res";
				expect( res.status_code ).toBe( 200 );
				expect( res.responseheader["Cache-Control"] ).toBe( "no-cache" );
				expect( res.responseheader["Content-Type"] ).toBe( "application/java-archive" );
			});

			it( title="LDEV-6885 other responseHeaders keys still get the x-amz-meta- prefix", skip=isNotSupported(), body=function(currentSpec){
				var cred=Util::getAWSCredentials();
				var presignedUrl = S3GeneratePresignedURL(
					bucketName:"bundle-downloadx"
					,objectName:"ldev6885-upload.txt"
					,httpMethod:"PUT"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,responseHeaders:{ "foo":"bar", "x-amz-meta-baz":"qux" }
				);
				expect( presignedUrl ).toInclude( "x-amz-meta-foo=bar" );
				expect( presignedUrl ).toInclude( "x-amz-meta-baz=qux" );
				expect( presignedUrl ).notToInclude( "x-amz-meta-x-amz-meta-" );
			});
		});
	}

	private function doFind(value){
		return value EQ "world";
	}
	
	private boolean function isNotSupported() {
		res= Util::getAWSCredentials();
		return isNull(res) || len(res)==0;
	}

}