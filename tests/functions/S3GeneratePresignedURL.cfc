component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GeneratePresignedURL()",skip=isNotSupported(), body=function() {
			it(title="checking function with a path", skip=isNotSupported(), body=function( currentSpec ) {
				var cred=getCredentials();
				var res=S3GeneratePresignedURL(
					path:"s3:///bundle-download/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				expect(res.status_code).toBe("200");
			});

			it(title="checking function with a path containing dot notation bucket (not existing)", skip=isNotSupported(), body=function( currentSpec ) {
				var cred=getCredentials();
				var res=S3GeneratePresignedURL(
					path:"s3:///bundle.download/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				expect(res.status_code).toBe("404");
			});

			it(title="checking function with a bucketname/objectname", skip=isNotSupported(), body=function( currentSpec ) {
				var cred=getCredentials();
				var res=S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				expect(res.status_code).toBe("200");
			});

			it(title="checking function with a bucketname/objectname with dot in bucketname", skip=isNotSupported(), body=function( currentSpec ) {
				var cred=getCredentials();
				var res=S3GeneratePresignedURL(
					bucketName:"bundle.download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				expect(res.status_code).toBe("404");
			});

			it( title="should handle dots in bucket names ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);

				// if it has a dot it should not start with http://bundle.download.s3.amazonaws.com

				assert(res.startsWith("http://s3.amazonaws.com/bundle.download"));
			});

			it( title="checking S3GeneratePresignedURL() with a httpmethod argument", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,httpmethod:"GET"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				http url=res result="local.res";
				expect(res.status_code).toBe("200");
			});

			it( title="checking S3GeneratePresignedURL() with a content type argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,type:"application/zip"
				);
				http url=res result="local.res";
				expect(res.mimeType).toBe("application/zip");
			});

			it( title="checking S3GeneratePresignedURL() with a host argument", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,host="s3.amazonaws.com"
				);
				expect(res).toInclude("amazonaws.com");
			})

			it( title="checking S3GeneratePresignedURL() with a sseAlgorithm=AES256 argument", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,sseAlgorithm:"AES256"
				);
				http url=res result="local.res";
				expect(res.header).toInclude("AES256");
			})

			it( title="checking S3GeneratePresignedURL() with a sseCustomerKey argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,sseCustomerKey:"a8cfde6331ssssssbd59eb2ac96f8911c4b666"
				);
				http url=res result="local.res";
				expect(res.status_code).toBe("200");
			})

			it( title="checking S3GeneratePresignedURL() with a checksum argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					 bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,checksum:"SHA256"
				);
				expect(res).toInclude("SHA256");
			})

			it( title="checking S3GeneratePresignedURL() with a disposition argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,disposition:"inline "
				);
				expect(res).toInclude("inline");
			})

			it( title="checking S3GeneratePresignedURL() with a encoding argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,encoding:"udf-8"
				);
				expect(res).toInclude("udf-8");
			})

			it( title="checking S3GeneratePresignedURL() with a version argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,version:"12121212"
				);
				expect(res).toInclude("12121212");
			});

			it( title="checking S3GeneratePresignedURL() with a responseHeaders argument ", skip=isNotSupported(), body=function(currentSpec){
				var cred=getCredentials();
				var res = S3GeneratePresignedURL(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
					,responseHeaders:{
						"value" = "101"
					}
				);
				expect(res).toInclude("x-amz-meta-value=101");
			})

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