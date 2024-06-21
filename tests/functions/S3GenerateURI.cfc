component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GenerateURI()", body=function() {
			it(title="checking function with type virtualhost",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GenerateURI(
					path:"s3:///bundle-downloadx/sentry-log4j-1.7.22.jar"
					,type:"virtualhost"
					,secured:false
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("http://bundle-downloadx.s3.us-west-1.amazonaws.com/sentry-log4j-1.7.22.jar", res);
			});			
			it(title="checking function with type path",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GenerateURI(
					bucketName:"bundle-downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"path"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("https://s3.us-west-1.amazonaws.com/bundle-downloadx/sentry-log4j-1.7.22.jar", res);
			});				
			it(title="checking function with type arn",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GenerateURI(
					bucketName:"bundle.downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"arn"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("arn:aws:s3:::bundle.downloadx/sentry-log4j-1.7.22.jar", res);
			});					
			it(title="checking function with type arn",skip=isNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				var  res=S3GenerateURI(
					bucketName:"bundle-downloadx"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"s3"
					,accessKeyId:cred.ACCESS_KEY_ID
					,secretAccessKey:cred.SECRET_KEY
				);
				assertEquals("s3://bundle-downloadx/sentry-log4j-1.7.22.jar", res);
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