component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GenerateURI()", body=function() {
			it(title="checking function with type virtualhost", body = function( currentSpec ) {
				var  res=S3GenerateURI(
					path:"s3:///bundle-download/sentry-log4j-1.7.22.jar"
					,type:"virtualhost"
					,secured:false
				);
				assertEquals("http://bundle-download.s3.eu-west-1.amazonaws.com/sentry-log4j-1.7.22.jar", res);
			});			
			it(title="checking function with type path", body = function( currentSpec ) {
				var  res=S3GenerateURI(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"path"
				);
				assertEquals("https://s3.eu-west-1.amazonaws.com/bundle-download/sentry-log4j-1.7.22.jar", res);
			});				
			it(title="checking function with type arn", body = function( currentSpec ) {
				var  res=S3GenerateURI(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"arn"
				);
				assertEquals("arn:aws:s3:::bundle-download/sentry-log4j-1.7.22.jar", res);
			});					
			it(title="checking function with type arn", body = function( currentSpec ) {
				var  res=S3GenerateURI(
					bucketName:"bundle-download"
					,objectName:"sentry-log4j-1.7.22.jar"
					,type:"s3"
				);
				assertEquals("s3://bundle-download/sentry-log4j-1.7.22.jar", res);
			});			
		});
	}

	private function doFind(value){
		return value EQ "world";
	}
}