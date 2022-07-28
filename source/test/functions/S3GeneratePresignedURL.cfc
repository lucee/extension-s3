component extends="org.lucee.cfml.test.LuceeTestCase"{
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GeneratePresignedURL()", body=function() {
			it(title="checking function with a path", body = function( currentSpec ) {
				var  res=S3GeneratePresignedURL(
					path:"s3:///bundle-download/sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
				);
				http url=res result="local.res";
				assertEquals(200, res.status_code);
			});
			it(title="checking function with a bucketname/objectname", body = function( currentSpec ) {
				var  res=S3GeneratePresignedURL(
					bucketName:"bundle-download"
					objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
				);
				http url=res result="local.res";
				assertEquals(200, res.status_code);
			});

			
			it( title="should handle dots in bucket names ", body=function(currentSpec){
				var res = S3GeneratePresignedURL(
					bucketName:"bundle.download"
					objectName:"sentry-log4j-1.7.22.jar"
					,expire:dateAdd("n", 5, now())
				);

				// if it has a dot it should not start with http://bundle.download.s3.amazonaws.com

				assert(res.startsWith("http://s3.amazonaws.com/bundle.download"));
			});
		});
	}

	private function doFind(value){
		return value EQ "world";
	}
}