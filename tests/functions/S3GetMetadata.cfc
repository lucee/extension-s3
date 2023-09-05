component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3GetMetadata()", body=function() {
			it(title="check with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials());
			});	

			it(title="check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials());
			});	

			it(title="check with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials());
			});		

			it(title="check with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials());
			});			
	
		});
	}


	private function testit(cred) {
		try {
			// create variables
			var bucketName=cred.PREFIX&"-get-metadata"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectName="sub/test.txt";
			
			Util::deleteIfExists(cred,bucketName,objectName);
			

			// create source bucket
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:"Susi Sorglos",
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}

			var meta=S3GetMetadata( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			
			//throw serialize(meta);
			assertEquals(bucketName, meta.bucketName);
			assertEquals(objectName, meta.objectName);

			// TODO add more checks,data looks like this
			//  {"bucketName":"lucee-ldev-e--get-metadata","directory":false,"xamz-server-side-encryption":"AES256","ownerName":"info","bucket":false,"eTag":"42ea390c02057acdf6ba36de2a655342","lastModified":createDateTime(2023,8,18,13,45,29,0,"Etc/UTC"),"size":12,"contentType":"UTF-8; charset=UTF-8","objectName":"sub/test.txt","file":true,"owner":"info","acceptRanges":"bytes","exists":true,"region":"us-east-1","acl":[{"permission":"FULL_CONTROL","displayName":"info","id":"c721e378b354ad60dd08acfde873d12c4bc8fccbea74afd0ca87362cff6a664d"}],"contentLength":12,"ownerNd":"c721e378b354ad60dd08acfde873d12c4bc8fccbea74afd0ca87362cff6a664d"}


		}
		catch(e) {
			if(!findNoCase("Transaction cap exceeded", e.message) ) throw e;
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
		
	}


	private function doFind(value){
		return value EQ "world";
	}
	
}