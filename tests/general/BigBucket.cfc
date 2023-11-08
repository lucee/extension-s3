component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		
		describe( title="Test suite for big buckets",skip=isNotSupported(), body=function() {
			cred=getCredentials();
			var bucketName=cred.PREFIX&"-big"&listFirst(replace(server.lucee.version,".","","all"),"-");
			


			// does the bucket exists?
			if(!S3Exists( 
				bucketName:bucketName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				
				S3CreateBucket( 
					bucketName:bucketName
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
	
			}
			var executionTime=getTickCount();
			var qry=S3ListBucket( 
					bucketName:bucketName
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			var executionTime=getTickCount()-executionTime;
			var records=qry.recordcount;
			
			if(records==0) {
				loop from=1 to=10000 index="i" {
					S3Write( 
						value:i
						bucketName:bucketName
						objectName:"file#i#.txt"
						accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				}
			}

			it(title="checking with S3ListBucket", skip=isNotSupported(), body = function( currentSpec ) {
				var cred=getCredentials();
				
				if(records==0){
					var executionTime=getTickCount();
					var qry=S3ListBucket( 
							bucketName:bucketName
							accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
					var executionTime=getTickCount()-executionTime;
					var records=qry.recordcount;
				}
				assertEquals(200, executionTime);
			});
		});
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