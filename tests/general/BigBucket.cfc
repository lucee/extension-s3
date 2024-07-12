component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	
	function beforeAll() {
        variables.cred=Util::getAWSCredentials();
		variables.bucketName = Util::createBucketName("big");
    }

	function afterAll() {
        deleteBucketEL(variables.cred,variables.bucketName);
    }

	function run( testResults , testBox ) {
		
		describe( title="Test suite for big buckets",skip=Util::isAWSNotSupported(), body=function() {
			
			// does the bucket exists?
			if(!S3Exists( 
				bucketName:bucketName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				
				S3CreateBucket( 
					bucketName:bucketName,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
	
			}
			var executionTime=getTickCount();
			var qry=S3ListBucket( 
					bucketName:bucketName,
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			var executionTime=getTickCount()-executionTime;
			var records=qry.recordcount;
			
			if(records==0) {
				loop from=1 to=100 index="i" {
					S3Write( 
						value:i,
						bucketName:bucketName,
						objectName:"file#i#.txt",
						accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				}
			}

			it(title="checking with S3ListBucket", skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials()
			
				if(records==0){
					var executionTime=getTickCount();
					var qry=S3ListBucket( 
							bucketName:bucketName,
							accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
					var executionTime=getTickCount()-executionTime;
					var records=qry.recordcount;
				}
				
				assertTrue( executionTime<3000 );
				assertEquals(10000, records); // 10000] but received [861
			});


			it(title="checking with S3Exists", skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials()
			
				var executionTime=getTickCount();
				S3Exists( 
					bucketName:bucketName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				var executionTime=getTickCount()-executionTime;
				
				
				assertTrue( executionTime<100 );
				// assertEquals(10000, executionTime);
			});

		});
	}


}