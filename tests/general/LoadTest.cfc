component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" skip="true" {

	// Skipped on purpose: this is a manual load test (threadCount x repetition parallel round trips) that is too slow
	// and too expensive for the CI. Remove skip="true" to run it against Amazon or Wasabi.

	function run( testResults , testBox ) {
		describe( "Load test", function() {
			it(title="write and read back objects from many threads", skip=isNull(getCredentials()), body=function( currentSpec ) {
				var cred=getCredentials();
				var bucketName=Util::createBucketName("load-test");
				try {
					parallel(10,2,function(threadNumber) {
						roundTrip(cred,bucketName,threadNumber);
					});
				}
				finally {
					Util::deleteBucketEL(cred,bucketName);
				}
			});
		});
	}

	// first provider that is configured
	private function getCredentials()  {
		if(!Util::isAWSNotSupported()) return Util::getAWSCredentials();
		if(!Util::isWasabiNotSupported()) return Util::getWasabiCredentials();
		return nullValue();
	}

	// writes an object and reads the same content back
	private function roundTrip(cred,bucketName,threadNumber) {
		var objectName="load/#threadNumber#-#createUUID()#.txt";
		var value="Susi Sorglos #threadNumber#";
		S3Write( 
			value:value,
			bucketName:bucketName, objectName:objectName, 
			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
		// a freshly created bucket can answer NoSuchBucket for a moment on AWS
		var result="";
		for(var attempt=1;attempt<=10;attempt++) {
			try {
				result=S3Read( 
					bucketName:bucketName, objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
				break;
			}
			catch(e) {
				if(attempt==10 || !findNoCase("NoSuchBucket",(e.message?:"")&(e.detail?:""))) rethrow;
				sleep(attempt*200);
			}
		}
		if(result!=value) throw "expected [#value#], got [#result#]";
	}

	// runs udf in threadCount threads, repetition times one after the other, and rethrows the first exception
	private function parallel(threadCount, repetition, udf)  {
		if(arguments.threadCount<2 || arguments.threadCount>1000) {
			throw "thread count need to be a number between 2 and 1000, now it is [#arguments.threadCount#]";
		}
		if(arguments.repetition<1 || arguments.repetition>1000) {
			throw "repetition need to be a number between 1 and 1000, now it is [#arguments.repetition#]";
		}

		var exceptions = [];
		for (var i = 1; i <= arguments.repetition; i++) {
			var names = [];
			for (var y = 1; y <= arguments.threadCount; y++) {
				var name="testThread:#i#:#y#";
				arrayAppend(names, name);
				thread action="run" name=name work=arguments.udf threadNumber="#i#-#y#" {
					// an exception thrown here ends up in cfthread[name].error
					var task=attributes.work;
					task(attributes.threadNumber);
				}
			}
			thread action="join" name=arrayToList(names); 
			loop array=names item="local.n" {
				if(!isNull(cfthread[n].error)) arrayAppend(exceptions, cfthread[n].error);
			}
		}

		if(len(exceptions)) {
			loop array=exceptions item="local.e" {
				systemOutput(e.message,1,1);
			}
			throw exceptions[1];
		}
	}

}
