component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults , testBox ) {
		describe( "Loud test", function() {
			it( 'make a load test', parallel);
		});

	}

	private function parallel() threadCount=10 repetitition=2 {
		var udfs=getPageContext().getUDFs();
		var meta=getMetadata(udfs[len(udfs)]);
		
		if(meta.threadcount<1 || meta.threadcount>1000) {
			throw "thread count need to be a number between 1 and 1000, now it is [#meta.threadcount#]";
		}
		if(meta.repetitition<1 || meta.repetitition>1000) {
			throw "repetitition need to be a number between 1 and 1000, now it is [#meta.repetitition#]";
		}
		if(meta.threadcount==1 || meta.repetitition==1) {
			throw "repetitition or thread count need to be bigger than 1";
		
		var names = [];    
		var exceptions = [];    
		for (var i = 1; i <= arguments.repetitition; i++) {
			for (var y = 1; y <= arguments.threadcount; y++) {
				var name="testThread:#i#:#y#";
				arrayAppend(names, name);
				thread action="run" name=name udf=code args=arguments exceptions=exceptions {
					try {
						udf(argumentCollection:args);
					}
					catch(e) {
						arrayAppend(exceptions, e);
					}
					
				}
			}
		}
		thread action="join" name=arrayToList(names); 

		if(len(exceptions)) {
			throw exceptions[1];
		}
	}

	private function code() {
		systemOutput("execute the code",1,1);
		throw "upsi dupsi!";
	}

}