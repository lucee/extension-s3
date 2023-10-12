component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults , testBox ) {
		describe( "Loud test", function() {
			it( 'make a load test', parallel);

			parallel( 'make a load test',10,2, function () {
				systemOutput("execute the code",1,1);
				assertEquals("a", "b");
			});


		});

	}

	private function parallel(text, threadCount=1, repetitition=1, udf)  {
		if(arguments.threadcount<1 || arguments.threadcount>1000) {
			throw "thread count need to be a number between 1 and 1000, now it is [#arguments.threadcount#]";
		}
		if(arguments.repetitition<1 || arguments.repetitition>1000) {
			throw "repetitition need to be a number between 1 and 1000, now it is [#arguments.repetitition#]";
		}
		if(arguments.threadcount==1 || arguments.repetitition==1) {
			throw "repetitition or thread count need to be bigger than 1";
		}

		var names = [];    
		var exceptions = [];    
		for (var i = 1; i <= arguments.repetitition; i++) {
			for (var y = 1; y <= arguments.threadcount; y++) {
				var name="testThread:#i#:#y#";
				arrayAppend(names, name);
				thread action="run" name=name text=arguments.text udf=arguments.udf exceptions=exceptions {
					try {
						it(text,udf);
					}
					catch(e) {
						//arrayAppend(exceptions, e);
					}
				}
			}
		}
		thread action="join" name=arrayToList(names); 

		/*if(len(exceptions)) {
			loop array=exceptions item="local.e" {
				systemOutput(e,1,1);
			}
			throw exceptions[1];
		}*/

	}



	private function parallelX() threadCount=10 repetitition=2 {
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
		}
		var names = [];    
		var exceptions = [];    
		for (var i = 1; i <= meta.repetitition; i++) {
			for (var y = 1; y <= meta.threadcount; y++) {
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
			loop array=exceptions item="local.e" {
				systemOutput(e,1,1);
			}
			throw exceptions[1];
		}
	}

	private function code() {
		systemOutput("execute the code",1,1);
		assertEquals("a", "b");
	}

}