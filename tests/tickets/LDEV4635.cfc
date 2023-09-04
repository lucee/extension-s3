component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {


	private numeric function checkS3Version(){
		var s3Version = extensionList().filter(function(row){
			return (row.name contains "s3");
		}).version;
		return listFirst( s3Version, "." ) ;
	};

	private function copyToBucket( required credentials, required string storelocation, required string renameLocation="", boolean invalid=false ){
		arguments.bucket=getTestBucketUrl(credentials);
		try {
			var renameBucket = "";
			var srcDir = getTempDirectory() & createUniqueID() & "/";
			expect( directoryExists( arguments.bucket ) ).toBeFalse(); // exists creates a false positive

			directoryCreate( srcDir );
			fileWrite(srcDir & "region.txt", storelocation );

			if ( arguments.invalid ) {
				expect( function(){
					directory action="copy" directory="#srcDir#" destination="#arguments.bucket#" storelocation="#arguments.storelocation#";
				}).toThrow();
			} else {
				// try coping local dir to a new s3 bucket with a region
				try {
					directory action="copy" directory="#srcDir#" destination="#arguments.bucket#" storelocation="#arguments.storelocation#";
				} catch (e){
					throw REReplaceNoCase(e.stacktrace,"[***]", "all");
				}
				expect( directoryExists( arguments.bucket ) ).toBeTrue();
				if ( checkS3Version() neq 0 ) {
					var info = StoreGetMetadata( arguments.bucket ); // only works with v2 due to https://luceeserver.atlassian.net/browse/LDEV-4202
					expect( info ).toHaveKey( "region" );
					expect( info.region ).toBe( arguments.storelocation );
				}

				// now try rename to a bucket in a different region 
				// fails between regions https://luceeserver.atlassian.net/browse/LDEV-4639
				if ( len( renameLocation ) ) {
					renameBucket = getTestBucketUrl(credentials);
					try {
						directory action="rename" directory="#arguments.bucket#" newDirectory="#renameBucket#" storelocation="#arguments.renameLocation#";
					} catch (e){
						throw REReplaceNoCase(e.stacktrace,"[***]", "all");
					}
					expect( directoryExists( renameBucket ) ).toBeTrue();
					if ( checkS3Version() neq 0 ) {
						var info = StoreGetMetadata( renameBucket ); // only works with v2 due to https://luceeserver.atlassian.net/browse/LDEV-4202
						expect( info ).toHaveKey( "region" );
						expect( info.region ).toBe( arguments.renameLocation );
					}
				}

			}
		} finally {
			// BlazeBucket does not like to delete buckets
			Util::deleteBucketEL(credentials,srcDir);
			Util::deleteBucketEL(credentials,bucket);
			if ( !isEmpty( renameBucket )) Util::deleteBucketEL(renameBucket,srcDir);
		}
	}

	public function run( testResults , testBox ) {
		describe( title="Test suite for LDEV-4635 ( checking s3 copy directory operations )", body=function() {
			
			// Blackbaze
				it(title="Blackbaze: Copying dir to a new s3 bucket, valid region name [us-east-1]", skip=Util::isBackBlazeNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("BackBlaze"), "us-east-1" );
				});
	
				it(title="Blackbaze: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::isBackBlazeNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("BackBlaze"), "eu-west-1" );
				});
	
				it(title="Blackbaze: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::isBackBlazeNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("BackBlaze"), "eu-west-1" ); // fails, can't current copy between regions LDEV-4639
				});
	
				it(title="Blackbaze: Copying dir to a new s3 bucket, invalid region name [down-under]", skip=Util::isBackBlazeNotSupported(), body=function( currentSpec ){
					copyToBucket(getCredentials("BackBlaze"), "down-under", "", true );
				});
			
			// AWS
				it(title="AWS: Copying dir to a new s3 bucket, valid region name [us-east-1]", skip=Util::isAWSNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("AWS"), "us-east-1", "us-east-1" );
				});
	
				it(title="AWS: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::isAWSNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("AWS"), "eu-west-1", "eu-west-1" );
				});
	
				it(title="AWS: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::isAWSNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("AWS"), "eu-west-1", "eu-central-1" ); // fails, can't current copy between regions LDEV-4639
				});
	
				it(title="AWS: Copying dir to a new s3 bucket, invalid region name [down-under]", skip=Util::isAWSNotSupported(), body=function( currentSpec ){
					copyToBucket(getCredentials("AWS"), "down-under", "", true );
				});
			
			// Wasabi
				it(title="Wasabi: Copying dir to a new s3 bucket, valid region name [us-east-1]", skip=Util::getWasabiCredentials(), body=function( currentSpec ) {
					copyToBucket(getCredentials("Wasabi"), "us-east-1", "us-east-1" );
				});
	
				it(title="Wasabi: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::getWasabiCredentials(), body=function( currentSpec ) {
					copyToBucket(getCredentials("Wasabi"), "eu-west-1", "eu-west-1" );
				});
	
				it(title="Wasabi: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::getWasabiCredentials(), body=function( currentSpec ) {
					copyToBucket(getCredentials("Wasabi"), "eu-west-1", "eu-central-1" ); // fails, can't current copy between regions LDEV-4639
				});
	
				it(title="Wasabi: Copying dir to a new s3 bucket, invalid region name [down-under]", skip=Util::getWasabiCredentials(), body=function( currentSpec ){
					copyToBucket(getCredentials("Wasabi"), "down-under", "", true );
				});
			
			// Google
				it(title="Google: Copying dir to a new s3 bucket, valid region name [us-east-1]", skip=Util::isGoogleNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("Google"), "us-east-1", "us-east-1" );
				});
	
				it(title="Google: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::isGoogleNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("Google"), "eu-west-1", "eu-west-1" );
				});
	
				it(title="Google: Copying dir to a new s3 bucket, valid region name [eu-west-1]", skip=Util::isGoogleNotSupported(), body=function( currentSpec ) {
					copyToBucket(getCredentials("Google"), "eu-west-1", "eu-central-1" ); // fails, can't current copy between regions LDEV-4639
				});
	
				it(title="Google: Copying dir to a new s3 bucket, invalid region name [down-under]", skip=Util::isGoogleNotSupported(), body=function( currentSpec ){
					copyToBucket(getCredentials("Google"), "down-under", "", true );
				});


		});
	}

	private string function getTestBucketUrl(required credentials) localmode=true {
		local.s3Details=arguments.credentials;
		var bucketName = server.getTestService("s3").bucket_prefix & lcase("4635-#lcase(hash(CreateGUID()))#");
		return "s3://#s3Details.ACCESS_KEY_ID#:#s3Details.SECRET_KEY#@#(s3Details.HOST?:"")#/#bucketName#";
	}
	
	private function getCredentials(required string provider) localmode=true {
		if("BackBlaze"==arguments.provider) return Util::getBackBlazeCredentials();
		else if("AWS"==arguments.provider) return Util::getAWSCredentials();
		else if("Wasabi"==arguments.provider) return Util::getWasabiCredentials();
		else if("Google"==arguments.provider) return Util::getGoogleCredentials();
	}

}

