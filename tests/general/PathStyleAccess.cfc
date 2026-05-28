component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {
		describe( title="Test suite for pathStyleAccess setting", body=function() {

			// AWS — path-style explicitly disabled (virtual-hosted style)
			it(title="AWS: pathStyleAccess=false — basic file write/read/delete succeeds", skip=Util::isAWSNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getAWSCredentials(), false);
			});

			// AWS — path-style explicitly enabled
			it(title="AWS: pathStyleAccess=true — basic file write/read/delete succeeds", skip=Util::isAWSNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getAWSCredentials(), true);
			});

			// AWS — pathStyleAccess not set (null / default behaviour)
			it(title="AWS: pathStyleAccess unset (null) — basic file write/read/delete succeeds", skip=Util::isAWSNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getAWSCredentials(), nullValue());
			});

			// BackBlaze — path-style explicitly disabled
			it(title="BackBlaze: pathStyleAccess=false — basic file write/read/delete succeeds", skip=Util::isBackBlazeNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getBackBlazeCredentials(), false);
			});

			// BackBlaze — path-style explicitly enabled
			it(title="BackBlaze: pathStyleAccess=true — basic file write/read/delete succeeds", skip=Util::isBackBlazeNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getBackBlazeCredentials(), true);
			});

			// Wasabi — path-style explicitly disabled
			it(title="Wasabi: pathStyleAccess=false — basic file write/read/delete succeeds", skip=Util::isWasabiNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getWasabiCredentials(), false);
			});

			// Wasabi — path-style explicitly enabled
			it(title="Wasabi: pathStyleAccess=true — basic file write/read/delete succeeds", skip=Util::isWasabiNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getWasabiCredentials(), true);
			});

			// Google — path-style explicitly disabled
			it(title="Google: pathStyleAccess=false — basic file write/read/delete succeeds", skip=Util::isGoogleNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getGoogleCredentials(), false);
			});

			// Google — path-style explicitly enabled
			it(title="Google: pathStyleAccess=true — basic file write/read/delete succeeds", skip=Util::isGoogleNotSupported(), body=function( currentSpec ) {
				testWithPathStyleAccess(Util::getGoogleCredentials(), true);
			});

		});
	}

	/**
	 * Sets up the application S3 context with the given credentials and pathStyleAccess value,
	 * then performs a basic write/read/delete cycle to confirm the setting is accepted and
	 * the connection still works.
	 *
	 * @cred           Credential struct from Util (ACCESS_KEY_ID, SECRET_KEY, optional HOST/PREFIX)
	 * @pathStyleAccess  true, false, or null (omitted → uses provider default)
	 */
	private function testWithPathStyleAccess(required struct cred, any pathStyleAccess) localMode=true {
		// Build the s3 application struct; include pathStyleAccess only when it is not null
		var s3cfg = {
			accessKeyId:  cred.ACCESS_KEY_ID,
			awsSecretKey: cred.SECRET_KEY
		};
		if (!isNull(cred.HOST) && len(cred.HOST))
			s3cfg.host = cred.HOST;
		if (!isNull(arguments.pathStyleAccess))
			s3cfg.pathStyleAccess = arguments.pathStyleAccess;

		application action="update" s3=s3cfg;

		// Build a unique bucket / object path
		var bucketName = Util::createBucketName("pathstyleaccess");
		var objectName = "pathstyle-test.txt";
		var content    = "path style access test content";

		var pathWithCred = "s3://#cred.ACCESS_KEY_ID#:#cred.SECRET_KEY#@";
		if (!isNull(cred.HOST) && len(cred.HOST))
			pathWithCred &= cred.HOST;
		pathWithCred &= "/#bucketName#/#objectName#";

		// Implicit-credential VFS path (relies on application context set above)
		var pathImplicit = "s3://#bucketName#/#objectName#";

		try {
			// Bucket / object must not exist yet
			assertFalse(fileExists(pathWithCred), "File should not exist before creation");

			// Create parent bucket via VFS
			directoryCreate("s3://#cred.ACCESS_KEY_ID#:#cred.SECRET_KEY#@#(!isNull(cred.HOST) && len(cred.HOST) ? cred.HOST : '')#/#bucketName#/");

			// Write via explicit-credential VFS path
			fileWrite(pathWithCred, content);
			assertTrue(fileExists(pathWithCred), "File should exist after fileWrite");

			// Read back and verify content
			var readBack = fileRead(pathWithCred);
			assertEquals(content, readBack, "File content must match what was written");

			// Also accessible via implicit VFS path (uses application context credentials)
			assertTrue(fileExists(pathImplicit), "File should also be accessible via implicit VFS path");

			// Delete
			fileDelete(pathWithCred);
			assertFalse(fileExists(pathWithCred), "File should not exist after deletion");
		}
		finally {
			Util::deleteBucketEL(cred, bucketName);
		}
	}

}
