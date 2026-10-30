/**
 * LDEV-4003: copying and moving a whole directory on S3 (directoryCopy, directoryRename)
 * https://luceeserver.atlassian.net/browse/LDEV-4003
 *
 * With a Lucee that knows lucee.commons.io.res.ResourceCopyMove the extension copies and moves the
 * directory on the S3 side, with an older Lucee (or when the case is not supported, for example a
 * filter) Lucee does it object by object. The result has to be the same in all cases, that is what
 * these tests check.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-4003: directoryCopy and directoryRename on S3", skip=isNotSupported(), body=function() {

			beforeEach( function() {
				variables.cred = getCredentials();
				variables.bucket = "ldev4003-" & lcase( left( createUUID(), 8 ) ) & "-" & listFirst( replace( server.lucee.version, ".", "", "all" ), "-" );
				var s3settings = buildS3Settings( cred );
				application action="update" s3=s3settings;
				directoryCreate( s3p( "" ) );
			});

			afterEach( function() {
				deleteBucketEL( cred, bucket );
				if ( !isNull( variables.bucket2 ) ) deleteBucketEL( cred, variables.bucket2 );
				structDelete( variables, "bucket2" );
			});

			it( title="directoryCopy recursive copies all files, sub directories and empty directories", body=function() {
				var src = s3p( "src" );
				var trg = s3p( "trg" );
				buildTree( src );

				directoryCopy( src, trg, true );

				expectTree( trg );
				// source is untouched
				expectTree( src );
			});

			it( title="directoryRename moves all files, sub directories and empty directories", body=function() {
				var src = s3p( "from" );
				var trg = s3p( "to" );
				buildTree( src );

				directoryRename( src, trg );

				expectTree( trg );
				expect( directoryExists( src ) ).toBeFalse();
				expect( fileExists( src & "/a.txt" ) ).toBeFalse();
				expect( fileExists( src & "/sub/deep/c.txt" ) ).toBeFalse();
				expect( directoryExists( src & "/sub" ) ).toBeFalse();
			});

			it( title="only the content of the directory is copied, not a sibling with the same name prefix", body=function() {
				// "dir" and "dir2" share the prefix "dir", only "dir/" belongs to the directory "dir"
				fileWrite( s3p( "dir/a.txt" ), "in dir" );
				fileWrite( s3p( "dir2/b.txt" ), "in dir2" );

				directoryCopy( s3p( "dir" ), s3p( "copy" ), true );

				expect( fileRead( s3p( "copy/a.txt" ) ) ).toBe( "in dir" );
				expect( fileExists( s3p( "copy/b.txt" ) ) ).toBeFalse();
				expect( directoryExists( s3p( "copy2" ) ) ).toBeFalse();

				directoryRename( s3p( "dir" ), s3p( "moved" ) );
				expect( fileRead( s3p( "moved/a.txt" ) ) ).toBe( "in dir" );
				expect( directoryExists( s3p( "dir" ) ) ).toBeFalse();
				// the sibling is not touched
				expect( fileRead( s3p( "dir2/b.txt" ) ) ).toBe( "in dir2" );
			});

			it( title="a directory that only exists as key prefix (no directory marker objects) can be copied and moved", body=function() {
				// only the objects are written, no directory is created explicitly
				fileWrite( s3p( "virtual/v1.txt" ), "v1" );
				fileWrite( s3p( "virtual/inner/v2.txt" ), "v2" );

				directoryCopy( s3p( "virtual" ), s3p( "virtualCopy" ), true );
				expect( fileRead( s3p( "virtualCopy/v1.txt" ) ) ).toBe( "v1" );
				expect( fileRead( s3p( "virtualCopy/inner/v2.txt" ) ) ).toBe( "v2" );
				expect( fileRead( s3p( "virtual/inner/v2.txt" ) ) ).toBe( "v2" );

				directoryRename( s3p( "virtual" ), s3p( "virtualMoved" ) );
				expect( fileRead( s3p( "virtualMoved/v1.txt" ) ) ).toBe( "v1" );
				expect( fileRead( s3p( "virtualMoved/inner/v2.txt" ) ) ).toBe( "v2" );
				expect( directoryExists( s3p( "virtual" ) ) ).toBeFalse();
				expect( fileExists( s3p( "virtual/v1.txt" ) ) ).toBeFalse();
			});

			it( title="directoryCopy with a filter only copies what matches the filter", body=function() {
				fileWrite( s3p( "filtered/keep1.txt" ), "k1" );
				fileWrite( s3p( "filtered/keep2.txt" ), "k2" );
				fileWrite( s3p( "filtered/skip.log" ), "s" );

				directoryCopy( s3p( "filtered" ), s3p( "filteredCopy" ), true, "*.txt" );

				expect( fileRead( s3p( "filteredCopy/keep1.txt" ) ) ).toBe( "k1" );
				expect( fileRead( s3p( "filteredCopy/keep2.txt" ) ) ).toBe( "k2" );
				expect( fileExists( s3p( "filteredCopy/skip.log" ) ) ).toBeFalse();
			});

			it( title="directoryRename to an existing directory still fails and changes nothing", body=function() {
				buildTree( s3p( "one" ) );
				fileWrite( s3p( "two/x.txt" ), "x" );

				expect( function() {
					directoryRename( s3p( "one" ), s3p( "two" ) );
				}).toThrow();

				expectTree( s3p( "one" ) );
				expect( fileRead( s3p( "two/x.txt" ) ) ).toBe( "x" );
				expect( fileExists( s3p( "two/a.txt" ) ) ).toBeFalse();
			});

			it( title="directoryCopy and directoryRename into a bucket that does not exist yet", body=function() {
				variables.bucket2 = bucket & "-b2";
				var other = "s3:///" & variables.bucket2;
				buildTree( s3p( "toOther" ) );

				directoryCopy( s3p( "toOther" ), other & "/copied", true );
				expectTree( other & "/copied" );

				directoryRename( s3p( "toOther" ), other & "/moved" );
				expectTree( other & "/moved" );
				expect( directoryExists( s3p( "toOther" ) ) ).toBeFalse();
			});

			it( title="directoryCopy and directoryRename with a target below a directory that does not exist yet", body=function() {
				buildTree( s3p( "deepSrc" ) );

				directoryCopy( source=s3p( "deepSrc" ), destination=s3p( "new/parent/copied" ), recurse=true, createPath=true );
				expectTree( s3p( "new/parent/copied" ) );
				expect( directoryExists( s3p( "new/parent" ) ) ).toBeTrue();

				directoryRename( s3p( "deepSrc" ), s3p( "other/parent/moved" ), true );
				expectTree( s3p( "other/parent/moved" ) );
				expect( directoryExists( s3p( "other/parent" ) ) ).toBeTrue();
				expect( directoryExists( s3p( "deepSrc" ) ) ).toBeFalse();
			});

			it( title="the parent directory of a moved directory does not vanish when it was the last thing in it", body=function() {
				fileWrite( s3p( "holder/only/a.txt" ), "a" );

				directoryRename( s3p( "holder/only" ), s3p( "elsewhere" ) );

				expect( fileRead( s3p( "elsewhere/a.txt" ) ) ).toBe( "a" );
				expect( directoryExists( s3p( "holder" ) ) ).toBeTrue();
				expect( directoryExists( s3p( "holder/only" ) ) ).toBeFalse();
			});

			it( title="a copied or moved directory can be listed", body=function() {
				buildTree( s3p( "listed" ) );

				directoryRename( s3p( "listed" ), s3p( "listedMoved" ) );

				var names = directoryList( s3p( "listedMoved" ), true, "name" );
				arraySort( names, "text" );
				expect( names ).toInclude( "a.txt" );
				expect( names ).toInclude( "b.txt" );
				expect( names ).toInclude( "c.txt" );
				expect( names ).toInclude( "empty" );
			});
		});
	}

	// the tree used in the tests: files in the root, nested sub directories and an empty directory
	private function buildTree( required string base ) {
		directoryCreate( base );
		fileWrite( base & "/a.txt", "content a" );
		fileWrite( base & "/b.txt", "content b" );
		directoryCreate( base & "/sub" );
		directoryCreate( base & "/sub/deep" );
		fileWrite( base & "/sub/deep/c.txt", "content c" );
		directoryCreate( base & "/empty" );
	}

	private function expectTree( required string base ) {
		expect( directoryExists( base ) ).toBeTrue( base );
		expect( fileRead( base & "/a.txt" ) ).toBe( "content a" );
		expect( fileRead( base & "/b.txt" ) ).toBe( "content b" );
		expect( directoryExists( base & "/sub" ) ).toBeTrue( base & "/sub" );
		expect( directoryExists( base & "/sub/deep" ) ).toBeTrue( base & "/sub/deep" );
		expect( fileRead( base & "/sub/deep/c.txt" ) ).toBe( "content c" );
		expect( directoryExists( base & "/empty" ) ).toBeTrue( base & "/empty" );
		expect( directoryList( base & "/empty" ) ).toBeEmpty();
		// nothing more than what we created
		expect( directoryList( base, true, "path" ) ).toHaveLength( 6, serializeJSON( directoryList( base, true, "path" ) ) );
	}

	private string function s3p( required string relative ) {
		// no credentials in the path, they come from the application settings
		return "s3:///" & bucket & "/" & relative;
	}

	private struct function buildS3Settings( required struct cred ) {
		var settings = { accessKeyId: cred.ACCESS_KEY_ID, awsSecretKey: cred.SECRET_KEY };
		// the credentials of the application replace the ones from the environment, so a host defined
		// by environment variable (S3 compatible service instead of Amazon) has to be passed on
		var host = server.system.environment.LUCEE_S3_HOST ?: "";
		if ( len( host ) ) settings.host = host;
		return settings;
	}

	private boolean function isNotSupported() {
		return structIsEmpty( getCredentials() );
	}

	// same credentials the other tests of this extension use (Amazon S3)
	private struct function getCredentials() {
		var env = server.system.environment;
		if ( len( env.S3_AMAZON_ACCESS_KEY_ID ?: "" ) && len( env.S3_AMAZON_SECRET_KEY ?: "" ) ) {
			return { ACCESS_KEY_ID: env.S3_AMAZON_ACCESS_KEY_ID, SECRET_KEY: env.S3_AMAZON_SECRET_KEY };
		}
		return {};
	}

	private function deleteBucketEL( required struct cred, required string bucketName ) {
		var bucketPath = "s3:///" & bucketName;
		if ( directoryExists( bucketPath ) ) directoryDelete( bucketPath, true );
	}
}
