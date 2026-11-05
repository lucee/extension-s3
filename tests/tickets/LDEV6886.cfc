/**
 * LDEV-6886: s3Delete() accepts an array for objectName, to delete several objects with one call (in
 * batches of 1000 keys per request). An entry is a key or a struct with key and versionId. The result is a
 * struct with the deleted keys and the errors per key, a key that does not exist counts as deleted.
 * https://luceeserver.atlassian.net/browse/LDEV-6886
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		// the versioning of a bucket is changed, so the version tests need a bucket of their own (not S3_CUSTOM_BUCKET),
		// and a server that supports versioning (MinIO only does in some setups)
		variables.versioningSupported = false;
		if ( CustomS3::isSupported() && !len( CustomS3::getCredentials().BUCKET ) ) {
			var probe = CustomS3::prepare( "6886p" );
			try {
				var status = CustomS3::enableVersioning( probe );
				if ( status == 200 ) variables.versioningSupported = true;
				else if ( status != 501 ) throw( message="could not check if the server supports versioning: " & status );
			}
			finally {
				CustomS3::cleanup( probe );
			}
		}

		describe( title="LDEV-6886: s3Delete() with an array of objects", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6886" );
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="deletes all objects of the array and returns the deleted keys", body=function() {
				var keys = [ CustomS3::key( ctx, "a.txt" ), CustomS3::key( ctx, "b.txt" ), CustomS3::key( ctx, "c.txt" ) ];
				var keep = CustomS3::key( ctx, "keep.txt" );
				for ( var k in keys ) s3Write( bucketName=ctx.bucket, objectName=k, value="hello" );
				s3Write( bucketName=ctx.bucket, objectName=keep, value="keep me" );
				// the existence is cached, the delete has to clear the cache
				for ( var k in keys ) expect( s3Exists( bucketName=ctx.bucket, objectName=k ) ).toBeTrue();

				var result = s3Delete( bucketName=ctx.bucket, objectName=keys );

				expect( result ).toBeStruct();
				expect( result.deleted ).toBe( keys );
				expect( result.errors ).toBeArray();
				expect( arrayLen( result.errors ) ).toBe( 0 );
				for ( var k in keys ) {
					expect( s3Exists( bucketName=ctx.bucket, objectName=k ) ).toBeFalse( k );
					expect( fileExists( "s3:///" & ctx.bucket & "/" & k ) ).toBeFalse( k );
				}
				expect( s3Exists( bucketName=ctx.bucket, objectName=keep ) ).toBeTrue();
				expect( s3Read( bucketName=ctx.bucket, objectName=keep ) ).toBe( "keep me" );
			});

			it( title="works with positional arguments", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				var b = CustomS3::key( ctx, "b.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );
				s3Write( bucketName=ctx.bucket, objectName=b, value="hello" );

				var result = s3Delete( ctx.bucket, [ a, b ] );

				expect( result.deleted ).toBe( [ a, b ] );
				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName=b ) ).toBeFalse();
			});

			it( title="a key that does not exist counts as deleted", body=function() {
				var exists = CustomS3::key( ctx, "exists.txt" );
				var missing = CustomS3::key( ctx, "missing.txt" );
				var missingInFolder = CustomS3::key( ctx, "nofolder/missing.txt" );
				s3Write( bucketName=ctx.bucket, objectName=exists, value="hello" );

				var result = s3Delete( bucketName=ctx.bucket, objectName=[ exists, missing, missingInFolder ] );

				expect( result.deleted ).toBe( [ exists, missing, missingInFolder ] );
				expect( arrayLen( result.errors ) ).toBe( 0 );
				expect( s3Exists( bucketName=ctx.bucket, objectName=exists ) ).toBeFalse();

				// all of them are missing now, and also with throwOnError that is not an error
				result = s3Delete( bucketName=ctx.bucket, objectName=[ exists, missing ], throwOnError=true );
				expect( result.deleted ).toBe( [ exists, missing ] );
				expect( arrayLen( result.errors ) ).toBe( 0 );
			});

			it( title="more than 1000 keys are deleted in batches", body=function() {
				var count = 1001;
				var keys = [];
				for ( var i = 1; i <= count; i++ ) {
					var k = CustomS3::key( ctx, "bulk/file-" & numberFormat( i, "0000" ) & ".txt" );
					s3Write( bucketName=ctx.bucket, objectName=k, value="x" );
					arrayAppend( keys, k );
				}
				var keep = CustomS3::key( ctx, "bulk/keep.txt" );
				s3Write( bucketName=ctx.bucket, objectName=keep, value="keep me" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=keys[ 1 ] ) ).toBeTrue();
				expect( s3Exists( bucketName=ctx.bucket, objectName=keys[ count ] ) ).toBeTrue();

				var result = s3Delete( bucketName=ctx.bucket, objectName=keys );

				expect( arrayLen( result.deleted ) ).toBe( count );
				expect( result.deleted ).toBe( keys );
				expect( arrayLen( result.errors ) ).toBe( 0 );
				for ( var k in keys ) {
					if ( s3Exists( bucketName=ctx.bucket, objectName=k ) ) fail( "the object [#k#] still exists" );
				}
				expect( s3Exists( bucketName=ctx.bucket, objectName=keep ) ).toBeTrue();
			});

			it( title="an object is deleted once if it is in the array more than once", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );

				var result = s3Delete( bucketName=ctx.bucket, objectName=[ a, a ] );

				expect( result.deleted ).toBe( [ a ] );
				expect( arrayLen( result.errors ) ).toBe( 0 );
				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeFalse();
			});

			it( title="the names are exact keys, a folder is not deleted with its content", body=function() {
				var dir = CustomS3::key( ctx, "dir" );
				var child = dir & "/child.txt";
				s3Write( bucketName=ctx.bucket, objectName=child, value="hello" );

				// the array form does not know directories: "dir" is the key of a missing object
				var result = s3Delete( bucketName=ctx.bucket, objectName=[ dir ] );

				expect( result.deleted ).toBe( [ dir ] );
				expect( s3Exists( bucketName=ctx.bucket, objectName=child ) ).toBeTrue();
			});

			it( title="the folder of the deleted objects still exists", body=function() {
				var dir = CustomS3::key( ctx, "folder" );
				var a = dir & "/a.txt";
				var b = dir & "/sub/b.txt";
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );
				s3Write( bucketName=ctx.bucket, objectName=b, value="hello" );
				expect( directoryExists( "s3:///" & ctx.bucket & "/" & dir ) ).toBeTrue();
				expect( directoryExists( "s3:///" & ctx.bucket & "/" & dir & "/sub" ) ).toBeTrue();

				s3Delete( bucketName=ctx.bucket, objectName=[ a, b ] );

				// like with the delete of a single object: the folder was maybe only a pseudo folder
				expect( directoryExists( "s3:///" & ctx.bucket & "/" & dir ) ).toBeTrue();
				expect( directoryExists( "s3:///" & ctx.bucket & "/" & dir & "/sub" ) ).toBeTrue();
				expect( fileExists( "s3:///" & ctx.bucket & "/" & a ) ).toBeFalse();
				expect( fileExists( "s3:///" & ctx.bucket & "/" & b ) ).toBeFalse();
			});

			it( title="an empty array deletes nothing, and not the bucket", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );

				var result = s3Delete( bucketName=ctx.bucket, objectName=[] );

				expect( arrayLen( result.deleted ) ).toBe( 0 );
				expect( arrayLen( result.errors ) ).toBe( 0 );
				expect( s3Exists( bucketName=ctx.bucket ) ).toBeTrue();
				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeTrue();
			});

			it( title="a string still deletes a single object (and returns nothing)", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );

				var result = s3Delete( bucketName=ctx.bucket, objectName=a );

				expect( isNull( result ) ).toBeTrue();
				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeFalse();
				// and a missing object is still an error, if not ignored
				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=a );
				}).toThrow( message="does not exist" );
			});

			it( title="an entry that is not a valid object name is an error, nothing is deleted", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );

				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=[ a, "" ] );
				}).toThrow( message="not a valid object name" );
				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=[ a, { versionId: "abc" } ] );
				}).toThrow( message="needs the key" );

				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeTrue();
			});

			it( title="a bucket that does not exist is an error for the whole call", body=function() {
				expect( function() {
					s3Delete( bucketName=ctx.bucket & "-gone", objectName=[ "a.txt" ] );
				}).toThrow();
			});

			it( title="an object that can not be deleted is part of the result, the others are deleted", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				var bad = CustomS3::key( ctx, "bad.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );
				s3Write( bucketName=ctx.bucket, objectName=bad, value="hello" );

				// a version id that is not valid is rejected by the server for that key only
				var result = s3Delete( bucketName=ctx.bucket, objectName=[ a, { key: bad, versionId: "not-a-version" } ] );

				expect( result.deleted ).toBe( [ a ] );
				expect( arrayLen( result.errors ) ).toBe( 1 );
				expect( result.errors[ 1 ].key ).toBe( bad );
				expect( result.errors[ 1 ].versionId ).toBe( "not-a-version" );
				expect( len( result.errors[ 1 ].code ) ).toBeGT( 0 );
				expect( result.errors[ 1 ] ).toHaveKey( "message" );
				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName=bad ) ).toBeTrue();
			});

			it( title="throwOnError throws if an object could not be deleted, after the others were deleted", body=function() {
				var a = CustomS3::key( ctx, "a.txt" );
				var bad = CustomS3::key( ctx, "bad.txt" );
				s3Write( bucketName=ctx.bucket, objectName=a, value="hello" );
				s3Write( bucketName=ctx.bucket, objectName=bad, value="hello" );

				expect( function() {
					s3Delete( bucketName=ctx.bucket, objectName=[ a, { key: bad, versionId: "not-a-version" } ], throwOnError=true );
				}).toThrow( message="bad.txt" );

				expect( s3Exists( bucketName=ctx.bucket, objectName=a ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName=bad ) ).toBeTrue();
			});

		});

		describe( title="LDEV-6886: s3Delete() with an array, versions", skip=!variables.versioningSupported, body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6886v" );
				expect( CustomS3::enableVersioning( ctx ) ).toBe( 200 );
				obj = CustomS3::key( ctx, "versioned.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="version 1" );
				version1 = s3GetMetaData( bucketName=ctx.bucket, objectName=obj ).versionId;
				s3Write( bucketName=ctx.bucket, objectName=obj, value="version 2" );
				version2 = s3GetMetaData( bucketName=ctx.bucket, objectName=obj ).versionId;
				// the preconditions: two versions
				expect( len( version1 ) ).toBeGT( 0 );
				expect( len( version2 ) ).toBeGT( 0 );
				expect( version1 ).notToBe( version2 );
			});

			afterEach( function() {
				// a bucket with versions can not be removed, so delete the versions we know
				try {
					s3Delete( bucketName=ctx.bucket, objectName=[ { key: obj, versionId: version1 }, { key: obj, versionId: version2 } ] );
				}
				catch ( any e ) {
					systemOutput( "LDEV6886 cleanup failed: " & e.message, true );
				}
				CustomS3::cleanup( ctx );
			});

			it( title="a struct with key and versionId deletes only that version", body=function() {
				var result = s3Delete( bucketName=ctx.bucket, objectName=[ { key: obj, versionId: version1 } ] );

				expect( arrayLen( result.errors ) ).toBe( 0 );
				expect( arrayLen( result.deleted ) ).toBe( 1 );
				expect( result.deleted[ 1 ].key ).toBe( obj );
				expect( result.deleted[ 1 ].versionId ).toBe( version1 );

				// the other version is the current one and still there, the deleted one is gone
				expect( s3Read( bucketName=ctx.bucket, objectName=obj ) ).toBe( "version 2" );
				expect( s3Read( bucketName=ctx.bucket, objectName=obj, versionId=version2 ) ).toBe( "version 2" );
				expect( function() {
					s3Read( bucketName=ctx.bucket, objectName=obj, versionId=version1 );
				}).toThrow();
			});

			it( title="keys and versions can be mixed in one call", body=function() {
				var other = CustomS3::key( ctx, "other.txt" );
				s3Write( bucketName=ctx.bucket, objectName=other, value="other" );
				var otherVersion = s3GetMetaData( bucketName=ctx.bucket, objectName=other ).versionId;

				var result = s3Delete( bucketName=ctx.bucket, objectName=[ { key: obj, versionId: version1 }, { key: other, versionId: otherVersion }, { key: obj, versionId: version2 } ] );

				expect( arrayLen( result.errors ) ).toBe( 0 );
				expect( arrayLen( result.deleted ) ).toBe( 3 );
				expect( s3Exists( bucketName=ctx.bucket, objectName=other ) ).toBeFalse();
				expect( s3Exists( bucketName=ctx.bucket, objectName=obj ) ).toBeFalse();
			});

		});
	}
}
