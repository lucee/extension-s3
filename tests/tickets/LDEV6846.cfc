/**
 * LDEV-6846: s3ListBucket() has the arguments delimiter, startAfter and maxResults, and the query has the
 * additional columns eTag, storageClass and isPrefix.
 * https://luceeserver.atlassian.net/browse/LDEV-6846
 *
 * Runs against the S3 compatible server from the S3_CUSTOM_* environment variables (MinIO in the CI).
 * When S3_CUSTOM_BUCKET is used, the bucket holds more than the objects of the test, the tests only look at the
 * keys with the unique prefix of the test.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6846: s3ListBucket() delimiter, startAfter, maxResults and the columns eTag, storageClass", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6846" );
				// with a shared bucket, the unique prefix of the test contains a "/", so use a different delimiter there
				sep = len( ctx.prefix ) ? "~" : "/";
			});

			afterEach( function() {
				CustomS3::cleanup( ctx );
			});

			it( title="the query has the columns eTag, storageClass and isPrefix, in addition to the existing columns", body=function() {
				var obj = CustomS3::key( ctx, "one.txt" );
				s3Write( bucketName=ctx.bucket, objectName=obj, value="hello" );

				var qry = s3ListBucket( bucketName=ctx.bucket );
				var cols = listToArray( lcase( qry.columnList ) );
				for ( var c in [ "objectname", "size", "lastmodified", "owner", "etag", "storageclass", "isprefix" ] ) {
					expect( cols ).toInclude( c );
				}
				// the existing columns keep their place
				expect( left( lcase( qry.columnList ), len( "objectname,size,lastmodified,owner" ) ) ).toBe( "objectname,size,lastmodified,owner" );

				var row = mine( qry )[ 1 ];
				expect( row.objectName ).toBe( obj );
				expect( row.size ).toBe( 5 );
				expect( row.isPrefix ).toBeFalse();
				expect( len( row.eTag ) ).toBeGT( 0 );
				expect( len( row.storageClass ) ).toBeGT( 0 );

				// the eTag is the one of s3GetMetaData()
				var meta = s3GetMetaData( bucketName=ctx.bucket, objectName=obj );
				var metaETag = "";
				for ( var k in meta ) {
					if ( lcase( k ) == "etag" ) metaETag = meta[ k ];
				}
				expect( len( metaETag ) ).toBeGT( 0, "s3GetMetaData returned no eTag: #serializeJSON( structKeyList( meta ) )#" );
				expect( replace( row.eTag, '"', "", "all" ) ).toBe( replace( metaETag, '"', "", "all" ) );
			});

			it( title="without the new arguments all keys are listed and none is a prefix", body=function() {
				write( "a#sep#1", "a1" );
				write( "a#sep#2", "a2" );
				write( "b#sep#1", "b1" );

				var rows = mine( s3ListBucket( bucketName=ctx.bucket ) );
				expect( rows ).toHaveLength( 3 );
				for ( var r in rows ) expect( r.isPrefix ).toBeFalse();
				expect( names( rows ) ).toBe( [ ctx.prefix & "a#sep#1", ctx.prefix & "a#sep#2", ctx.prefix & "b#sep#1" ] );
			});

			it( title="delimiter returns the common prefixes as rows with isPrefix=true", body=function() {
				write( "a#sep#1", "a1" );
				write( "a#sep#2", "a2" );
				write( "b#sep#1", "b1" );
				write( "top.txt", "top" );

				var rows = mine( s3ListBucket( bucketName=ctx.bucket, delimiter=sep ) );
				// a/ and b/ (grouped) and top.txt
				expect( names( rows ) ).toBe( [ ctx.prefix & "a#sep#", ctx.prefix & "b#sep#", ctx.prefix & "top.txt" ] );
				expect( rows[ 1 ].isPrefix ).toBeTrue();
				expect( rows[ 2 ].isPrefix ).toBeTrue();
				expect( rows[ 3 ].isPrefix ).toBeFalse();
				expect( rows[ 3 ].size ).toBe( 3 );
				expect( len( rows[ 3 ].eTag ) ).toBeGT( 0 );
			});

			it( title="delimiter works over several pages (blockfactor)", body=function() {
				for ( var i = 1; i <= 4; i++ ) {
					write( "d#i##sep#1", "x" );
					write( "d#i##sep#2", "x" );
				}
				write( "top1.txt", "x" );
				write( "top2.txt", "x" );

				var expected = [];
				for ( var i = 1; i <= 4; i++ ) expected.append( ctx.prefix & "d#i##sep#" );
				expected.append( ctx.prefix & "top1.txt" );
				expected.append( ctx.prefix & "top2.txt" );

				var rows = mine( s3ListBucket( bucketName=ctx.bucket, delimiter=sep, blockfactor=2 ) );
				expect( names( rows ) ).toBe( expected );
				rows = mine( s3ListBucket( bucketName=ctx.bucket, delimiter=sep, blockfactor=1 ) );
				expect( names( rows ) ).toBe( expected );
			});

			it( title="maxResults limits the number of rows", body=function() {
				write( "a#sep#1", "a1" );
				write( "a#sep#2", "a2" );
				write( "b#sep#1", "b1" );

				expect( s3ListBucket( bucketName=ctx.bucket, maxResults=1 ).recordCount ).toBe( 1 );
				// it is not the size of a page: with pages of 1 key, 2 rows are still 2 rows
				expect( s3ListBucket( bucketName=ctx.bucket, maxResults=2, blockfactor=1 ).recordCount ).toBe( 2 );
				// more than there is
				expect( mine( s3ListBucket( bucketName=ctx.bucket, maxResults=1000 ) ) ).toHaveLength( 3 );

				// starting at the objects of the test, so the result is the same with a shared bucket
				var rows = s3ListBucket( bucketName=ctx.bucket, startAfter=ctx.prefix & "a#sep#1", maxResults=1 );
				expect( rows.recordCount ).toBe( 1 );
				expect( rows.objectName[ 1 ] ).toBe( ctx.prefix & "a#sep#2" );

				// prefixes count as rows
				rows = s3ListBucket( bucketName=ctx.bucket, delimiter=sep, startAfter=( len( ctx.prefix ) ? left( ctx.prefix, len( ctx.prefix ) - 1 ) : "" ), maxResults=1 );
				expect( rows.recordCount ).toBe( 1 );
			});

			it( title="maxResults of 0 lists everything, a negative value is an error", body=function() {
				write( "a#sep#1", "a1" );
				write( "a#sep#2", "a2" );
				expect( mine( s3ListBucket( bucketName=ctx.bucket, maxResults=0 ) ) ).toHaveLength( 2 );
				expect( function() {
					s3ListBucket( bucketName=ctx.bucket, maxResults=-1 );
				}).toThrow( message="maxResults" );
			});

			it( title="startAfter lists the keys after this key", body=function() {
				write( "a#sep#1", "a1" );
				write( "a#sep#2", "a2" );
				write( "b#sep#1", "b1" );

				var rows = mine( s3ListBucket( bucketName=ctx.bucket, startAfter=ctx.prefix & "a#sep#1" ) );
				expect( names( rows ) ).toBe( [ ctx.prefix & "a#sep#2", ctx.prefix & "b#sep#1" ] );
				// the key itself is not part of the result, also not if it does not exist
				rows = mine( s3ListBucket( bucketName=ctx.bucket, startAfter=ctx.prefix & "a#sep#15" ) );
				expect( names( rows ) ).toBe( [ ctx.prefix & "a#sep#2", ctx.prefix & "b#sep#1" ] );
				// behind the last key
				expect( mine( s3ListBucket( bucketName=ctx.bucket, startAfter=ctx.prefix & "b#sep#1" ) ) ).toHaveLength( 0 );
			});

			it( title="startAfter and maxResults resume a listing", body=function() {
				for ( var i = 1; i <= 5; i++ ) write( "k#i#", "x" );

				var found = [];
				var last = ctx.prefix & "k0";
				for ( var round = 1; round <= 5; round++ ) {
					var rows = s3ListBucket( bucketName=ctx.bucket, startAfter=last, maxResults=2 );
					if ( !rows.recordCount ) break;
					for ( var r = 1; r <= rows.recordCount; r++ ) found.append( rows.objectName[ r ] );
					last = rows.objectName[ rows.recordCount ];
					if ( len( found ) >= 5 ) break;
				}
				expect( found ).toBe( [ ctx.prefix & "k1", ctx.prefix & "k2", ctx.prefix & "k3", ctx.prefix & "k4", ctx.prefix & "k5" ] );
			});

			it( title="the arguments work with a listener, and maxResults stops the paging", body=function() {
				for ( var i = 1; i <= 4; i++ ) write( "l#i#", "x" );

				var calls = 0;
				var total = 0;
				var seenColumns = "";
				s3ListBucket( bucketName=ctx.bucket, startAfter=ctx.prefix & "l0", maxResults=3, blockfactor=2, listener=function( qry ) {
					calls++;
					total += qry.recordCount;
					seenColumns = qry.columnList;
					return true;
				});
				// 3 rows in pages of 2 keys
				expect( total ).toBe( 3 );
				expect( calls ).toBe( 2 );
				expect( listFindNoCase( seenColumns, "eTag" ) ).toBeGT( 0 );
				expect( listFindNoCase( seenColumns, "isPrefix" ) ).toBeGT( 0 );
			});

			it( title="empty values are the same as not set", body=function() {
				write( "a#sep#1", "a1" );
				write( "b#sep#1", "b1" );
				var rows = mine( s3ListBucket( bucketName=ctx.bucket, delimiter="", startAfter="", maxResults=0 ) );
				expect( rows ).toHaveLength( 2 );
				expect( rows[ 1 ].isPrefix ).toBeFalse();
			});

		});
	}

	private function write( required string name, required string value ) {
		s3Write( bucketName=ctx.bucket, objectName=CustomS3::key( ctx, name ), value=value );
	}

	/** the rows of the test (a shared bucket holds more), as an array of structs */
	private array function mine( required query qry ) {
		var rows = [];
		for ( var r = 1; r <= qry.recordCount; r++ ) {
			if ( !len( ctx.prefix ) || left( qry.objectName[ r ], len( ctx.prefix ) ) == ctx.prefix ) {
				rows.append( {
					objectName: qry.objectName[ r ],
					size: qry.size[ r ],
					isPrefix: qry.isPrefix[ r ],
					eTag: qry.eTag[ r ],
					storageClass: qry.storageClass[ r ]
				} );
			}
		}
		return rows;
	}

	private array function names( required array rows ) {
		return arrayMap( rows, function( r ) {
			return r.objectName;
		} );
	}
}
