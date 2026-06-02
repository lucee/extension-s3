/**
 * Regression tests for LDEV-6373 (S3 HTTP connection pool exhaustion).
 * https://luceeserver.atlassian.net/browse/LDEV-6373
 *
 * Verifies parallel s3Download/s3Read and VFS reads do not leave connections leased
 * (ConnectionPoolTimeoutException under ~50 concurrent uses per JVM).
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	variables.parallelCount = 40;
	variables.objectName = "ldev-6373-pool-test.txt";
	variables.objectContent = "LDEV-6373 connection pool regression";

	function run( testResults , testBox ) {
		describe(
			title="LDEV-6373: parallel direct S3 functions",
			skip=Util::isAWSNotSupported(),
			body=function() {

				aroundEach( function( spec, suite ) {
					var bucketName = Util::createBucketName( "ldev6373" );
					var cred = Util::getAWSCredentials();
					try {
						S3Write(
							value=objectContent,
							bucketName=bucketName,
							objectName=objectName,
							accessKeyId=cred.ACCESS_KEY_ID,
							secretAccessKey=cred.SECRET_KEY,
							host=( isNull( cred.HOST ) ? nullValue() : cred.HOST )
						);
						spec.body( {
							bucketName: bucketName,
							cred: cred,
							parallelCount: parallelCount,
							objectName: objectName,
							objectContent: objectContent
						} );
					}
					finally {
						Util::deleteBucketEL( cred, bucketName );
					}
				} );

				it( title="parallel s3Download to temp files", body=function( currentSpec ) {
					runParallel( currentSpec, "download" );
				} );

				it( title="two waves of parallel s3Download reuse the connection pool", body=function( currentSpec ) {
					runParallel( currentSpec, "download" );
					runParallel( currentSpec, "download" );
				} );

				it( title="parallel s3Read", body=function( currentSpec ) {
					runParallel( currentSpec, "read" );
				} );

				it( title="parallel s3ReadBinary", body=function( currentSpec ) {
					runParallel( currentSpec, "readBinary" );
				} );
			}
		);

		describe(
			title="LDEV-6373: parallel VFS fileRead (S3ObjectInputStream)",
			skip=Util::isAWSNotSupported(),
			body=function() {

				aroundEach( function( spec, suite ) {
					var bucketName = Util::createBucketName( "ldev6373vfs" );
					var cred = Util::getAWSCredentials();
					setupVfs( cred );
					try {
						S3Write(
							value=objectContent,
							bucketName=bucketName,
							objectName=objectName,
							accessKeyId=cred.ACCESS_KEY_ID,
							secretAccessKey=cred.SECRET_KEY,
							host=( isNull( cred.HOST ) ? nullValue() : cred.HOST )
						);
						spec.body( {
							bucketName: bucketName,
							cred: cred,
							s3Path: "s3:///" & bucketName & "/" & objectName,
							parallelCount: parallelCount,
							objectContent: objectContent
						} );
					}
					finally {
						Util::deleteBucketEL( cred, bucketName );
					}
				} );

				it( title="parallel fileRead on s3:// path", body=function( currentSpec ) {
					runParallelVfs( currentSpec );
				} );

				it( title="two waves of parallel VFS fileRead", body=function( currentSpec ) {
					runParallelVfs( currentSpec );
					runParallelVfs( currentSpec );
				} );
			}
		);
	}

	private void function setupVfs( required struct cred ) {
		application action="update" s3={
			accessKeyId: cred.ACCESS_KEY_ID,
			awsSecretKey: cred.SECRET_KEY,
			host: ( isNull( cred.HOST ) ? nullValue() : cred.HOST )
		};
	}

	private boolean function isConnectionPoolTimeout( required any e ) {
		var msg = ( e.message ?: "" ) & ( e.detail ?: "" ) & ( e.stackTrace ?: "" );
		if ( structKeyExists( e, "cause" ) && !isNull( e.cause ) ) {
			msg &= ( e.cause.message ?: "" ) & ( e.cause.stackTrace ?: "" );
		}
		return findNoCase( "Timeout waiting for connection from pool", msg )
			|| findNoCase( "ConnectionPoolTimeoutException", msg );
	}

	private void function runParallel( required struct spec, required string mode ) {
		var cred = spec.cred;
		var names = [];
		var exceptions = [];
		var tempDir = getTempDirectory() & "ldev6373-" & createUUID() & "/";
		directoryCreate( tempDir );

		try {
			for ( var i = 1; i <= spec.parallelCount; i++ ) {
				var threadName = "ldev6373-#mode#-#i#";
				arrayAppend( names, threadName );
				thread action="run" name=threadName mode=mode spec=spec cred=cred tempDir=tempDir exceptions=exceptions {
					try {
						if ( mode == "download" ) {
							var target = tempDir & "dl-" & createUUID() & ".txt";
							s3Download(
								bucketName=spec.bucketName,
								objectName=spec.objectName,
								target=target,
								accessKeyId=cred.ACCESS_KEY_ID,
								secretAccessKey=cred.SECRET_KEY,
								host=( isNull( cred.HOST ) ? nullValue() : cred.HOST )
							);
							assertEquals( fileRead( target ), spec.objectContent );
							if ( fileExists( target ) ) fileDelete( target );
						}
						else if ( mode == "read" ) {
							var data = s3Read(
								bucketName=spec.bucketName,
								objectName=spec.objectName,
								accessKeyId=cred.ACCESS_KEY_ID,
								secretAccessKey=cred.SECRET_KEY,
								host=( isNull( cred.HOST ) ? nullValue() : cred.HOST )
							);
							assertEquals( data, spec.objectContent );
						}
						else if ( mode == "readBinary" ) {
							var bin = s3ReadBinary(
								bucketName=spec.bucketName,
								objectName=spec.objectName,
								accessKeyId=cred.ACCESS_KEY_ID,
								secretAccessKey=cred.SECRET_KEY,
								host=( isNull( cred.HOST ) ? nullValue() : cred.HOST )
							);
							assertEquals( toString( bin ), spec.objectContent );
						}
					}
					catch ( any err ) {
						arrayAppend( exceptions, err );
					}
				}
			}

			thread action="join" name=arrayToList( names );

			if ( arrayLen( exceptions ) ) {
				if ( isConnectionPoolTimeout( exceptions[ 1 ] ) ) {
					throw "LDEV-6373: connection pool timeout during parallel #mode# [#spec.parallelCount# threads]. #exceptions[ 1 ].message#";
				}
				throw exceptions[ 1 ];
			}
		}
		finally {
			if ( directoryExists( tempDir ) ) {
				directoryDelete( tempDir, true );
			}
		}
	}

	private void function runParallelVfs( required struct spec ) {
		var names = [];
		var exceptions = [];

		for ( var i = 1; i <= spec.parallelCount; i++ ) {
			var threadName = "ldev6373-vfs-#i#";
			arrayAppend( names, threadName );
			thread action="run" name=threadName s3Path=spec.s3Path objectContent=spec.objectContent exceptions=exceptions {
				try {
					assertEquals( fileRead( s3Path ), objectContent );
				}
				catch ( any err ) {
					arrayAppend( exceptions, err );
				}
			}
		}

		thread action="join" name=arrayToList( names );

		if ( arrayLen( exceptions ) ) {
			if ( isConnectionPoolTimeout( exceptions[ 1 ] ) ) {
				throw "LDEV-6373: connection pool timeout during parallel VFS fileRead [#spec.parallelCount# threads]. #exceptions[ 1 ].message#";
			}
			throw exceptions[ 1 ];
		}
	}

}
