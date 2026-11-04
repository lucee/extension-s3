/**
 * LDEV-6854: specs for the timeout settings and the connection pool.
 * https://luceeserver.atlassian.net/browse/LDEV-6854
 *
 * - the "pool" settings (maxConnections, connectionTimeout, socketTimeout, connectionMaxIdleMillis) of the s3
 *   application settings end up in the HTTP client configuration (no S3 server needed)
 * - socketTimeout/connectionTimeout make a request fail in time when the endpoint accepts the connection but never
 *   answers (a local socket, no network needed)
 * - the "timeout" argument of the functions (lifetime of the client in ms) does not change the result
 *   (needs the self hosted S3 server of the tests, MinIO in the CI)
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6854: pool settings", body=function() {

			it( title="defaults: 128 connections, no timeouts set", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b" } );
				expect( config.getMaxConnections() ).toBe( 128 );
				// the defaults of the AWS SDK
				expect( config.getConnectionTimeout() ).toBe( 10000 );
				expect( config.getSocketTimeout() ).toBe( 50000 );
			});

			it( title="pool settings are applied to the client configuration", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { maxConnections: 7, connectionTimeout: 1234, socketTimeout: 2345, connectionMaxIdleMillis: 3456 } } );
				expect( config.getMaxConnections() ).toBe( 7 );
				expect( config.getConnectionTimeout() ).toBe( 1234 );
				expect( config.getSocketTimeout() ).toBe( 2345 );
				expect( config.getConnectionMaxIdleMillis() ).toBe( 3456 );
			});

			it( title="numbers as strings are accepted", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { maxConnections: "9", socketTimeout: "1500" } } );
				expect( config.getMaxConnections() ).toBe( 9 );
				expect( config.getSocketTimeout() ).toBe( 1500 );
			});

			it( title="a setting that is not set keeps its default", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { socketTimeout: 1500 } } );
				expect( config.getMaxConnections() ).toBe( 128 );
				expect( config.getConnectionTimeout() ).toBe( 10000 );
				expect( config.getSocketTimeout() ).toBe( 1500 );
			});

		});

		describe( title="LDEV-6854: timeouts of an endpoint that does not answer", body=function() {

			it( title="socketTimeout of this.s3.pool: the request fails in time", body=function() {
				// the socket accepts the connection (the OS does) but nobody reads or answers
				var sock = createObject( "java", "java.net.ServerSocket" ).init( 0, 50, createObject( "java", "java.net.InetAddress" ).getByName( "127.0.0.1" ) );
				try {
					var res = call( sock.getLocalPort(), { socketTimeout: 300, connectionTimeout: 300 } );
					expect( res.failed ).toBeTrue( "a request to an endpoint that does not answer must fail" );
					// without the setting every try takes 50 seconds (socket timeout of the AWS SDK), with it 300ms
					expect( res.duration ).toBeLT( 20000, "the request took #res.duration# ms, the socketTimeout of 300 ms was not used" );
				}
				finally {
					sock.close();
				}
			});

		});

		describe( title="LDEV-6854: exhausted connection pool", body=function() {

			it( title="a request that cannot get a connection fails after connectionTimeout and is not 'does not exist'", body=function() {
				// the socket accepts the connections (the OS does) but nobody answers, so the only connection of the pool is busy
				// until the socketTimeout (1 second per try, 4 tries) is over
				var sock = createObject( "java", "java.net.ServerSocket" ).init( 0, 50, createObject( "java", "java.net.InetAddress" ).getByName( "127.0.0.1" ) );
				try {
					var res = internalRequest( template: baseURI() & "parallel.cfm", url: {
						port: sock.getLocalPort(), pool: serializeJSON( { maxConnections: 1, connectionTimeout: 200, socketTimeout: 1000 } ), bucket: "ldev6854-bucket", key: "some/file.txt", count: 3 } );
					expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
					var results = deserializeJSON( res.filecontent );
					var poolTimeouts = 0;
					for ( var r in results ) {
						expect( r ).notToBe( "false", "a failed request was reported as 'does not exist'" );
						expect( left( r, 8 ) ).notToBe( "missing:", r );
						if ( findNoCase( "connection from pool", r ) ) poolTimeouts++;
					}
					expect( poolTimeouts ).toBeGT( 0, "no request ran into the connectionTimeout of the pool: " & serializeJSON( results ) );
				}
				finally {
					sock.close();
				}
			});

		});

		describe( title="LDEV-6854: timeout argument of the functions", skip=!CustomS3::isSupported(), body=function() {

			beforeEach( function() {
				ctx = CustomS3::prepare( "6854" );
				obj = CustomS3::key( ctx, "timeout.txt" );
			});

			afterEach( function() {
				if ( !isNull( ctx ) ) CustomS3::cleanup( ctx );
			});

			it( title="the functions return the same with a timeout of 1 ms (client is created for every call) and of 1 minute", body=function() {
				for ( var timeout in [ 1, 60000 ] ) {
					s3Write( bucketName=ctx.bucket, objectName=obj, value="content #timeout#", timeout=timeout );
					expect( s3Exists( bucketName=ctx.bucket, objectName=obj, timeout=timeout ) ).toBeTrue( "timeout #timeout#" );
					expect( s3Read( bucketName=ctx.bucket, objectName=obj, timeout=timeout ) ).toBe( "content #timeout#" );
					expect( arrayLen( s3ReadBinary( bucketName=ctx.bucket, objectName=obj, timeout=timeout ) ) ).toBe( len( "content #timeout#" ) );
					var meta = s3GetMetaData( bucketName=ctx.bucket, objectName=obj, timeout=timeout );
					expect( meta ).toHaveKey( "contentLength" );
					expect( s3ListBucket( bucketName=ctx.bucket, timeout=timeout ).recordCount ).toBeGTE( 1 );
					s3Delete( bucketName=ctx.bucket, objectName=obj, timeout=timeout );
					expect( s3Exists( bucketName=ctx.bucket, objectName=obj, timeout=timeout ) ).toBeFalse( "timeout #timeout#" );
				}
			});

			it( title="saturated pool (1 connection, 1 ms timeout): parallel exists() never answers false for an existing object", body=function() {
				s3Write( bucketName=ctx.bucket, objectName=obj, value="LDEV-6854 exists must not lie" );
				// pool settings are only passed on by Application.cfc (this.s3), not by "application action=update"
				var pool = { maxConnections: 1, connectionTimeout: 1 };
				var results = parallel( ctx, obj, 100, pool );
				for ( var r in results ) {
					expect( r ).notToBe( "false", "an existing object was reported as missing" );
					expect( left( r, 8 ) ).notToBe( "missing:", r );
				}
				// a normal pool: the object is found, so no "not existing" was cached by the failed calls
				results = parallel( ctx, obj, 10, { maxConnections: 50, connectionTimeout: 10000 } );
				for ( var r in results ) expect( r ).toBe( "true" );
			});

		});
	}

	private struct function call( required numeric port, required struct pool ) {
		var res = internalRequest( template: baseURI() & "index.cfm", url: { port: arguments.port, pool: serializeJSON( arguments.pool ) } );
		expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
		return deserializeJSON( res.filecontent );
	}

	private array function parallel( required struct ctx, required string key, required numeric count, required struct pool ) {
		var res = internalRequest( template: baseURI() & "parallel.cfm", url: { custom: true, pool: serializeJSON( arguments.pool ), bucket: arguments.ctx.bucket, key: arguments.key, count: arguments.count } );
		expect( isJSON( res.filecontent ) ).toBe( true, "no JSON returned: " & res.filecontent );
		return deserializeJSON( res.filecontent );
	}

	private string function baseURI() {
		// the CI maps the tests of the extension to /testAdditional, locally they can be mapped to /test
		var folder = "tickets/LDEV6854/";
		if ( directoryExists( expandPath( "/testAdditional/" & folder ) ) ) return "/testAdditional/" & folder;
		return "/test/" & folder;
	}

	/**
	 * loads the s3 settings like the extension does for an application and applies the pool settings to a
	 * com.amazonaws.ClientConfiguration, the classes of the extension are loaded by the class loader of its resource provider
	 */
	private function clientConfig( required struct settings ) {
		var cl = "";
		loop array=getPageContext().getConfig().getResourceProviders() item="local.provider" {
			if ( provider.getScheme() == "s3" ) {
				cl = provider.getClass().getClassLoader();
				break;
			}
		}
		expect( isSimpleValue( cl ) ).toBeFalse( "the resource provider for s3 is not available" );
		var cls = cl.loadClass( "org.lucee.extension.resource.s3.S3Properties" );
		var types = javaCast( "java.lang.Class[]", [ cl.loadClass( "lucee.runtime.PageContext" ), cl.loadClass( "lucee.runtime.type.Struct" ), cl.loadClass( "java.lang.String" ) ] );
		var props = cls.getMethod( "load", types ).invoke( javaCast( "null", "" ), javaCast( "java.lang.Object[]", [ getPageContext(), { s3: arguments.settings }, "" ] ) );
		var configClass = cl.loadClass( "com.amazonaws.ClientConfiguration" );
		var config = configClass.getConstructor( javaCast( "java.lang.Class[]", [] ) ).newInstance( javaCast( "java.lang.Object[]", [] ) );
		var pool = props.getHttpPool();
		pool.getClass().getMethod( "apply", javaCast( "java.lang.Class[]", [ configClass ] ) ).invoke( pool, javaCast( "java.lang.Object[]", [ config ] ) );
		return config;
	}
}
