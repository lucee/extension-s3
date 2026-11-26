/**
 * LDEV-7004: an expired cached client is shut down while other threads are still using it.
 * https://luceeserver.atlassian.net/browse/LDEV-7004
 *
 * Works on org.lucee.extension.resource.s3.AmazonS3Client directly (loaded by the class loader of the s3 resource
 * provider) against local sockets, no S3 server needed:
 * - an expired client that another thread is in the middle of an operation with is not shut down by get()
 * - a retired client is shut down once its connection pool is idle
 * - threads that hit a shut down client at the same time replace it once and never shut down the replacement
 * - a SdkClientException caused by a shut down pool is retried like an IllegalStateException
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-7004: expired client", body=function() {

			it( title="get() does not shut down an expired client that another thread is still using", body=function() {
				// a local HTTP endpoint that sends the head and the first half of the body, then waits until it is released
				// before it sends the rest: the request in the thread keeps its connection leased while it reads the body
				variables.sock = createObject( "java", "java.net.ServerSocket" ).init( 0, 50, createObject( "java", "java.net.InetAddress" ).getByName( "127.0.0.1" ) );
				variables.released = false;
				var serverName = "ldev7004-server-" & createUUID();
				var threadName = "ldev7004-midop-" & createUUID();
				try {
					thread action="run" name=serverName {
						var s = variables.sock.accept();
						try {
							var reader = createObject( "java", "java.io.BufferedReader" ).init( createObject( "java", "java.io.InputStreamReader" ).init( s.getInputStream(), "UTF-8" ) );
							while ( true ) {
								var line = reader.readLine();
								if ( isNull( line ) || !len( line ) ) break;
							}
							var out = s.getOutputStream();
							var crlf = chr( 13 ) & chr( 10 );
							out.write( charsetDecode( "HTTP/1.1 200 OK" & crlf & "Content-Type: text/plain" & crlf & "Content-Length: 10" & crlf & "Connection: close" & crlf & crlf & "01234", "UTF-8" ) );
							out.flush();
							var deadline = getTickCount() + 10000;
							while ( !variables.released && getTickCount() < deadline ) sleep( 20 );
							out.write( charsetDecode( "56789", "UTF-8" ) );
							out.flush();
						}
						catch ( any e ) {
							// without the fix the client closed the connection already, the request in the thread shows it
							thread.error = e.message;
						}
						finally {
							s.close();
						}
					}

					var host = "http://127.0.0.1:" & variables.sock.getLocalPort();
					var key = "ldev7004-" & createUUID();
					// liveTimeout of 1 ms: the client is expired for the next get()
					variables.expired = getClient( key, host, 1, { socketTimeout: 5000, connectionTimeout: 500 } );
					var inner = innerClient( variables.expired );

					// Lucee 6.0/6.1 did not expose a value set on the thread scope after join when it was built
					// from a Java byte array, so the body is also stored on variables (already shared with the thread)
					variables.midOpBody = "";
					thread action="run" name=threadName {
						try {
							var obj = variables.expired.getObject( "ldev7004-bucket", "some/file.txt" );
							var content = obj.getObjectContent();
							try {
								var bytes = content.readAllBytes();
								var body = charsetEncode( bytes, "UTF-8" );
								if ( !len( body ) ) body = createObject( "java", "java.lang.String" ).init( bytes, "UTF-8" ).toString();
								variables.midOpBody = body;
								thread.readBody = body;
								thread.byteLen = arrayLen( bytes );
							}
							finally {
								content.close();
							}
						}
						catch ( any e ) {
							variables.midOpBody = fullMessage( e );
							thread.readBody = variables.midOpBody;
						}
					}
					// wait until the request in the thread has leased its connection (it then waits for the rest of the body)
					waitForBusyConnection( inner, 5000 );
					var replacement = getClient( key, host, 1, { socketTimeout: 5000, connectionTimeout: 500 } );

					// state of the expired client while the request in the thread is still running; these helpers use
					// internals that only exist with the fix, so their failure is checked after the public assertion
					var internals = { error: "" };
					try {
						// the request in the thread still holds its connection: get() did not shut the pool down under it
						internals.busy = busyConnections( inner );
						// a sweep long after the expiry still keeps the client, it has a leased connection
						shutdownRetired( nowMillis() + 20000 );
					}
					catch ( any e ) {
						internals.error = e.message;
					}

					variables.released = true;
					joinThreads( [ threadName, serverName ], 30000 );
					var th = cfthread[ threadName ];
					var res = len( variables.midOpBody ?: "" ) ? variables.midOpBody : ( th.readBody ?: "" );
					// public API only: the request in the thread reads the whole object, get() did not shut its pool down
					expect( res ).notToInclude( "pool shut down", "the request in the thread failed because the expired client was shut down: " & res );
					expect( res ).toBe( "0123456789", "the request in the thread failed (status #th.status#, thread.readBody '#th.readBody ?: ""#', byteLen #th.byteLen ?: -1#): " & res );

					// nothing listens anymore, a request with the expired client fails fast, but not with a shut down pool
					variables.sock.close();
					internals.shutdown = isShutdown( inner );

					expect( replacement.equals( variables.expired ) ).toBeFalse( "get() did not replace the expired client" );
					expect( internals.error ).toBe( "" );
					expect( internals.busy ).toBeGTE( 1, "the expired client has no leased connection anymore, was it shut down by get()?" );
					expect( internals.shutdown ).toBeFalse( "the expired client was shut down while a thread was still using it" );
				}
				finally {
					variables.released = true;
					variables.sock.close();
				}
			});

			it( title="a retired client is shut down once it is idle", body=function() {
				var key = "ldev7004-" & createUUID();
				var host = "http://127.0.0.1:1";
				var first = getClient( key, host, 1, {} );
				var inner = innerClient( first );
				sleep( 5 );
				var second = getClient( key, host, 1, {} );
				expect( second.equals( first ) ).toBeFalse();
				// right after the expiry it is not shut down, even if idle
				shutdownRetired( nowMillis() );
				expect( isShutdown( inner ) ).toBeFalse( "the retired client was shut down right away" );
				expect( retiredCount( inner ) ).toBe( 1 );
				// later, idle: shut down and removed from the retired clients
				shutdownRetired( nowMillis() + 20000 );
				expect( isShutdown( inner ) ).toBeTrue( "the idle retired client was not shut down" );
				expect( retiredCount( inner ) ).toBe( 0 );
			});

		});

		describe( title="LDEV-7004: invalidate a shut down client", body=function() {

			it( title="parallel requests on a shut down client replace it once and do not shut down the replacement", body=function() {
				var key = "ldev7004-" & createUUID();
				// nothing listens on port 1, every request fails fast with "connection refused"
				variables.shared = getClient( key, "http://127.0.0.1:1", 0, {} );
				var failed = innerClient( variables.shared );
				failed.shutdown();

				var names = [];
				for ( var i = 1; i <= 16; i++ ) {
					var threadName = "ldev7004-inv-#i#-" & createUUID();
					arrayAppend( names, threadName );
					thread action="run" name=threadName {
						try {
							variables.shared.getObjectMetadata( "ldev7004-bucket", "some/file.txt" );
							thread.readBody = "ok";
						}
						catch ( any e ) {
							thread.readBody = fullMessage( e );
						}
					}
				}
				joinThreads( names, 60000 );

				for ( var threadName in names ) {
					var res = cfthread[ threadName ].readBody ?: "";
					expect( res ).notToInclude( "pool shut down", "a request failed with the shut down client: " & res );
				}
				var current = innerClient( variables.shared );
				expect( current.equals( failed ) ).toBeFalse( "the shut down client was not replaced" );
				expect( isShutdown( current ) ).toBeFalse( "the replacement client was shut down" );
				// replaced once: the failed client is retired once, the replacement is not retired
				expect( retiredCount( failed ) ).toBe( 1 );
				expect( retiredCount( current ) ).toBe( 0 );
			});

			it( title="a SdkClientException caused by a shut down pool is retried, other failures are not", body=function() {
				var cl = providerClassLoader();
				var sdkEx = cl.loadClass( "com.amazonaws.SdkClientException" );
				// RequestAbortedException is loaded by the class loader of HttpClient (the Apache client inside the AWS SDK client)
				var inner = innerClient( getClient( "ldev7004-" & createUUID(), "http://127.0.0.1:1", 0, {} ) );
				var apacheClient = fieldValue( fieldValue( fieldValue( inner, "client" ), "httpClient" ), "delegate" );
				var aborted = apacheClient.getClass().getClassLoader().loadClass( "org.apache.http.impl.execchain.RequestAbortedException" );
				var ise = createObject( "java", "java.lang.IllegalStateException" ).init( "Connection pool shut down" );
				var stringThrowable = javaCast( "java.lang.Class[]", [ cl.loadClass( "java.lang.String" ), cl.loadClass( "java.lang.Throwable" ) ] );

				var abortedEx = aborted.getConstructor( stringThrowable ).newInstance( javaCast( "java.lang.Object[]", [ "Request execution failed", ise ] ) );
				var wrapped = sdkEx.getConstructor( stringThrowable ).newInstance( javaCast( "java.lang.Object[]", [ "Unable to execute HTTP request: Request execution failed", abortedEx ] ) );
				expect( isPoolShutdown( wrapped ) ).toBeTrue( "RequestAbortedException 'Request execution failed'" );

				var wrappedIse = sdkEx.getConstructor( stringThrowable ).newInstance( javaCast( "java.lang.Object[]", [ "Unable to execute HTTP request: Connection pool shut down", ise ] ) );
				expect( isPoolShutdown( wrappedIse ) ).toBeTrue( "Connection pool shut down" );
				expect( isPoolShutdown( ise ) ).toBeTrue( "IllegalStateException" );

				var timeout = createObject( "java", "java.net.SocketTimeoutException" ).init( "Read timed out" );
				var wrappedTimeout = sdkEx.getConstructor( stringThrowable ).newInstance( javaCast( "java.lang.Object[]", [ "Unable to execute HTTP request: Read timed out", timeout ] ) );
				expect( isPoolShutdown( wrappedTimeout ) ).toBeFalse( "read timeout" );
			});

		});
	}

	/** the class loader of the s3 resource provider, it loads the classes of the extension */
	private function providerClassLoader() {
		loop array=getPageContext().getConfig().getResourceProviders() item="local.provider" {
			if ( provider.getScheme() == "s3" ) return provider.getClass().getClassLoader();
		}
		throw "the resource provider for s3 is not available";
	}

	private function clientClass() {
		return providerClassLoader().loadClass( "org.lucee.extension.resource.s3.AmazonS3Client" );
	}

	/** AmazonS3Client.get() with the given pool settings (loaded like the application settings do) */
	private function getClient( required string key, required string host, required numeric liveTimeout, required struct pool ) {
		var cl = providerClassLoader();
		var propsClass = cl.loadClass( "org.lucee.extension.resource.s3.S3Properties" );
		var types = javaCast( "java.lang.Class[]", [ cl.loadClass( "lucee.runtime.PageContext" ), cl.loadClass( "lucee.runtime.type.Struct" ), cl.loadClass( "java.lang.String" ) ] );
		var props = propsClass.getMethod( "load", types ).invoke( javaCast( "null", "" ), javaCast( "java.lang.Object[]", [ getPageContext(), { s3: { accessKeyId: "a", awsSecretKey: "b", pool: arguments.pool } }, "" ] ) );
		var httpPool = props.getHttpPool();

		var regionClass = cl.loadClass( "org.lucee.extension.resource.s3.region.RegionFactory$Region" );
		var region = cl.loadClass( "org.lucee.extension.resource.s3.region.RegionFactory" ).getField( "US_EAST_1" ).get( javaCast( "null", "" ) );
		var logClass = cl.loadClass( "lucee.commons.io.log.Log" );
		var strClass = cl.loadClass( "java.lang.String" );
		var get = clientClass().getMethod( "get", javaCast( "java.lang.Class[]", [ strClass, strClass, strClass, regionClass,
			createObject( "java", "java.lang.Long" ).TYPE, createObject( "java", "java.lang.Boolean" ).TYPE, httpPool.getClass(), logClass ] ) );
		return get.invoke( javaCast( "null", "" ), javaCast( "java.lang.Object[]", [ arguments.key, "secret", arguments.host, region,
			javaCast( "long", arguments.liveTimeout ), javaCast( "boolean", true ), httpPool, getPageContext().getConfig().getLog( "application" ) ] ) );
	}

	/** the inner com.amazonaws.services.s3.AmazonS3 of the wrapper */
	private function innerClient( required wrapper ) {
		var f = clientClass().getDeclaredField( "client" );
		f.setAccessible( true );
		return f.get( arguments.wrapper );
	}

	/** joins the threads, fails if one of them did not finish within the timeout (milliseconds, for all threads together) */
	private function joinThreads( required array names, required numeric timeout ) {
		var deadline = getTickCount() + arguments.timeout;
		for ( var name in arguments.names ) {
			thread action="join" name=name timeout=max( 1, deadline - getTickCount() );
			var status = cfthread[ name ].status;
			expect( listFindNoCase( "COMPLETED,TERMINATED", status ) ).toBeGT( 0, "thread [#name#] did not finish within #arguments.timeout# ms (status #status#)" );
		}
	}

	/**
	 * waits until the connection pool of the inner client has a leased connection (or a thread waiting for one), at
	 * most timeout milliseconds; without the fix busyConnections() does not exist, it then just waits 500 ms
	 */
	private function waitForBusyConnection( required inner, required numeric timeout ) {
		var deadline = getTickCount() + arguments.timeout;
		try {
			while ( busyConnections( arguments.inner ) < 1 ) {
				if ( getTickCount() > deadline ) throw "the request in the thread did not lease a connection within #arguments.timeout# ms";
				sleep( 20 );
			}
		}
		catch ( any e ) {
			if ( !findNoCase( "NoSuchMethod", ( e.type ?: "" ) & " " & ( e.message ?: "" ) & " " & ( e.stacktrace ?: "" ) ) ) rethrow;
			sleep( 500 );
		}
	}

	private numeric function busyConnections( required inner ) {
		return invokeStatic( "busyConnections", [ providerClassLoader().loadClass( "com.amazonaws.services.s3.AmazonS3" ) ], [ arguments.inner ] );
	}

	private boolean function isPoolShutdown( required ex ) {
		return invokeStatic( "isPoolShutdown", [ providerClassLoader().loadClass( "java.lang.Throwable" ) ], [ arguments.ex ] );
	}

	private numeric function shutdownRetired( required numeric atMillis ) {
		return invokeStatic( "shutdownRetired", [ createObject( "java", "java.lang.Long" ).TYPE, createObject( "java", "java.lang.Boolean" ).TYPE ],
			[ javaCast( "long", arguments.atMillis ), javaCast( "boolean", true ) ] );
	}

	private function invokeStatic( required string name, required array types, required array args ) {
		var m = clientClass().getDeclaredMethod( arguments.name, javaCast( "java.lang.Class[]", arguments.types ) );
		m.setAccessible( true );
		return m.invoke( javaCast( "null", "" ), javaCast( "java.lang.Object[]", arguments.args ) );
	}

	/** how often the given inner client is in the list of the retired clients */
	private numeric function retiredCount( required inner ) {
		var f = clientClass().getDeclaredField( "retired" );
		f.setAccessible( true );
		var count = 0;
		var it = f.get( javaCast( "null", "" ) ).iterator();
		while ( it.hasNext() ) {
			var r = it.next();
			var c = r.getClass().getDeclaredField( "client" );
			c.setAccessible( true );
			if ( c.get( r ).equals( arguments.inner ) ) count++;
		}
		return count;
	}

	/** true if the connection pool of the inner client is shut down (a request fails with "Connection pool shut down") */
	private boolean function isShutdown( required inner ) {
		try {
			arguments.inner.getObjectMetadata( "ldev7004-bucket", "some/file.txt" );
			return false;
		}
		catch ( any e ) {
			return findNoCase( "pool shut down", fullMessage( e ) ) > 0;
		}
	}

	/** milliseconds since epoch, "now" for the retired clients */
	private numeric function nowMillis() {
		return createObject( "java", "java.lang.System" ).currentTimeMillis();
	}

	/** message, detail and stack trace (with the "Caused by" of the Java exception) of a catch block */
	private string function fullMessage( required e ) {
		return ( e.message ?: "" ) & " " & ( e.detail ?: "" ) & " " & ( e.stacktrace ?: "" );
	}

	/** value of a (private) field, declared by the class of the object or one of its super classes */
	private function fieldValue( required obj, required string name ) {
		var clazz = arguments.obj.getClass();
		while ( !isNull( clazz ) ) {
			try {
				var f = clazz.getDeclaredField( arguments.name );
				f.setAccessible( true );
				return f.get( arguments.obj );
			}
			catch ( java.lang.NoSuchFieldException e ) {
				clazz = clazz.getSuperclass();
			}
		}
		throw "field [#arguments.name#] not found";
	}
}
