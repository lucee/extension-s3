/**
 * LDEV-6864: an invalid pool setting became 0 (wait forever for a connection or an answer, or no connection at all)
 * instead of being ignored.
 * https://luceeserver.atlassian.net/browse/LDEV-6864
 *
 * Loads the settings with S3Properties.load, no S3 server needed.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6864: invalid pool settings", body=function() {

			it( title="invalid connectionTimeout keeps the default", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { connectionTimeout: "nope" } } );
				expect( config.getConnectionTimeout() ).toBe( 10000 );
			});

			it( title="invalid socketTimeout keeps the default", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { socketTimeout: "nope" } } );
				expect( config.getSocketTimeout() ).toBe( 50000 );
			});

			it( title="invalid maxConnections keeps the default", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { maxConnections: "nope" } } );
				expect( config.getMaxConnections() ).toBe( 128 );
			});

			it( title="valid values next to an invalid one are used", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { maxConnections: "nope", socketTimeout: 1500 } } );
				expect( config.getMaxConnections() ).toBe( 128 );
				expect( config.getSocketTimeout() ).toBe( 1500 );
			});

			it( title="valid values are still used", body=function() {
				var config = clientConfig( { accessKeyId: "a", awsSecretKey: "b", pool: { maxConnections: "9", connectionTimeout: 1234, socketTimeout: "1500" } } );
				expect( config.getMaxConnections() ).toBe( 9 );
				expect( config.getConnectionTimeout() ).toBe( 1234 );
				expect( config.getSocketTimeout() ).toBe( 1500 );
			});

		});
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
