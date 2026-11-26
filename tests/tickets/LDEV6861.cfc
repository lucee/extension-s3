/**
 * LDEV-6861: S3Properties.toS3 read the "bucketname" setting only when no host was defined (the condition
 * tested host instead of bucket), so the bucket of the s3 settings was ignored when a host was set, and
 * reset to null when no host and no "bucketname" were set.
 * https://luceeserver.atlassian.net/browse/LDEV-6861
 *
 * Loads the settings with S3Properties.load, no S3 server needed.
 */
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function run( testResults, testBox ) {

		describe( title="LDEV-6861: bucket of the s3 settings", body=function() {

			it( title="bucket without host", body=function() {
				expect( loadBucket( { accessKeyId: "a", awsSecretKey: "b", bucket: "my-bucket" } ) ).toBe( "my-bucket" );
			});

			it( title="bucket with host", body=function() {
				expect( loadBucket( { accessKeyId: "a", awsSecretKey: "b", host: "s3.example.org", bucket: "my-bucket" } ) ).toBe( "my-bucket" );
			});

			it( title="bucketname with host", body=function() {
				expect( loadBucket( { accessKeyId: "a", awsSecretKey: "b", host: "s3.example.org", bucketname: "other-bucket" } ) ).toBe( "other-bucket" );
			});

			it( title="bucketname without host", body=function() {
				expect( loadBucket( { accessKeyId: "a", awsSecretKey: "b", bucketname: "other-bucket" } ) ).toBe( "other-bucket" );
			});

			it( title="bucket wins over bucketname", body=function() {
				expect( loadBucket( { accessKeyId: "a", awsSecretKey: "b", host: "s3.example.org", bucket: "my-bucket", bucketname: "other-bucket" } ) ).toBe( "my-bucket" );
			});

			it( title="no bucket defined", body=function() {
				expect( isNull( loadBucket( { accessKeyId: "a", awsSecretKey: "b", host: "s3.example.org" } ) ) ).toBeTrue();
			});

		});
	}

	// the classes of the extension are loaded by the class loader of its resource provider
	private function loadBucket( required struct settings ) {
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
		var load = cls.getMethod( "load", types );
		var props = load.invoke( javaCast( "null", "" ), javaCast( "java.lang.Object[]", [ getPageContext(), { s3: arguments.settings }, "" ] ) );
		return props.getBucket();
	}
}
