component extends = "org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	function beforeAll(){
		variables.uri = createURI("LDEV4407");
	}

	function run( testResults, testBox ){
		describe( "Test case for LDEV4407", function(){
			it(title = "only define S3.ACL in Application.cfc", skip=isNotSupported(), body = function( currentSpec ){
				local.result = _InternalRequest(
					template : "#uri#\index.cfm"
				)
				expect(result.filecontent).tobe("true");
			});
		});
	}

	private string function createURI(string calledName){
		var baseURI="/test/#listLast(getDirectoryFromPath(getCurrenttemplatepath()),"\/")#/";
		return baseURI&""&calledName;
	}
	
	private boolean function isNotSupported() {
		res= getCredentials();
		return isNull(res) || len(res)==0;
	}
	private struct function getCredentials() {
		var ACCESS_KEY_ID=server.system.environment.S3_ACCESS_ID_TEST?:nullValue();
		if(isNull(ACCESS_KEY_ID) || isEmpty(ACCESS_KEY_ID)) return {};
		var SECRET_KEY=server.system.environment.S3_SECRET_KEY_TEST?:nullValue();
		if(isNull(SECRET_KEY) || isEmpty(SECRET_KEY)) return {};

		return {ACCESS_KEY_ID:ACCESS_KEY_ID,SECRET_KEY:SECRET_KEY};
	}
}