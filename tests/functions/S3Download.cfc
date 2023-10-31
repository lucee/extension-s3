component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	function run( testResults , testBox ) {
		describe( title="Test suite for S3Download()",skip=Util::isAWSNotSupported(), body=function() {
			
			var cred=Util::getAWSCredentials();
			var bucketName=cred.PREFIX&"-download"&listFirst(replace(server.lucee.version,".","","all"),"-");
			var objectName="sub/test.txt";
			var content="Susi
Sorglos";
			if(!S3Exists( 
				bucketName:bucketName,  objectName:objectName, 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST))) {
				S3Write( 
					value:content,
					bucketName:bucketName,  objectName:objectName, 
					accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST));
			}
			
			it(title="download as binary", body = function( currentSpec ) {
				var data=s3Download(bucket:bucketName,object:objectName,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				assertTrue(isBinary(data));
				assertEquals(len(data),13);
				assertEquals(toString(data), content);
			});	
			
			it(title="download as string", body = function( currentSpec ) {
				var data=s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),13);
				assertEquals(data, content);
			});	
			
			it(title="download to file", body = function( currentSpec ) {
				var target=getDirectoryFromPath(getCurrentTemplatePath())&"temp.txt";
				try {
					s3Download(bucket:bucketName,object:objectName,target:fileOpen(target,"write"),accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
					var data=fileRead(target);
					assertTrue(isSimpleValue(data));
					assertEquals(len(data),13);
					assertEquals(data, content);
				}
				finally {
					if(fileExists(target)) fileDelete(target);
				}
			});	

			it(title="download to UDF:line", body = function( currentSpec ) {
				var data="";
				s3Download(bucket:bucketName,object:objectName,target:function(line){
					data&=line;
					return false;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),4);
				assertEquals(data, "Susi");
			});	

			it(title="download to UDF:line with charset", body = function( currentSpec ) {
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(line){
					data&=line;
					return false;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),4);
				assertEquals(data, "Susi");
			});	

			it(title="download to UDF:line with charset", body = function( currentSpec ) {
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(string4){
					data&=string4&":"&len(string4)&";";
        			return true;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),4);
				assertEquals(data, "Susi");
			});	

			it(title="download to UDF:string with charset", body = function( currentSpec ) {
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(string4){
					data&=string4&":"&len(string4)&";";
        			return true;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),4);
				assertEquals(data, "Susi");
			});	

			it(title="download to UDF:binary", body = function( currentSpec ) {
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(binary4){
					data&=len(binary4)&";";
        			return true;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),4);
				assertEquals(data, "Susi");
			});	
			
		});
	}
}