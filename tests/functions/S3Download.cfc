component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {
	



	function run( testResults , testBox ) {
		describe( title="Test suite for S3Download()",skip=Util::isAWSNotSupported(), body=function() {
			var objectName="sub/test.txt";
			var content="Susi
Sorglos";
			
			aroundEach( function( spec, suite ){
			    var bucketName = Util::createBucketName("download");
				var cred = Util::getAWSCredentials();
				try {
					if(!S3Exists( 
						bucketName=bucketName,  objectName=objectName, 
						accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host=(isNull(cred.HOST)?nullvalue():cred.HOST))) {
						S3Write( 
							value=content,
							bucketName=bucketName,  objectName=objectName, 
							accessKeyId=cred.ACCESS_KEY_ID, secretAccessKey=cred.SECRET_KEY, host=(isNull(cred.HOST)?nullvalue():cred.HOST));
					}
					arguments.spec.body({bucketName:bucketName,cred:cred});
				}	
				finally {
					Util::deleteBucketEL(cred,bucketName);
				}
			});

			
			it(title="download as binary", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var data=s3Download(bucket:bucketName,object:objectName,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				assertTrue(isBinary(data));
				assertEquals(len(data),12);
				assertEquals(toString(data), content);
			});	
			
			it(title="download as string", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var data=s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),12);
				assertEquals(data, content);
			});	

//////// FILE ///////////		
			it(title="download to file (fileOpen)", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var target=getDirectoryFromPath(getCurrentTemplatePath())&"temp.txt";
				try {
					s3Download(bucket:bucketName,object:objectName,target:fileOpen(target,"write"),accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
					var data=fileRead(target);
					assertTrue(isSimpleValue(data));
					assertEquals(len(data),12);
					assertEquals(data, content);
				}
				finally {
					if(fileExists(target)) fileDelete(target);
				}
			});	

				
			it(title="download to file (string path)", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var target=getDirectoryFromPath(getCurrentTemplatePath())&"temp.txt";
				try {
					s3Download(bucket:bucketName,object:objectName,target:target,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
					var data=fileRead(target);
					assertTrue(isSimpleValue(data));
					assertEquals(len(data),12);
					assertEquals(data, content);
				}
				finally {
					if(fileExists(target)) fileDelete(target);
				}
			});	

//////// UDF ///////////
			it(title="download to UDF:line", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
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
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
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
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(string4){
					data&=string4&":"&len(string4)&";";
        			return true;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),21);
				assertEquals(data, "Susi:4;
Sor:4;glos:4;");
			});	

			it(title="download to UDF:string with charset", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(string4){
					data&=string4&":"&len(string4)&";";
        			return true;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),21);
				assertEquals(data, "Susi:4;
Sor:4;glos:4;");
			});	

			it(title="download to UDF:binary", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var data="";
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:function(binary5){
					data&=len(binary5)&";";
        			return true;
				},accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),6);
				assertEquals(data, "5;5;2;");
			});	



//////// COMPONENT ///////////
			it(title="download to component:line", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var listener=new LineListener();
				s3Download(bucket:bucketName,object:objectName,target:listener,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				var data=listener.getData();
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),26);
				assertEquals(data, "before;Susi;Sorglos;after;");
			});	

			it(title="download to component:line with charset", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var listener=new LineListener();
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:listener,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				var data=listener.getData();
				assertTrue(isSimpleValue(data));
				assertEquals(len(data),26);
				assertEquals(data, "before;Susi;Sorglos;after;");
			});	

			it(title="download to component:string with charset", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var listener=new StringListener();
				s3Download(bucket:bucketName,object:objectName,charset:"UTF-8",target:listener,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				var data=listener.getData();
				assertTrue(isSimpleValue(data));
				assertEquals(data, "before;Susi;after;");
			});	

			it(title="download to component:binary", body = function( currentSpec ) {
				var cred=currentSpec.cred;
				var bucketName=currentSpec.bucketName;
				var listener=new BinaryListener();
				s3Download(bucket:bucketName,object:objectName,target:listener,accessKeyId:cred.ACCESS_KEY_ID,secretAccessKey:cred.SECRET_KEY);
				var data=listener.getData();
				assertTrue(isSimpleValue(data));
				assertEquals(data, "before;4;4;4;after;");
			});	
	
		});
	}
}