<!--- 
 *
 * Copyright (c) 2015, Lucee Assosication Switzerland. All rights reserved.
 * Copyright (c) 2014, the Railo Company LLC. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either 
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public 
 * License along with this library.  If not, see <http://www.gnu.org/licenses/>.
 * 
 ---><cfscript>
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3"	{
	
	function run( testResults , testBox ) {
		describe( title="Test suite for ACL funcions", body=function() {
			
		// AWS
			/*it(title="check with amazon add acl for bucket store style",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketStore(cred);
			});	
	
			it(title="check with amazon add acl for bucket s3 style",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketS3(cred);
			});	

			it(title="check with amazon set acl for bucket store style",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketStore(cred);
			});	
			
			it(title="check with amazon set acl for bucket s3 style",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketS3(cred);
			});	

			it(title="check with amazon add acl for object store style",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectStore(cred);
			});	

			it(title="check with amazon add acl for object s3 style",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				var cred=Util::getAWSCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectS3(cred);
			});	*/
		
		// Blackbaze
			it(title="check with Blackbaze add acl for bucket store style",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=Util::getBackBlazeCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketStore(cred);
			});	

			it(title="check with Blackbaze add acl for bucket s3 style",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=Util::getBackBlazeCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketS3(cred);
			});	

			it(title="check with Blackbaze set acl for bucket store style",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=Util::getBackBlazeCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketStore(cred);
			});	
			
			it(title="check with Blackbaze set acl for bucket s3 style",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=Util::getBackBlazeCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketS3(cred);
			});	

			it(title="check with Blackbaze add acl for object store style",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=Util::getBackBlazeCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectStore(cred);
			});	

			it(title="check with Blackbaze add acl for object s3 style",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				var cred=Util::getBackBlazeCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectS3(cred);
			});	
		
		// Google
			it(title="check with Google add acl for bucket store style",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				var cred=Util::getGoogleCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketStore(cred);
			});	

			it(title="check with Google add acl for bucket s3 style",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				var cred=Util::getGoogleCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketS3(cred);
			});	

			it(title="check with Google set acl for bucket store style",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				var cred=Util::getGoogleCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketStore(cred);
			});	
			
			it(title="check with Google set acl for bucket s3 style",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				var cred=Util::getGoogleCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketS3(cred);
			});	

			it(title="check with Google add acl for object store style",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				var cred=Util::getGoogleCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectStore(cred);
			});	

			it(title="check with Google add acl for object s3 style",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				var cred=Util::getGoogleCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectS3(cred);
			});	

		// WASABI
			it(title="check with Wasabi add acl for bucket store style",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				var cred=Util::getWasabiCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketStore(cred);
			});	
	
			it(title="check with Wasabi add acl for bucket s3 style",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				var cred=Util::getWasabiCredentials();
				if(!setup(cred)) return;
				testStoreAddACLBucketS3(cred);
			});	

			it(title="check with Wasabi set acl for bucket store style",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				var cred=Util::getWasabiCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketStore(cred);
			});	
			
			it(title="check with Wasabi set acl for bucket s3 style",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				var cred=Util::getWasabiCredentials();
				if(!setup(cred)) return;
				testStoreSetACLBucketS3(cred);
			});	

			it(title="check with Wasabi add acl for object store style",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				var cred=Util::getWasabiCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectStore(cred);
			});	

			it(title="check with Wasabi add acl for object s3 style",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				var cred=Util::getWasabiCredentials();
				if(!setup(cred)) return;
				testStoreAddACLObjectS3(cred);
			});	
		});
	}

	private function setup(cred) {
		if(!isNull(cred.ACCESS_KEY_ID)) {
			application action="update" s3={
				accessKeyId: cred.ACCESS_KEY_ID,
				awsSecretKey: cred.SECRET_KEY,
				host:isNull(cred.HOST)?nullValue(): cred.HOST,
			};
			cred.PREFIXVersion=cred.PREFIX&""&listFirst(replace(server.lucee.version,".","","all"),"-");
			return true;
		}
		return false;
	}

	private function testStoreAddACLBucketStore(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIXVersion#addaclbuckets","",true,true,false);
		}
		finally {
			Util::deleteBucketEL(cred,"#cred.PREFIXVersion#addaclbuckets");
		}
	}
	private function testStoreAddACLBucketS3(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIXVersion#addaclbucketss3","",true,true,true);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIXVersion#addaclbucketss3");
		}
	}

	private function testStoreSetACLBucketStore(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIXVersion#setaclbuckets","",true,false,false);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIXVersion#setaclbuckets");
		}
	}
	private function testStoreSetACLBucketS3(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIXVersion#setaclbuckets3","",true,false,true);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIXVersion#setaclbuckets3");
		}
	}

	private function testStoreAddACLObjectStore(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIXVersion#addaclobjs","sub12234",false,true,false);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIXVersion#addaclobjs");
		}
	}
	private function testStoreAddACLObjectS3(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIXVersion#addaclobjs3","sub12234",false,true,true);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIXVersion#addaclobjs3");
		}
	}

	private function testStoreSetACLObjectStore(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIX#setaclobjs","sub12234",false,false);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIX#setaclobjs");
		}
	}
	private function testStoreSetACLObjectS3(cred) localMode=true {
		try{
			testStoreACL(cred,"#cred.PREFIX#setaclobjs3","sub12234",false,false);
		}
		finally {
			Util::deleteBucketEL(cred,"s3://#cred.PREFIX#setaclobjs3");
		}
	}

	private function testStoreACL(cred,required string bucketName,required string objectName="", required boolean bucket, required boolean add, boolean useS3Function=false) localMode=true {
		// set path
		var dir="s3://#arguments.bucketName#";    
		if(!isEmpty(arguments.objectName)) dir&="/"&arguments.objectName;
		start=getTickCount();
		Util::deleteIfExists(cred,arguments.bucketName,arguments.objectName);
		
		assertFalse(S3Exists( 
			bucketName:bucketName,  objectName:objectName?:nullValue(), 
			accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
		
		assertFalse(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			
		
			directoryCreate(dir);
		    
		    // check inital data
			if(useS3Function) {
				var acl=S3GetACL(arguments.bucketName,isEmpty(arguments.objectName)?"":arguments.objectName);
			}
			else {
				var acl=StoreGetACL(dir);
			}
			assertEquals(1,acl.len());
			assertEquals("FULL_CONTROL",toList(acl,"permission"));
			
			
			// add ACL
			if(add) {
			    arr=[{'group':"authenticated",'permission':"WRITE"}];
			    if(useS3Function) {
					if(isEmpty(arguments.objectName)) S3AddACL(arguments.bucketName,"",arr);
					else S3AddACL(arguments.bucketName,arguments.objectName,arr);
				}
				else {
					StoreAddACL(dir,arr); 
				}

			    if(useS3Function) {
					var acl=S3GetACL(arguments.bucketName,isEmpty(arguments.objectName)?"":arguments.objectName);
				}
				else {
					var acl=StoreGetACL(dir);
				}
				// test result
				assertEquals(2,acl.len());
				assertEquals("FULL_CONTROL,WRITE",toList(acl,"permission"));
				assertEquals("authenticated",toList(acl,"group"));
			}
			// set ACL 
			else {
				arr=[{'group':"authenticated",'permission':"WRITE"}];
			    
				if(useS3Function) {
					if(isEmpty(arguments.objectName)) S3SetACL(arguments.bucketName,"",arr);
					else S3SetACL(arguments.bucketName,arguments.objectName,arr);
				}
				else {
					StoreSetACL(dir,arr); 
				}

				// test output
				if(useS3Function) {
					var acl=S3GetACL(arguments.bucketName,isEmpty(arguments.objectName)?"":arguments.objectName);
				}
				else {
					var acl=StoreGetACL(dir);
				}
			    
				assertEquals(1,acl.len());
				if(toList(acl,"permission")!="WRITE")throw serialize(acl);
				assertEquals("WRITE",toList(acl,"permission"));
				assertEquals("authenticated",toList(acl,"group"));
			}
	}


	private function toList(arr,key){
		var rtn="";
		loop array=arr item="local.sct" {
			if(!isNull(sct[key]))rtn=listAppend(rtn,sct[key]);
		}
		return listSort(rtn,"textnoCase");
 	}

} 
</cfscript>