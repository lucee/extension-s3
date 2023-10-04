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
component extends="org.lucee.cfml.test.LuceeTestCase"	{
	
	function run( testResults , testBox ) {
		describe( title="Test suite for ACL funcions", body=function() {
			it(title="check region with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testit(Util::getBackBlazeCredentials());
			});	

			it(title="check with amazon",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testit(Util::getAWSCredentials());
			});	

			it(title="check with wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testit(Util::getWasabiCredentials());
			});		

			it(title="check with google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testit(Util::getGoogleCredentials());
			});			
	
		});
	}

	private function testit(cred) {

		if(!isNull(cred.ACCESS_KEY_ID)) {
			application action="update" s3={
				accessKeyId: cred.ACCESS_KEY_ID,
				awsSecretKey: cred.SECRET_KEY
				host:isNull(cred.HOST)?nullValue(): cred.HOST
			};
		}

		testStoreAddACLBucket(cred);
		testStoreSetACLBucket(cred);
		testStoreAddACLObject(cred);
	}

	private function testStoreAddACLBucket(cred) localMode=true {
		try{
			testStoreACL("s3://lucee-testsuite-addaclbucket",true,true);
		}
		finally {
			directoryDelete("s3://lucee-testsuite-addaclbucket",true);
		}
	}


	private function testStoreSetACLBucket(cred) localMode=true {
		try{
			testStoreACL("s3://lucee-testsuite-setaclbucket2",true,false);
		}
		finally {
			directoryDelete("s3://lucee-testsuite-setaclbucket2",true);
		}
	}

	private function testStoreAddACLObject(cred) localMode=true {
		try{
			testStoreACL("s3://lucee-testsuite-addaclobject/sub12234",false,true);
		}
		finally {
			directoryDelete("s3://lucee-testsuite-addaclobject",true);
		}
	}

	private function testStoreSetACLObject(cred) localMode=true {
		try{
			testStoreACL("s3://lucee-testsuite-setaclobject2/sub12234",false,false);
		}
		finally {
			directoryDelete("s3://lucee-testsuite-setaclobject2",true);
		}
	}

	private function testStoreACL(required dir, required boolean bucket, required boolean add) localMode=true {
		    start=getTickCount();
		    
		    if(DirectoryExists(dir)) directoryDelete(dir,true);

		    assertFalse(DirectoryExists(dir));
			directoryCreate(dir);
		    
		    // check inital data
			var acl=StoreGetACL(dir);
			assertEquals(1,acl.len());
			assertEquals("FULL_CONTROL",toList(acl,"permission"));
			assertEquals("info",toList(acl,"displayName"));
			//var id=acl[1].id;
			
			// add ACL
			if(add) {
			    arr=[{'group':"authenticated",'permission':"WRITE"}];
			    StoreAddACL(dir,arr); 

			    // test output
			    var acl=StoreGetACL(dir);
			    
				assertEquals(2,acl.len());
				assertEquals("FULL_CONTROL,WRITE",toList(acl,"permission"));
				assertEquals("authenticated",toList(acl,"group"));
				
			}
			// set ACL 
			else {
				arr=[{'group':"authenticated",'permission':"WRITE"}];
			    StoreSetACL(dir,arr); 

				// test output
			    var acl=StoreGetACL(dir);
			    
					assertEquals(1,acl.len());
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