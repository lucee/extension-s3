<!--- 
 *
 * Copyright (c) 2015, Lucee Assosication Switzerland. All rights reserved.
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
	
	//public function beforeTests(){}
	
	//public function afterTests(){}


	private struct function getCredencials() {
		return server.getTestService("s3");
	}
	  
	public function setUp(){
		var s3=getCredencials();
		if(!isNull(s3.accessKeyId)) {
			application action="update" s3={
				accessKeyId: s3.ACCESS_KEY_ID,
				awsSecretKey: s3.SECRET_KEY
			}; 
			variables.s3Supported=true;
		}
		else 
			variables.s3Supported=false;
	}

	public function testStoreMetadata() localMode=true {
		if(!variables.s3Supported) return;
		var bucketName="#server.getTestService("s3").bucket_prefix#metadata-#listFirst(replace(server.lucee.version,".","","all"),"-")#";
		var objectName="object";
		var dir="s3://#bucketName#/#objectName#/";
		if(DirectoryExists(dir)) directoryDelete(dir,true);
		try {
			assertFalse(DirectoryExists(dir));
			directoryCreate(dir);
			
			var md=s3GetMetaData(bucketName,objectName);
			var countBefore=structCount(md);
			s3SetMetaData(bucketName,objectName,{"susi":"Susanne"});
    		var md=s3GetMetaData(bucketName,objectName);
    		assertEquals(countBefore+1,structCount(md));
			assertEquals("Susanne",md.susi);
		}
		finally {
    		if(DirectoryExists(dir))
    			directoryDelete(dir,true);
    	}  
	}
} 
</cfscript>