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
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3"	{
	function run( testResults , testBox ) {
		describe( title="Test suite for S3 Metadata", body=function() {
			it(title="check with blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
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
	  
	private function setUp(s3){
		if(!isNull(s3.accessKeyId)) {
			application action="update" s3={
				accessKeyId: s3.ACCESS_KEY_ID,
				awsSecretKey: s3.SECRET_KEY,
				host:isNull(s3.HOST)?nullValue(): s3.HOST
			};
		}
	}

	private function testit(cred) localMode=true {
		setUp(cred);
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