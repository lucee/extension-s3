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
		describe( title="Test suite to compare the VFS against the direct approch vs HTTP (if possible)", body=function() {
			
		// FileExists vs S3Exists
			it(title="check FileExists vs S3Exists with AWS",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsFileExists(Util::getAWSCredentials());
			});	
			it(title="check FileExists vs S3Exists with Blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsFileExists(Util::getBackBlazeCredentials());
			});	
			it(title="check FileExists vs S3Exists with Google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsFileExists(Util::getGoogleCredentials());
			});	
			it(title="check FileExists vs S3Exists with Wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsFileExists(Util::getWasabiCredentials());
			});	
				

		});
	}

	private function setup(cred) {
		application action="update" s3={
			accessKeyId: cred.ACCESS_KEY_ID,
			awsSecretKey: cred.SECRET_KEY,
			host:isNull(cred.HOST)?nullValue(): cred.HOST
		};
		cred.PREFIXVersion=cred.PREFIX&""&listFirst(replace(server.lucee.version,".","","all"),"-");
	}


	private function testS3ExistsVsFileExists(required struct cred) localMode=true {
		// setup env for VFS access
		setup(cred);
		
		var bucketName=cred.PREFIXVersion&"vfs-write-add-"&getTickCount();
		var objectName="test.txt";
		var path="s3://#bucketName#/#objectName#"; 
		var pathWithCred="s3://#cred.ACCESS_KEY_ID#:#cred.SECRET_KEY#@";
		if(!isNull(cred.HOST)) pathWithCred&=cred.HOST;
		pathWithCred&="/#bucketName#/#objectName#";
		
		var phrase1="Just some content.";
		var phrase2="Some more content.";


		try {
			////////////////////////////////////////////////////////////////////////
			// does not exists yet (direct approach with explicit credentials)
			assertFalse(fileExists(pathWithCred));

			// write
			fileWrite(pathWithCred,phrase1);
			assertTrue(fileExists(pathWithCred),path);
			var content=fileRead(pathWithCred);
			assertEqual(content,phrase1);

			// append
			fileAppend(pathWithCred,phrase2);
			var content=fileRead(pathWithCred);
			assertEqual(content,phrase1&phrase2);

			fileDelete(pathWithCred);
			assertFalse(fileExists(pathWithCred));
		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}
} 
</cfscript>