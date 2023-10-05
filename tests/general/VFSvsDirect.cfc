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
			
		// DirectoryExists vs S3Exists
			it(title="check FileExists vs S3Exists with AWS",skip=Util::isAWSNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsDirectoryExists(Util::getAWSCredentials());
			});	
			it(title="check FileExists vs S3Exists with Blackbaze",skip=Util::isBackBlazeNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsDirectoryExists(Util::getBackBlazeCredentials());
			});	
			it(title="check FileExists vs S3Exists with Google",skip=Util::isGoogleNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsDirectoryExists(Util::getGoogleCredentials());
			});	
			it(title="check FileExists vs S3Exists with Wasabi",skip=Util::isWasabiNotSupported(), body = function( currentSpec ) {
				testS3ExistsVsDirectoryExists(Util::getWasabiCredentials());
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
		
		var bucketName=cred.PREFIXVersion&"vfs-vs-direct-exists"&getTickCount();
		var objectName="test.txt";
		var path="s3://#bucketName#/#objectName#"; 
		var pathWithCred="s3://#cred.ACCESS_KEY_ID#:#cred.SECRET_KEY#@";
		if(!isNull(cred.HOST)) pathWithCred&=cred.HOST;
		pathWithCred&="/#bucketName#/#objectName#";
		
		try {
			////////////////////////////////////////////////////////////////////////
			// does not exists yet (direct approach with explicit credentials)
			assertFalse(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// does not exists yet (direct approach with implicit credentials)
			assertFalse(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// does not exists yet (VFS approach with explicit credentials)
			assertFalse(fileExists(pathWithCred));
			// does not exists yet (VFS approach with implicit credentials)
			assertFalse(fileExists(path));

			////////////////////////////////////////////////////////////////////////
			// create the file direct approach
			s3Write(bucketName,  objectName,"Just some content");

			////////////////////////////////////////////////////////////////////////
			// must exists now (direct approach with explicit credentials)
			assertTrue(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// must exists now (direct approach with implicit credentials)
			assertTrue(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// must exists now (VFS approach with explicit credentials)
			assertTrue(fileExists(pathWithCred),path);
			// must exists now (VFS approach with implicit credentials)
			assertTrue(fileExists(path),path);
			
			var presignedURL=S3GeneratePresignedURL(path);
			assertTrue(fileExists(presignedURL),presignedURL);

			////////////////////////////////////////////////////////////////////////
			// deleteing the file again
			S3Delete(bucketName,  objectName,true);

			////////////////////////////////////////////////////////////////////////
			// does not exists anymore (direct approach with explicit credentials)
			assertFalse(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// does not exists anymore (direct approach with implicit credentials)
			assertFalse(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// does not exists anymore (VFS approach with explicit credentials)
			assertFalse(fileExists(pathWithCred));
			// does not exists anymore (VFS approach with implicit credentials)
			assertFalse(fileExists(path));

			////////////////////////////////////////////////////////////////////////
			// create the file VFS approach
			fileWrite(path, "Just some content"); // directory/bucket should exist at this point
			
			////////////////////////////////////////////////////////////////////////
			// must exists now (direct approach with explicit credentials)
			assertTrue(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// must exists now (direct approach with implicit credentials)
			assertTrue(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// must exists now (VFS approach with explicit credentials)
			assertTrue(fileExists(pathWithCred));
			// must exists now (VFS approach with implicit credentials)
			assertTrue(fileExists(path));

			var presignedURL=S3GeneratePresignedURL(path);
			assertTrue(fileExists(presignedURL),presignedURL);

			
			////////////////////////////////////////////////////////////////////////
			// deleteing the file again VFS approach
			fileDelete(path);

			////////////////////////////////////////////////////////////////////////
			// does not exists anymore (direct approach with explicit credentials)
			assertFalse(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// does not exists anymore (direct approach with implicit credentials)
			assertFalse(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// does not exists anymore (VFS approach with explicit credentials)
			assertFalse(fileExists(pathWithCred));
			// does not exists anymore (VFS approach with implicit credentials)
			assertFalse(fileExists(path));

		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
		}
	}


	private function testS3ExistsVsDirectoryExists(required struct cred) localMode=true {
		// setup env for VFS access
		setup(cred);
		
		var bucketName=cred.PREFIXVersion&"vfs-vs-direct-exists"&getTickCount();
		var objectName="subfolder";
		var path="s3://#bucketName#/#objectName#"; 
		var pathWithCred="s3://#cred.ACCESS_KEY_ID#:#cred.SECRET_KEY#@";
		if(!isNull(cred.HOST)) pathWithCred&=cred.HOST;
		pathWithCred&="/#bucketName#/#objectName#";
		
		try {
			////////////////////////////////////////////////////////////////////////
			// does not exists yet (direct approach with explicit credentials)
			assertFalse(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// does not exists yet (direct approach with implicit credentials)
			assertFalse(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// does not exists yet (VFS approach with explicit credentials)
			assertFalse(directoryExists(pathWithCred));
			// does not exists yet (VFS approach with implicit credentials)
			assertFalse(directoryExists(path));

			////////////////////////////////////////////////////////////////////////
			// create the file VFS approach
			directoryCreate(path);

			////////////////////////////////////////////////////////////////////////
			// must exists now (direct approach with explicit credentials)
			assertTrue(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// must exists now (direct approach with implicit credentials)
			assertTrue(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// must exists now (VFS approach with explicit credentials)
			assertTrue(directoryExists(pathWithCred),path);
			// must exists now (VFS approach with implicit credentials)
			assertTrue(directoryExists(path),path);

			
			////////////////////////////////////////////////////////////////////////
			// deleteing the file again VFS approach
			directoryDelete(path, true);

			////////////////////////////////////////////////////////////////////////
			// does not exists anymore (direct approach with explicit credentials)
			assertFalse(S3Exists( 
				bucketName:bucketName,  objectName:objectName?:nullValue(), 
				accessKeyId:cred.ACCESS_KEY_ID, secretAccessKey:cred.SECRET_KEY, host:(isNull(cred.HOST)?nullvalue():cred.HOST)));
			// does not exists anymore (direct approach with implicit credentials)
			assertFalse(S3Exists( bucketName:bucketName,  objectName:objectName?:nullValue()));
			// does not exists anymore (VFS approach with explicit credentials)
			assertFalse(directoryExists(pathWithCred));
			// does not exists anymore (VFS approach with implicit credentials)
			assertFalse(directoryExists(path));

		}
		finally {
			Util::deleteBucketEL(cred,bucketName);
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