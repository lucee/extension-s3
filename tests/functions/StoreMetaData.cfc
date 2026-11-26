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
component extends="org.lucee.cfml.test.LuceeTestCase" labels="s3" {

	// one spec per provider that is configured, each sets its own credentials
	function run( testResults , testBox ) {
		describe( title="Test suite for StoreGetMetaData()/StoreSetMetaData()", body=function() {
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

	// sets this test's own credentials as the Application s3 settings, so it does not depend on what an earlier test left there
	private function applyCredentials(cred){
		application action="update" s3={
			accessKeyId: cred.ACCESS_KEY_ID,
			awsSecretKey: cred.SECRET_KEY,
			host:isNull(cred.HOST)?nullValue(): cred.HOST
		};
	}

	private function testit(cred) localMode=true {
		applyCredentials(cred);
		var bucketName=Util::createBucketName("store-meta");
		var bucket="s3://" & bucketName & "/";
		var dir=bucket&"object/";
		try {
			assertFalse(DirectoryExists(dir));
			directoryCreate(dir);

			var md=storeGetMetaData(dir);
			var countBefore=structCount(md);

			// add a user metadata entry
			storeSetMetaData(dir,{"susi":"Susanne"});
			md=storeGetMetaData(dir);
			assertEquals(countBefore+1,structCount(md));
			assertEquals("Susanne",md.susi);

			// the same entry set again changes the value and does not add one
			storeSetMetaData(dir,{"susi":"Sorglos"});
			md=storeGetMetaData(dir);
			assertEquals(countBefore+1,structCount(md));
			assertEquals("Sorglos",md.susi);
		}
		finally {
			// delete the whole bucket (incl. objects), not only the object dir, otherwise the bucket leaks
			Util::deleteBucketEL(cred,bucketName);
		}
	}
}
</cfscript>
