/**
 *
 * Copyright (c) 2014, the Railo Company Ltd. All rights reserved.
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
 **/
package org.lucee.extension.resource.s3.info;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.jets3t.service.model.StorageObject;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;
import org.xml.sax.SAXException;

public final class StorageObjectWrapper implements S3Info {
	
	private final S3 s3;
	private final StorageObject so;
	

	public StorageObjectWrapper(S3 s3, StorageObject so) {
		this.s3=s3;
		this.so=so;
	}
	
	/**
	 * @return the bucketName
	 */
	public String getBucketName() {
		return so.getBucketName();
	}
	

	
	/**
	 * @return the key
	 */
	public String getKey() {
		return so.getKey();
	}
	
	/**
	 * @return the key
	 */
	public String getName() {
		return so.getName();
	}

	/**
	 * @return the lastModified
	 */
	@Override
	public long getLastModified() {
		return so.getLastModifiedDate().getTime();
	}

	/**
	 * @return the eTag
	 */
	public String getETag() {
		return so.getETag();
	}

	/**
	 * @return the size
	 */
	@Override
	public long getSize() {
		return so.getContentLength();
	}

	/**
	 * @return the storageClass
	 */
	public String getStorageClass() {
		return so.getStorageClass();
	}

	/**
	 * @return the ownerIdKey
	 */
	public String getOwnerId() {
		return so.getOwner().getId();
	}

	/**
	 * @return the ownerDisplayName
	 */
	public String getOwnerDisplayName() {
		return so.getOwner().getDisplayName();
	}

	public String getLink(int secondsValid) throws InvalidKeyException, NoSuchAlgorithmException, IOException {
		
		return s3.url(so.getBucketName(), so.getKey(), secondsValid*1000);
	}
	
	public InputStream getInputStream() throws S3Exception {
		return s3.getInputStream(so.getBucketName(), so.getKey());
	}
	
	
	@Override
	public String toString() {
		return "eTag:"+getETag()+";key:"+getKey()+";ownerDisplayName:"+getOwnerDisplayName()+
				";ownerIdKey:"+getOwnerId()+";size:"+getSize()+";storageClass:"+getStorageClass()+";";
	}
	
	@Override
	public boolean exists() {
		return true;
	}
	
	@Override
	public boolean isDirectory() {
		return getSize()==0 && getKey().endsWith("/");
	}
	
	@Override
	public boolean isFile() {
		return getSize()>0 || !getKey().endsWith("/");
	}

	@Override
	public String getObjectName() {
		return so.getName();
	}

	@Override
	public boolean isBucket() {
		return false;
	}
	
	
}