/**
 *
 * Copyright (c) 2015, Lucee Assosication Switzerland
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
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import lucee.commons.io.log.Log;
import lucee.loader.util.Util;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;

public final class StorageObjectWrapper extends S3InfoSupport {

	private final S3ObjectSummary so;
	private long validUntil;
	private Boolean isDirectory;
	private String contentType;

	public StorageObjectWrapper(S3 s3, S3ObjectSummary so, long validUntil, Log log) {
		super(s3, log);
		this.so = so;
		this.validUntil = validUntil;
	}

	/**
	 * @return the bucketName
	 */
	@Override
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
	@Override
	public String getName() {
		return getObjectName();
	}

	/**
	 * @return the lastModified
	 */
	@Override
	public long getLastModified() {
		return so.getLastModified() == null ? 0 : so.getLastModified().getTime();
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
		return so.getSize();
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
		Owner owner = so.getOwner();
		return owner == null ? null : owner.getId();
	}

	/**
	 * @return the ownerDisplayName
	 */
	public String getOwnerDisplayName() {
		Owner owner = so.getOwner();
		return owner == null ? null : owner.getDisplayName();
	}

	public URL getLink(int secondsValid) throws InvalidKeyException, NoSuchAlgorithmException, IOException {
		return s3.url(so.getBucketName(), so.getKey(), secondsValid * 1000);
	}

	public InputStream getInputStream() throws S3Exception {
		return s3.getInputStream(so.getBucketName(), so.getKey());
	}

	@Override
	public String toString() {
		return "eTag:" + getETag() + ";key:" + getKey() + ";ownerDisplayName:" + getOwnerDisplayName() + ";ownerIdKey:" + getOwnerId() + ";size:" + getSize() + ";storageClass:"
				+ getStorageClass() + ";";
	}

	@Override
	public boolean exists() {
		return true;
	}

	@Override
	public boolean isFile() {
		return !isDirectory();
	}

	@Override
	public boolean isDirectory() {
		if (isDirectory != null) return isDirectory.booleanValue();
		if (so.getSize() > 0) return isDirectory = false;
		if (so.getKey().endsWith("/")) return isDirectory = true;
		return isDirectory = "application/x-directory".equalsIgnoreCase(getContentType());
	}

	@Override
	public String getObjectName() {
		return so.getKey();
	}

	@Override
	public boolean isBucket() {
		return false;
	}

	@Override
	public long validUntil() {
		return validUntil;
	}

	public S3ObjectSummary getStorageObject() {
		return so;
	}

	@Override
	public Owner getOwner() {
		return so.getOwner();
	}

	@Override
	public boolean isVirtual() {
		return false;
	}

	public final String getContentType() {
		if (s3 == null) return null;
		if (contentType == null) {
			synchronized (S3.getToken("contentType:" + getBucketName() + ":" + getObjectName())) {
				if (contentType == null) {
					try {
						contentType = s3.getContentType(getBucketName(), getObjectName());
					}
					catch (Exception e) {
						if (log != null) log.error("s3", e);
						else e.printStackTrace();
					}
				}
			}
		}
		return contentType;
	}

	@Override
	public Struct getMetaData() throws S3Exception {
		Struct data = super.getMetaData();

		ObjectMetadata omd = s3.getObjectMetadata(getBucketName(), getObjectName());

		// raw
		{
			Map<String, Object> rmd = omd.getRawMetadata();
			Iterator<Entry<String, Object>> it = rmd.entrySet().iterator();
			Entry<String, Object> e;
			String name;
			while (it.hasNext()) {
				e = it.next();
				name = Util.replace(e.getKey(), "-", "", true);
				name = Character.toLowerCase(name.charAt(0)) + (name.length() > 1 ? name.substring(1) : "");
				data.setEL(name, e.getValue());
			}
		}

		// user
		{
			Map<String, String> umd = omd.getUserMetadata();
			Iterator<Entry<String, String>> it = umd.entrySet().iterator();
			Entry<String, String> e;
			String name;
			while (it.hasNext()) {
				e = it.next();
				name = Util.replace(e.getKey(), "-", "", true);
				name = Character.toLowerCase(name.charAt(0)) + (name.length() > 1 ? name.substring(1) : "");
				data.setEL(name, e.getValue());
			}
		}

		Array acl = s3.getAccessControlList(getBucketName(), getObjectName());
		if (acl != null) data.setEL("acl", acl);
		return data;
	}

}