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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public final class StorageObjectWrapper extends S3InfoSupport {

	private final S3 s3;
	private final S3ObjectSummary so;
	private long validUntil;
	private Boolean isDirectory;

	public StorageObjectWrapper(S3 s3, S3ObjectSummary so, long validUntil) {
		this.s3 = s3;
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

	public String getLink(int secondsValid) throws InvalidKeyException, NoSuchAlgorithmException, IOException {

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
	/*
	 * public boolean isDirectory() { String ct =
	 * CFMLEngineFactory.getInstance().getCastUtil().toString(so.getMetadata("Content-Type"),null); //
	 * sadly a directory not necessary has set "application/x-directory" so not existing does not mean
	 * it is not a directory if(!Util.isEmpty(ct) && "application/x-directory".equalsIgnoreCase(ct))
	 * return true; return getSize()==0 && getKey().endsWith("/");
	 * 
	 * }
	 */
	public boolean isFile() {
		return !isDirectory();
	}

	@Override
	public boolean isDirectory() {
		if (isDirectory != null) return isDirectory.booleanValue();

		if (so.isDirectoryPlaceholder()) return isDirectory = true;
		if (so.getSize() > 0) return isDirectory = false;
		Object o = so.getMetadata("Content-Type");
		// System.out.println("- Content-Type:"+o);
		if (o instanceof String) {
			String ct = (String) o;
			if ("application/x-directory".equalsIgnoreCase(ct)) return isDirectory = true;
			if (ct.startsWith("audio/")) return isDirectory = false;
			if (ct.startsWith("image/")) return isDirectory = false;
			if (ct.startsWith("text/")) return isDirectory = false;
			if (ct.startsWith("video/")) return isDirectory = false;
		}

		// when a file has "children" it is a directory
		/*
		 * if(sisters!=null) { String name=S3.improveObjectName(so.getName(), true); for(StorageObject
		 * sis:sisters) { if(sis.getName().startsWith(name) && sis.getName().length()>name.length()) return
		 * isDirectory=true; } }
		 */

		if (getKey().endsWith("/")) return isDirectory = true;
		if (getKey().contains(".")) return isDirectory = false;

		return isDirectory = true; // i don't like this, but this is a pattern used with S3
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
}