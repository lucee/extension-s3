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

import java.util.Map;

import org.jets3t.service.model.StorageOwner;

public interface S3Info {
	
	public String getName();
	public String getObjectName();
	public String getBucketName();
	
	public long getSize();
	public long getLastModified();
	
	public boolean exists();
	public boolean isDirectory();
	public boolean isFile();
	public boolean isBucket();
	public long validUntil();
	
	public StorageOwner getOwner();
	public String getLocation();
	public Map<String, Object> getMetaData();
	
	/**
	 * is this a pseudo object or an object really physically exists
	 */
	public boolean isVirtual();
	
}