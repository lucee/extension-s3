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

import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.Owner;

import lucee.runtime.type.Struct;

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

	public Owner getOwner();

	/**
	 * is this a pseudo object or an object really physically exists
	 */
	public boolean isVirtual();

	public Regions getRegion();

	public Struct getMetaData() throws S3Exception;

}