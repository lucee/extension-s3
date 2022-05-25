/**
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
 */
package org.lucee.extension.resource.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.lucee.extension.resource.s3.acl.ACLList;
import org.lucee.extension.resource.s3.acl.AccessControlListUtil;

public final class S3ResourceOutputStream extends OutputStream {

	private final S3 s3;
	private final String bucketName;
	private final String objectName;
	private final ACLList acl;

	private String location;

	private File temp;
	private FileOutputStream os;
	private String path;

	public S3ResourceOutputStream(S3 s3, String bucketName, String objectName, String path, Object acl, String location) throws IOException {
		this.s3 = s3;
		this.bucketName = bucketName;
		this.objectName = objectName;
		this.acl = acl == null ? null : AccessControlListUtil.toAccessControlList(acl);
		this.location = location;
		this.path = path;
		temp = File.createTempFile("aws-s3", "tmp");
		os = new FileOutputStream(temp);
		// ts=CFMLEngineFactory.getInstance().getIOUtil().createTemporaryStream();
	}

	@Override
	public void close() throws IOException {
		try {
			// closing the temp file
			os.close();

			// write the temp file to s3
			s3.write(bucketName, objectName, temp, acl, location);
		}
		finally {
			// delete the temp file
			temp.delete();
		}
	}

	@Override
	public void flush() throws IOException {
		os.flush();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
	}

	@Override
	public void write(byte[] b) throws IOException {
		os.write(b);
	}

	@Override
	public void write(int b) throws IOException {
		os.write(b);
	}
}