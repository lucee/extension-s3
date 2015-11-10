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
package org.lucee.extension.resource.s3;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;

public final class S3ResourceOutputStream extends OutputStream {
	
	private final S3 s3;

	private final String mimetype="application/octet-stream";
	private final String charset="iso-8859-1";
	private final String bucketName;
	private final String objectName;
	private final int acl;

	private OutputStream ts;

	private CFMLEngine engine;
	
	public S3ResourceOutputStream(S3 s3,String bucketName,String objectName,int acl) {
		engine = CFMLEngineFactory.getInstance();
		this.s3=s3;
		this.bucketName=bucketName;
		this.objectName=objectName;
		this.acl=acl;
		ts=CFMLEngineFactory.getInstance().getIOUtil().createTemporaryStream();
	}
	
	@Override
	public void close() throws IOException {
		ts.close();
		
		//InputStream is = ts.getInputStream();
		try {
			s3.put(bucketName, objectName, acl, ts,mimetype,charset);
		} 

		catch (SocketException se) {
			String msg = engine.getStringUtil().emptyIfNull(se.getMessage());
			if(msg.toLowerCase().indexOf("socket closed")==-1)
				throw se;
		}
		catch (Exception e) {
			throw engine.getExceptionUtil().toIOException(e);
		}
	}

	@Override
	public void flush() throws IOException {
		ts.flush();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		ts.write(b, off, len);
	}

	@Override
	public void write(byte[] b) throws IOException {
		ts.write(b);
	}

	@Override
	public void write(int b) throws IOException {
		ts.write(b);
	}
}