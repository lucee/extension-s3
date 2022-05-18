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

import java.io.IOException;

public class S3Exception extends IOException {

	private static final long serialVersionUID = 454222134889105256L;
	private String ec;
	private long proposedSize;

	public S3Exception(String message) {
		super(message);
	}

	public void setErrorCode(String ec) {
		this.ec = ec;
	}

	public String getErrorCode() {
		return ec;
	}

	public void setProposedSize(long proposedSize) {
		this.proposedSize = proposedSize;
	}

	public long getProposedSize() {
		return proposedSize;
	}
}