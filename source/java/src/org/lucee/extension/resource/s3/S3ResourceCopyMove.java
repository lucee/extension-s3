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

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceCopyMove;
import lucee.commons.io.res.filter.ResourceFilter;
import lucee.loader.engine.CFMLEngine;

/**
 * An S3Resource that can copy and move whole directories with a minimum of requests, instead of
 * Lucee doing this object by object. This class implements an interface that only exists in newer
 * versions of Lucee, it must never be loaded directly, only by way of S3ResourceFactory.
 */
final class S3ResourceCopyMove extends S3Resource implements ResourceCopyMove {

	private static final long serialVersionUID = 4003004003004003L;

	S3ResourceCopyMove(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String buckedName, String objectName) {
		super(engine, s3, props, location, provider, buckedName, objectName);
	}

	S3ResourceCopyMove(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String path) {
		super(engine, s3, props, location, provider, path);
	}

	@Override
	public boolean copyDirectory(Resource target, ResourceFilter filter) throws IOException {
		return copyDirectoryTo(target, filter);
	}

	@Override
	public boolean moveDirectory(Resource target) throws IOException {
		return moveDirectoryTo(target);
	}
}
