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

import lucee.loader.engine.CFMLEngine;

/**
 * Factory creating S3ResourceCopyMove instances, only used when the interface ResourceCopyMove
 * exists, see S3ResourceFactory.
 */
final class S3ResourceCopyMoveFactory extends S3ResourceFactory {

	@Override
	S3Resource create(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String buckedName, String objectName) {
		return new S3ResourceCopyMove(engine, s3, props, location, provider, buckedName, objectName);
	}

	@Override
	S3Resource create(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String path) {
		return new S3ResourceCopyMove(engine, s3, props, location, provider, path);
	}
}
