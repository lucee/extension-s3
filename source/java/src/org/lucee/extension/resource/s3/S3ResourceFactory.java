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
 * Creates the S3Resource instances. The core Lucee interface ResourceCopyMove (for copying and
 * moving whole directories) only exists in newer versions of Lucee, but this extension has to work
 * with older versions as well. A class implementing an interface that does not exist cannot be
 * loaded, so the classes using the interface (S3ResourceCopyMove and S3ResourceCopyMoveFactory) are
 * only touched when the interface is available, with older versions of Lucee plain S3Resource
 * instances are used.
 */
abstract class S3ResourceFactory {

	private static final String RESOURCE_COPY_MOVE = "lucee.commons.io.res.ResourceCopyMove";

	static final S3ResourceFactory INSTANCE = createInstance();

	abstract S3Resource create(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String buckedName, String objectName);

	abstract S3Resource create(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String path);

	private static S3ResourceFactory createInstance() {
		if (isCopyMoveAvailable()) return new S3ResourceCopyMoveFactory();
		return new S3ResourceFactory() {
			@Override
			S3Resource create(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String buckedName, String objectName) {
				return new S3Resource(engine, s3, props, location, provider, buckedName, objectName);
			}

			@Override
			S3Resource create(CFMLEngine engine, S3 s3, S3Properties props, String location, S3ResourceProvider provider, String path) {
				return new S3Resource(engine, s3, props, location, provider, path);
			}
		};
	}

	/**
	 * does the Lucee we are running in know the interface ResourceCopyMove
	 */
	static boolean isCopyMoveAvailable() {
		try {
			Class.forName(RESOURCE_COPY_MOVE, false, S3ResourceFactory.class.getClassLoader());
			return true;
		}
		catch (ClassNotFoundException cnfe) {
			// not an error, that is simply a version of Lucee without that interface
			return false;
		}
	}
}
