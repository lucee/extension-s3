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
package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3AddACL extends S3Function {

	private static final long serialVersionUID = -3487196822940881552L;

	@Override
	public Object invoke(final PageContext pc, final Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		if (args.length > 7 || args.length < 3) throw eng.getExceptionUtil().createFunctionException(pc, "S3AddACL", 3, 7, args.length);

		// required
		String bucketName = cast.toString(args[0]);
		String objectName = cast.toString(args[1]);
		Object objACL = args[2];
		if (isEmpty(objectName)) objectName = null;

		// optional
		String accessKeyId = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		String secretAccessKey = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String host = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		double timeout = args.length > 6 && !isEmpty(args[6]) ? cast.toDoubleValue(args[6]) : 0;

		try {
			// create S3 Instance
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			s3.addAccessControlList(bucketName, objectName, objACL);
			return null;
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}
}