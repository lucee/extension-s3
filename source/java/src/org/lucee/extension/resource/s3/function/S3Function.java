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

import org.lucee.extension.resource.s3.S3Resource;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceProvider;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.ext.function.BIF;

public abstract class S3Function extends BIF {

	public static S3Resource toS3Resource(PageContext pc, String url, String functionName) throws PageException {
		Resource res=CFMLEngineFactory.getInstance().getResourceUtil().toResourceNotExisting(pc, url);
		ResourceProvider provider = res.getResourceProvider();
		if(!provider.getScheme().equalsIgnoreCase("s3") || !res.exists()) 
			throw CFMLEngineFactory.getInstance().getExceptionUtil()
			.createFunctionException(pc,functionName,1,"url","file ["+url+"] does not exist.",null);
		
		return (S3Resource) res;
	}
}