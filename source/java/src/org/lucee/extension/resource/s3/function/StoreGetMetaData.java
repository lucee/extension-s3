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

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Struct;

public class StoreGetMetaData extends S3Function {

	private static final long serialVersionUID = -5755984801477439480L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		if(args.length!=1)
			throw engine.getExceptionUtil().createFunctionException(pc, "StoreGetMetaData", 1, 1, args.length);
		return call(pc, engine.getCastUtil().toString(args[0]));
	}
	
	public static Struct call(PageContext pc , String url) throws PageException {
		try {
			return _call(pc, url);
		} catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}

	public static Struct _call(PageContext pc , String url) throws PageException {
		S3Resource res=toS3Resource(pc,url,"StoreGetMetaData");
		return res.getMetaData();
	}
	

	
	
}