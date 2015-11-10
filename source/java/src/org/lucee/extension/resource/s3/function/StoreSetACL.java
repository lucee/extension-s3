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
package org.lucee.extension.resource.s3.function;

import java.io.IOException;
import java.util.List;

import org.lucee.extension.resource.s3.AccessControl;
import org.lucee.extension.resource.s3.AccessControlPolicy;
import org.lucee.extension.resource.s3.S3Resource;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;

public class StoreSetACL extends S3Function {

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		if(args.length!=2)
			throw engine.getExceptionUtil().createFunctionException(pc, "StoreSetACL", 2, 2, args.length);
		return call(pc, engine.getCastUtil().toString(args[0]), args[1]);
	}
	
	public static String call(PageContext pc , String url, Object objACL) throws PageException {
		try {
			return _call(pc, url, objACL);
		} catch (IOException e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}

	public static String _call(PageContext pc , String url, Object objACL) throws PageException, IOException {
		S3Resource res=toS3Resource(pc,url,"StoreSetACL");
		invoke(res, objACL);
		return null;
	}
	
	public static void invoke(S3Resource res, Object objACL) throws PageException, IOException {
		AccessControlPolicy acp = res.getAccessControlPolicy();
		List<AccessControl> acl = AccessControl.toAccessControlList(objACL);
		AccessControlPolicy.removeDuplicates(acl);
		acp.setAccessControlList(acl);
		res.setAccessControlPolicy(acp);
	}
	
	
}