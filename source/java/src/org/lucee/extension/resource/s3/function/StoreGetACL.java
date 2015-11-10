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
import java.util.Iterator;
import java.util.List;

import org.lucee.extension.resource.s3.AccessControl;
import org.lucee.extension.resource.s3.S3Exception;
import org.lucee.extension.resource.s3.S3Resource;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;

public class StoreGetACL extends S3Function {

	private static final long serialVersionUID = 7574286395500781525L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		if(args.length!=1)
			throw engine.getExceptionUtil().createFunctionException(pc, "StoreGetACL", 1, 1, args.length);
		return call(pc, engine.getCastUtil().toString(args[0]));
	}
	
	public static Object call(PageContext pc , String url) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		S3Resource res=toS3Resource(pc,url,"StoreGetACL");
		try {
			return toArrayStruct(engine,res.getAccessControlPolicy().getAccessControlList());
		} catch (IOException e) {
			throw engine.getCastUtil().toPageException(e);
		}
    }

	private static Object toArrayStruct(CFMLEngine engine,List<AccessControl> accessControlList) throws S3Exception {
		Array arr=engine.getCreationUtil().createArray();
		String type;
		Struct sct;
		AccessControl ac;
		Iterator<AccessControl> it = accessControlList.iterator();
		while(it.hasNext()){
			ac=it.next();
			arr.appendEL(sct=engine.getCreationUtil().createStruct());
			sct.setEL("id", ac.getId());
			sct.setEL("permission", ac.getPermission());
			
			type = AccessControl.toType(ac.getType());
			if("Group".equalsIgnoreCase(type)) 				
				setGroup(sct,ac);
			else if("CanonicalUser".equalsIgnoreCase(type)) 
				sct.setEL("displayName", ac.getDisplayName());
			else 
				sct.setEL("email", ac.getId());
		}
		return arr;
	}
	
	private static void setGroup(Struct sct, AccessControl ac) {
		String uri = ac.getUri();
		sct.setEL("id", uri);
		if("http://acs.amazonaws.com/groups/global/AllUsers".equalsIgnoreCase(uri))
			sct.setEL("group", "all");
		else if("http://acs.amazonaws.com/groups/global/AuthenticatedUsers".equalsIgnoreCase(uri))
			sct.setEL("group", "authenticated");
		else if("http://acs.amazonaws.com/groups/s3/LogDelivery".equalsIgnoreCase(uri))
			sct.setEL("group", "log_delivery");
	}

}