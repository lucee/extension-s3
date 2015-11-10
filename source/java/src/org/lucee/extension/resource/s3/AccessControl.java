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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Strings;

public class AccessControl {

	public static final short TYPE_GROUP=1;
	public static final short TYPE_EMAIL=2;
	public static final short TYPE_CANONICAL_USER=4;
	
	private String id;
	private String displayName;
	private String permission;
	private String uri;
	private short type;
	private String email;
	
	/**
	 * @return the type
	 */
	public short getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(short type) {
		this.type = type;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the displayName
	 */
	public String getDisplayName() {
		return displayName;
	}
	/**
	 * @param displayName the displayName to set
	 */
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	/**
	 * @return the permission
	 */
	public String getPermission() {
		return permission;
	}
	
	public void setPermission(String permission) {
		this.permission=permission;
	}
	
	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}
	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}
	

	
	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @param email the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}
	

	@Override
	public String toString(){
		return "displayName:"+displayName+";email:"+email+";id:"+id+";permission:"+permission+";type:"+type+";uri:"+uri;
	}

	public String hash() {
		try {
			return CFMLEngineFactory.getInstance().getSystemUtil().hashMd5(toString());
		} catch (IOException e) {
			return null;
		}
	}
	@Override
	public int hashCode() {
		return hash().hashCode();
	}
	
	
	
	
	
	
	

	public static List<AccessControl> toAccessControlList(Object objACL) throws S3Exception, PageException {
		Cast caster = CFMLEngineFactory.getInstance().getCastUtil();
		Array arr = caster.toArray(objACL,null);
		if(arr==null)
			throw new S3Exception("ACL Object must be a Array of Structs");
		
		Struct sct;
		Iterator<Object> it = arr.valueIterator();
		List<AccessControl> acl=new ArrayList<AccessControl>();
		while(it.hasNext()){
			sct=caster.toStruct(it.next(), null);
			if(sct==null)
				throw new S3Exception("ACL Object must be a Array of Structs");
			acl.add(toAccessControl(sct));
		}
		return acl;
	}
	
	
	public static AccessControl toAccessControl(Struct sct) throws S3Exception, PageException{
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		AccessControl ac=new AccessControl();
        ac.setPermission(AccessControl.toPermission(sct.get(caster.toKey("permission"),null)));
        
        
        
        // Email
        String email = CFMLEngineFactory.getInstance().getCastUtil().toString(sct.get(caster.toKey("email"),null),null);
        if(!engine.getStringUtil().isEmpty(email)){
        	ac.setType(AccessControl.TYPE_EMAIL);
        	ac.setEmail(email);
        	return ac;
        }
        
        // Group
        String uri=AccessControl.groupToURI(sct.get(caster.toKey("group"),null));
        if(!engine.getStringUtil().isEmpty(uri)) {
        	ac.setType(AccessControl.TYPE_GROUP);
        	ac.setUri(uri);
        	return ac;
        }
        
        // Canonical
        String id = engine.getCastUtil().toString(sct.get(caster.toKey("id")),null);
        String displayName = engine.getCastUtil().toString(sct.get(caster.toKey("displayName")),null);
        if(engine.getStringUtil().isEmpty(id)) 
        	throw new S3Exception("missing id for Canonical User defintion");
        
        ac.setType(AccessControl.TYPE_CANONICAL_USER);
        ac.setId(id);
        ac.setDisplayName(displayName);
        
        return ac;
	}
	
	
	public static String toPermission(Object oPermission) throws S3Exception {
		String permission=CFMLEngineFactory.getInstance().getCastUtil().toString(oPermission,null);
		if(CFMLEngineFactory.getInstance().getStringUtil().isEmpty(permission,true))
			throw new S3Exception("missing permission definition");
		
		permission=permission.toUpperCase().trim();
		permission=AccessControl.removeWordDelimter(permission);
		
		if("FULLCONTROL".equals(permission))
			return "FULL_CONTROL";
		else if("WRITEACP".equals(permission))
			return "WRITE_ACP";
		else if("READACP".equals(permission))
			return "READ_ACP";
		else if("WRITE".equals(permission))
			return "WRITE";
		else if("READ".equals(permission))
			return "READ";
		else
			throw new S3Exception("invalid permission definition ["+permission+"], valid permissions are [FULL_CONTROL, WRITE, WRITE_ACP, READ, READ_ACP]");
	}
	

	public static String groupToURI(Object oGroup) throws S3Exception {
		if(oGroup!=null && (!(oGroup instanceof String) || !CFMLEngineFactory.getInstance().getStringUtil().isEmpty((String)oGroup))) {
			String group=CFMLEngineFactory.getInstance().getCastUtil().toString(oGroup,null);
			if(group==null)
				throw new S3Exception("invalid object type for group definition");
            
			group=removeWordDelimter(group);
			if("all".equalsIgnoreCase(group))
                return "http://acs.amazonaws.com/groups/global/AllUsers";
			if("authenticated".equalsIgnoreCase(group) || "AuthenticatedUser".equalsIgnoreCase(group) || "AuthenticatedUsers".equalsIgnoreCase(group))
                return "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
			if("logdelivery".equalsIgnoreCase(group))
                return "http://acs.amazonaws.com/groups/s3/LogDelivery";
			throw new S3Exception("invalid group definition ["+group+"], valid group defintions are are [all,authenticated,log_delivery]");
                
        }
		return null;
	}
	

	public static String toType(short type) throws S3Exception {
		String rtn = toType(type, null);
		if(rtn!=null) return rtn;
		throw new S3Exception("invalid type defintion");
	}
	
	public static String toType(short type, String defaultValue) {
		switch(type){
		case TYPE_EMAIL: return "AmazonCustomerByEmail";
		case TYPE_GROUP: return "Group";
		case TYPE_CANONICAL_USER: return "CanonicalUser";
		}
		return defaultValue;
	}

	public static short toType(String type) throws S3Exception {
		short rtn = toType(type, (short)-1);
		if(rtn!=-1) return rtn;
		
		throw new S3Exception("invalid type defintion ["+type+"], valid types are [Email,Group,CanonicalUser]");
	}
	
	public static short toType(String type, short defaultValue) {
		type=removeWordDelimter(type);
		if("Email".equalsIgnoreCase(type)) return TYPE_EMAIL;
		if("AmazonCustomerByEmail".equalsIgnoreCase(type)) return TYPE_EMAIL;
		if("CanonicalUser".equalsIgnoreCase(type)) return TYPE_CANONICAL_USER;
		if("Group".equalsIgnoreCase(type)) return TYPE_GROUP;
		
		return defaultValue;
	}
	

	
	private static String removeWordDelimter(String str) {
		Strings util = CFMLEngineFactory.getInstance().getStringUtil();
		str=util.replace(str,"_", "", false,false);
		str=util.replace(str,"-", "", false,false);
		str=util.replace(str," ", "", false,false);
		return str;
	}
}