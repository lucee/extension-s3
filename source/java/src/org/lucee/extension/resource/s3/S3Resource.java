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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceProvider;
import lucee.commons.io.res.type.s3.S3Constants;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;

import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.StorageObject;
import org.lucee.extension.resource.ResourceSupport;
import org.lucee.extension.resource.s3.info.S3Info;
import org.xml.sax.SAXException;

public final class S3Resource extends ResourceSupport {

	private static final long serialVersionUID = 2265457088552587701L;

	private static final long FUTURE=50000000000000L;
	
	private static final S3Info UNDEFINED=new Dummy("undefined",0,0,false,false,false,false);
	private static final S3Info ROOT=new Dummy("root",0,0,true,false,true,false);
	private static final S3Info LOCKED = new Dummy("locked",0,0,true,false,false,false);
	private static final S3Info UNDEFINED_WITH_CHILDREN = new Dummy("undefined with children 1",0,0,true,false,true,false);
	private static final S3Info UNDEFINED_WITH_CHILDREN2 = new Dummy("undefined with children 2",0,0,true,false,true,false);


	private final S3ResourceProvider provider;
	private final String bucketName;
	private String objectName;
	private final S3 s3;
	long infoLastAccess=0;
	private int storage=S3Constants.STORAGE_UNKNOW;
	private int acl=S3Constants.ACL_PUBLIC_READ;

	private S3Resource(CFMLEngine engine,S3 s3,int storage, S3ResourceProvider provider, String buckedName,String objectName) {
		super(engine);
		this.s3=s3;
		this.provider=provider;
		this.bucketName=buckedName;
		this.objectName=objectName;
		this.storage=storage;
	}
	

	S3Resource(CFMLEngine engine,S3 s3,int storage, S3ResourceProvider provider, String path) {
		super(engine);
		this.s3=s3;
		this.provider=provider;
		
		if(path.equals("/") || engine.getStringUtil().isEmpty(path,true)) {
			this.bucketName=null;
			this.objectName="";
		}
		else {
			path=engine.getResourceUtil().translatePath(path, true, false);
			String[] arr=null;
			try {
				arr = engine.getListUtil().toStringArray( engine.getListUtil().toArrayRemoveEmpty(path,"/"));
			} catch (PageException e) {
				// that should never happen, because we have string as base!!!
			}
			bucketName=arr[0];
			for(int i=1;i<arr.length;i++) {
				if(Util.isEmpty(objectName))objectName=arr[i];
				else objectName+="/"+arr[i];
			}
			if(objectName==null)objectName="";
		}
		this.storage=storage;
		
	}

	@Override
	public void createDirectory(boolean createParentWhenNotExists) throws IOException {
		engine.getResourceUtil().checkCreateDirectoryOK(this, createParentWhenNotExists);
		try {
			provider.lock(this);
			if(isBucket()) {
				s3.createDirectory(bucketName, acl,storage);
			}
			else s3.createDirectory(bucketName, objectName+"/", acl,storage);	
		}
		catch (IOException ioe) {
			throw ioe;
		}
		finally {
			provider.unlock(this);
		}
		s3.releaseCache(getInnerPath());
	}

	@Override
	public void createFile(boolean createParentWhenNotExists) throws IOException {
		engine.getResourceUtil().checkCreateFileOK(this, createParentWhenNotExists);
		if(isBucket()) throw new IOException("can't create file ["+getPath()+"], on this level (Bucket Level) you can only create directories");
		try {
			provider.lock(this);
			s3.createFile(bucketName, objectName, acl, storage);
		}
		finally {
			provider.unlock(this);
		}
		s3.releaseCache(getInnerPath());
	}

	@Override
	public boolean exists() {
		
		return getInfo()
			.exists();
	}

	@Override
	public InputStream getInputStream() throws IOException {
		engine.getResourceUtil().checkGetInputStreamOK(this);
		provider.read(this);
		try {
			return engine.getIOUtil().toBufferedInputStream(s3.getInputStream(bucketName, objectName));
		} 
		catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}

	@Override
	public int getMode() {
		return 777;
	}

	@Override
	public String getName() {
		if(isRoot()) return "";
		if(isBucket()) return bucketName;
		return objectName.substring(objectName.lastIndexOf('/')+1);
	}

	@Override
	public boolean isAbsolute() {
		return true;
	}
	

	@Override
	public String getPath() {
		return getPrefix().concat(getInnerPath());
	}
	
	private String getPrefix()  {
		
		String aki=s3.getAccessKeyId();
		String sak=s3.getSecretAccessKey();
		
		StringBuilder sb=new StringBuilder(provider.getScheme()).append("://");
		boolean doHost=!s3.getHost().equals(S3.DEFAULT_HOST) && s3.getHost().length()>0;
		
		if(s3.getCustomCredentials() && !engine.getStringUtil().isEmpty(aki)){
			sb.append(aki);
			if(!engine.getStringUtil().isEmpty(sak)){
				sb.append(":").append(sak);
				if(storage!=S3Constants.STORAGE_UNKNOW){
					sb.append(":").append(S3.toLocation(storage,"us"));
				}
			}
			sb.append("@");
		}
		if(doHost) sb.append(s3.getHost());
		
		return sb.toString();
	}


	@Override
	public String getParent() {
		if(isRoot()) return null;
		return getPrefix().concat(getInnerParent());
	}
	
	private String getInnerPath() {
		if(isRoot()) return "/";
		return engine.getResourceUtil().translatePath(bucketName+"/"+objectName, true, false);
	}
	
	private String getInnerParent() {
		if(isRoot()) return null;
		if(Util.isEmpty(objectName)) return "/";
		if(objectName.indexOf('/')==-1) return "/"+bucketName;
		String tmp=objectName.substring(0,objectName.lastIndexOf('/'));
		return engine.getResourceUtil().translatePath(bucketName+"/"+tmp, true, false);
	}

	@Override
	public Resource getParentResource() {
		if(isRoot()) return null;
		return new S3Resource(engine,s3,isBucket()?S3Constants.STORAGE_UNKNOW:storage,provider,getInnerParent());// MUST direkter machen
	}

	public boolean isRoot() {
		return bucketName==null;
	}
	
	public boolean isBucket() {
		return bucketName!=null && Util.isEmpty(objectName);
	}

	@Override
	public String toString() {
		return getPath();
	}
	
	@Override
	public OutputStream getOutputStream(boolean append) throws IOException {

		engine.getResourceUtil().checkGetOutputStreamOK(this);
		//provider.lock(this);
		
		try {
			byte[] barr = null;
			if(append){
				InputStream is=null;
				OutputStream os=null;
				try{
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					os=baos;
					engine.getIOUtil().copy(is=getInputStream(), baos,false,false);
					barr=baos.toByteArray();
				}
				catch (Exception e) {
					throw engine.getExceptionUtil().createPageRuntimeException(engine.getCastUtil().toPageException(e));
				}
				finally{
					Util.closeEL(is);
					Util.closeEL(os);
				}
			}
			S3ResourceOutputStream os = new S3ResourceOutputStream(s3,bucketName,objectName,getInnerPath(),acl,storage);
			if(append && !(barr==null || barr.length==0))
				engine.getIOUtil().copy(new ByteArrayInputStream(barr),os,true,false);
			return os;
		}
		catch(IOException e) {
			throw e;
		}
		catch (Exception e) {
			throw engine.getExceptionUtil().createPageRuntimeException(engine.getCastUtil().toPageException(e));
		}
		finally {
			s3.releaseCache(getInnerPath());
		}
	}

	@Override
	public Resource getRealResource(String realpath) {
		realpath=engine.getResourceUtil().merge(getInnerPath(), realpath);
		if(realpath.startsWith("../"))return null;
		return new S3Resource(engine,s3,S3Constants.STORAGE_UNKNOW,provider,realpath);
	}

	@Override
	public ResourceProvider getResourceProvider() {
		return provider;
	}

	@Override
	public boolean isDirectory() {
		return getInfo().isDirectory();
	}

	@Override
	public boolean isFile() {
		return getInfo().isFile();
	}

	@Override
	public boolean isReadable() {
		return exists();
	}

	@Override
	public boolean isWriteable() {
		return exists();
	}

	@Override
	public long lastModified() {
		return getInfo().getLastModified();
	}

	private S3Info getInfo() {
		S3Info info = s3.getInfo(getInnerPath());
		
		if(info==null) {// || System.currentTimeMillis()>infoLastAccess
			if(isRoot()) {
				try {
					s3.list(false);
					info=ROOT;
				}
				catch (Exception e) {
					info=UNDEFINED;
				}
				infoLastAccess=FUTURE;
			}
			else {
				try {
					provider.read(this);
				} catch (IOException e) {
					return LOCKED;
				}
				try {	
					if(isBucket()) {
						Iterator<S3Info> it = s3.list(false).iterator();
						String name=getName();
						S3Info tmp;
						while(it.hasNext()) {
							tmp=it.next();
							if(tmp.getBucketName().equals(name)) {
								info=tmp;
								infoLastAccess=System.currentTimeMillis()+provider.getCache();
								break;
							}
						}
					}
					else {
						String objectNameWithSlash=objectName.endsWith("/")?objectName:objectName+"/";
						/*Iterator<S3Info> it = s3.list(bucketName,objectName,true).iterator();
						
						S3Info tmp;
						String objectNameWithSlash=objectName.endsWith("/")?objectName:objectName+"/";
						while(it.hasNext()) {
							tmp=it.next();
							if(tmp.getObjectName()==null) continue;
							// exact match
							if(tmp.getObjectName().equals(objectName)) {
								
							}
								
							if(tmp.getObjectName().equals(objectName) || tmp.getObjectName().startsWith(objectNameWithSlash))
						}*/
						S3Info tmp = s3.get(bucketName, objectName);
						if(tmp!=null) {
							info=tmp;
							infoLastAccess=System.currentTimeMillis()+provider.getCache();
						}
						else if((tmp = s3.get(bucketName, objectNameWithSlash))!=null) {
							info=tmp;
							infoLastAccess=System.currentTimeMillis()+provider.getCache();
						}
						else if(s3.list(bucketName, objectNameWithSlash,true).size()>0) {
							info=UNDEFINED_WITH_CHILDREN;
							infoLastAccess=System.currentTimeMillis()+provider.getCache();
						}	
					}
					if(info==null){
						info=UNDEFINED;
						infoLastAccess=System.currentTimeMillis()+provider.getCache();
					}
				}
				catch(Exception t) {
					return UNDEFINED;
				}
			}
			s3.setInfo(getInnerPath(), info);
		}
		return info;
	}

	@Override
	public long length() {
		return getInfo().getSize();
	}

	@Override
	public Resource[] listResources() {
		S3Resource[] children=null;
		try {
			if(isRoot()) {
				List<S3Info> list = s3.list(false);
				Iterator<S3Info> it = list.iterator();
				children=new S3Resource[list.size()];
				S3Info si;
				int index=0;
				while(it.hasNext()) {
					si=it.next();
					children[index]=new S3Resource(engine,s3,storage,provider,si.getBucketName(),"");
					s3.setInfo(children[index].getInnerPath(),si);
					index++;
				}
			}
			else if(isDirectory()){
				List<S3Info> list = isBucket()?
						s3.list(bucketName,true):
						s3.list(bucketName, objectName+"/",true);
						
				ArrayList<S3Resource> tmp = new ArrayList<S3Resource>();
				String key,name,path;
				int index;
				Set<String> names=new LinkedHashSet<String>();
				Set<String> pathes=new LinkedHashSet<String>();
				S3Resource r;
				boolean isb=isBucket();
				Iterator<S3Info> it = list.iterator();
				S3Info si;
				while(it.hasNext()) {
					si=it.next();
					key=engine.getResourceUtil().translatePath(si.getObjectName(), false, false);
					index=key.indexOf('/',(objectName==null?0:objectName.length())+1);
					if(index==-1) { 
						name=key;
						path=null;
					}
					else {
						name=key.substring(index+1);
						path=key.substring(0,index);
					}
					
					//print.out("1:"+key);
					//print.out("path:"+path);
					//print.out("name:"+name);
					if(path==null){
						names.add(name);
						tmp.add(r=new S3Resource(engine,s3,storage,provider,si.getBucketName(),key));
						s3.setInfo(r.getInnerPath(),si);
					}
					else {
						pathes.add(path);
					}
				}
				
				Iterator<String> _it = pathes.iterator();
				while(_it.hasNext()) {
					path=_it.next();
					if(names.contains(path)) continue;
					tmp.add(r=new S3Resource(engine,s3,storage,provider,bucketName,path));
					s3.setInfo(r.getInnerPath(),UNDEFINED_WITH_CHILDREN2);
				}
				
				//if(tmp.size()==0 && !isDirectory()) return null;
				
				children=tmp.toArray(new S3Resource[tmp.size()]);
			}
		}
		catch(Exception t) {
			t.printStackTrace();
			return null;
		}
		return children;
	}

	@Override
	public void remove(boolean force) throws IOException {
		if(isRoot()) throw new IOException("can not remove root of S3 Service");
		engine.getResourceUtil().checkRemoveOK(this);
		try{	
			if(isBucket()) {
				s3.delete(bucketName,force);
			}
			else {
				s3.delete(bucketName,isDirectory()?objectName+"/":objectName,force);
			}
		}
		finally {
			s3.releaseCache(getInnerPath());
		}
		// TODO provider.lock(this);	
	}

	@Override
	public boolean setLastModified(long time) {
		s3.releaseCache(getInnerPath());
		// TODO 
		return false;
	}

	@Override
	public void setMode(int mode) throws IOException {
		s3.releaseCache(getInnerPath());
		// TODO 
		
	}

	@Override
	public boolean setReadable(boolean readable) {
		s3.releaseCache(getInnerPath());
		// TODO 
		return false;
	}

	@Override
	public boolean setWritable(boolean writable) {
		s3.releaseCache(getInnerPath());
		// TODO 
		return false;
	}


	public AccessControlPolicy getAccessControlPolicy() {
		String p = getInnerPath();
		try {
			AccessControlPolicy acp = s3.getACP(p);
			if(acp==null){
				acp=s3.getAccessControlPolicy(bucketName,  getObjectName());
				s3.setACP(p, acp);
			}
				
			
			return acp;
		} 
		catch (Exception e) {
			throw engine.getExceptionUtil().createPageRuntimeException(engine.getCastUtil().toPageException(e));
		}
	}
	
	public void setAccessControlPolicy(AccessControlPolicy acp) {
		
		try {
			s3.setAccessControlPolicy(bucketName, getObjectName(),acp);
		} 
		catch (Exception e) {
			throw engine.getExceptionUtil().createPageRuntimeException(engine.getCastUtil().toPageException(e));
		}
		finally {
			s3.releaseCache(getInnerPath());
		}
	}
	
	private String getObjectName() {
		if(!engine.getStringUtil().isEmpty(objectName) && isDirectory()) {
			return objectName+"/";
		}
		return objectName;
	}


	public void setACL(int acl) {
		this.acl=acl;
	}


	public void setStorage(int storage) {
		this.storage=storage;
	}
	
	public void setStorage(String storage) throws S3Exception {
		this.storage=S3.toIntStorage(storage);
	}


	@Override
	public void copyFrom(Resource res, boolean append) throws IOException {
		copy(res, this, append);
	}


	@Override
	public void copyTo(Resource res, boolean append) throws IOException {
		copy(this, res, append);
	}
	
	private void copy(Resource from, Resource to, boolean append) throws IOException {
		try {
			if(from instanceof S3Resource || to instanceof S3Resource) {
				S3Resource f=(S3Resource) from;
				S3Resource t=(S3Resource) to;
				// whe have the same container
				if(f.s3.getAccessKeyId().equals(t.s3.getAccessKeyId()) && f.s3.getSecretAccessKey().equals(t.s3.getSecretAccessKey())) {
					s3.copy(f.bucketName,f.objectName,t.bucketName,t.objectName);
					return;
				}
				
			}
			super.copyTo(to, append);
		}
		finally {
			s3.releaseCache(getInnerPath());
		}
	}


	
	
	
	
	@Override
	public void moveTo(Resource dest) throws IOException {
		try {
			if(dest instanceof S3Resource) {
				S3Resource other=(S3Resource) dest;
				// whe have the same container
				if(other.s3.getAccessKeyId().equals(s3.getAccessKeyId()) && other.s3.getSecretAccessKey().equals(s3.getSecretAccessKey())) {
					s3.move(bucketName,objectName,other.bucketName,other.objectName);
					return;
				}
				
			}
			super.moveTo(dest);
		}
		finally {
			s3.releaseCache(getInnerPath());
		}
	}
	


}


 class Dummy implements S3Info {

		private long lastModified;
		private long size;
		private boolean exists;
		private boolean file;
		private boolean directory;
		private String label;
		private boolean bucket;
	
	 
	public Dummy(String label,long lastModified, long size, boolean exists,boolean file, boolean directory, boolean bucket) {
		this.label = label;
		this.lastModified = lastModified;
		this.size = size;
		this.exists = exists;
		this.file = file;
		this.directory = directory;
		this.bucket=bucket;
	}


	@Override
	public long getLastModified() {
		return lastModified;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String toString() {
		return "Dummy:"+getLabel();
	}


	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}


	@Override
	public boolean exists() {
		return exists;
	}

	@Override
	public boolean isDirectory() {
		return directory;
	}

	@Override
	public boolean isFile() {
		return file;
	}


	@Override
	public String getObjectName() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public String getBucketName() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean isBucket() {
		return bucket;
	}

}