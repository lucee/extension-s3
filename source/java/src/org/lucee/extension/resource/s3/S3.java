package org.lucee.extension.resource.s3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javafx.css.PseudoClass;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Decision;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.model.StorageOwner;
import org.jets3t.service.model.container.ObjectKeyAndVersion;
import org.jets3t.service.security.AWSCredentials;
import org.lucee.extension.resource.s3.info.ParentObject;
import org.lucee.extension.resource.s3.info.S3BucketWrapper;
import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.info.StorageObjectWrapper;

public class S3 {
	
	public static final String DEFAULT_HOST="s3.amazonaws.com";
	
	private final String host;
	private final String secretAccessKey;
	private final String accessKeyId;
	private final boolean customCredentials;
	private final long timeout;
	
	private RestS3Service service;
	//private Map<String,S3Info> info=new HashMap<String,S3Info>();
	//private Map<String,Array> acl=new HashMap<String,Array>();


	
/////////////////////// CACHE ////////////////
	private ValidUntilMap<S3BucketWrapper> buckets;
	private final Map<String,ValidUntilMap<StorageObjectWrapper>> bucketObjects=
			new HashMap<String, ValidUntilMap<StorageObjectWrapper>>(); 
	
	Map<String,ValidUntilAccessControlList> accessControlLists=new HashMap<String,ValidUntilAccessControlList>();
	
/////////////////////////////////////////////

	
	public S3(S3Properties props, long timeout) {
		this.host=props.getHost();
		this.secretAccessKey=props.getSecretAccessKey();
		this.accessKeyId=props.getAccessKeyId();
		this.timeout=timeout;
		this.customCredentials=props.getCustomCredentials();
		//new Throwable().printStackTrace();
	}

	public String getHost() {
		return host;
	}

	private void flush() {
		if(service==null) return;
		try {
			service.shutdown();
		} catch (ServiceException e) {}
		service=null;
	}


	public String getAccessKeyId() {
		return accessKeyId;
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}



	
	public S3Bucket createDirectory(String bucketName, String acl, String location) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		String l = improveLocation(location);
		AccessControlList a = toACL(acl,null);
		try{
			S3Bucket bucket;
			if(!Util.isEmpty(l,true)) {
				if(a!=null) bucket = getS3Service().createBucket(bucketName,l, a);
				else bucket = getS3Service().createBucket(bucketName,l);
			}
			else bucket = getS3Service().createBucket(bucketName);
			cacheSet(bucketName,bucket);
			return bucket;
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
	}

	/**
	 * 
	 * @param bucketName name of the bucket
	 * @param objectName name of the object
	 * @param acl 
	 * @param storage only used when creating a non existing bucket
	 * @throws IOException
	 */
	public void createDirectory(String bucketName, String objectName, String acl, String location) throws S3Exception {
		if(Util.isEmpty(objectName)) {
			createDirectory(bucketName, acl, location);
			return;
		}
		
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName, true);
		// TODO update bucket objects
		AccessControlList a = toACL(acl,null);
		
		S3Object object = new S3Object("object");
		if(a!=null)object.setAcl(a);
		
		objectName=improveObjectName(objectName, true);
		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			S3Object so = getS3Service().putObject(bucketName, object);
			cacheSet(bucketName, objectName, so);
		}
		catch (ServiceException se) {
			if(get(bucketName)!=null)throw toS3Exception(se);
			
			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				S3Object so = getS3Service().putObject(bucket, object);
				cacheSet(bucketName, objectName, so);
			} 
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}
	
	public void createFile(String bucketName, String objectName, String acl, String location) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName, false);
		
		
		AccessControlList a = toACL(acl,null);
		
		S3Object object = new S3Object("object");
		if(a!=null)object.setAcl(a);
		
		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			S3Object so = getS3Service().putObject(bucketName, object);
			cacheSet(bucketName, objectName, so);
		}
		catch (ServiceException se) {
			if(get(bucketName)!=null)throw toS3Exception(se);
			
			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				S3Object so = getS3Service().putObject(bucket, object);
				cacheSet(bucketName, objectName, so);
			} 
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}
	
	public S3Object getData(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		try {
			return getS3Service().getObject(bucketName, objectName);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	
	public InputStream getInputStream(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		
		try {
			return getData(bucketName, objectName).getDataInputStream();
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}


	public StorageObject getInfo(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		ValidUntilMap<StorageObjectWrapper> objects = bucketObjects.get(bucketName);
		if(objects!=null){
			StorageObjectWrapper obj = objects.get(objectName);
			if(obj!=null && obj.validUntil()>=System.currentTimeMillis())
				return obj.getStorageObject();
		}
		try {
			StorageObject so = getS3Service().getObjectDetails(bucketName, objectName);
			if(objects!=null) objects.put(objectName, new StorageObjectWrapper(this, so, bucketName,System.currentTimeMillis()+timeout));
			return so;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}


	public List<S3Info> list(boolean recursive, boolean listPseudoFolder) throws S3Exception {
		try {
			
			// no cache for buckets
			if(buckets==null || buckets.validUntil<System.currentTimeMillis()) {
				S3Bucket[] s3buckets = getS3Service().listAllBuckets();
				long now=System.currentTimeMillis();
				buckets=new ValidUntilMap<S3BucketWrapper>(now+timeout);
				for(int i=0;i<s3buckets.length;i++){
					buckets.put(s3buckets[i].getName(),new S3BucketWrapper(s3buckets[i],now+timeout));
				}
			}
			
			List<S3Info> list=new ArrayList<S3Info>();
			Iterator<S3BucketWrapper> it = buckets.values().iterator();
			S3Info info;
			while(it.hasNext()){
				info=it.next();
				list.add(info);
				if(recursive){
					Iterator<S3Info> iit = _list("",info.getBucketName(), recursive, listPseudoFolder);
					while(iit.hasNext()){
						list.add(iit.next());
					}
				}
			}
			return list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}

	/**
	 * list all allements in a specific bucket
	 * @param bucketName name of the bucket
	 * @param recursive show all objects (recursive==true) or direct kids
	 * @param listPseudoFolder if recursive false also list the "folder" of objects with sub folders
	 * @return
	 * @throws S3Exception
	 */
	public List<S3Info> list(String bucketName, boolean recursive, boolean listPseudoFolder) throws S3Exception {
		List<S3Info> list=new ArrayList<S3Info>();
		Iterator<S3Info> iit = _list("",bucketName,recursive,listPseudoFolder);
		while(iit.hasNext()){
			list.add(iit.next());
		}
		return list;
	}
	
	
	public Iterator<S3Info> _list(String prefix,String bucketName, boolean recursive, boolean listPseudoFolder) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		//if(Util.isEmpty(bucketName)) return list(recursive,listPseudoFolder);

		ValidUntilMap<StorageObjectWrapper> objects = __list(bucketName);
		Iterator<StorageObjectWrapper> it = objects.values().iterator();
		
		Map<String,S3Info> map=new LinkedHashMap<String,S3Info>();
		S3Info info;
		while(it.hasNext()){
			info=it.next();
			add(map,info,prefix,recursive,listPseudoFolder);
		}
		
		return map.values().iterator();
	}
	
	
	private void add(Map<String,S3Info> map, S3Info info, String prefix, boolean recursive, boolean addPseudoEntries) throws S3Exception {
		String name=improveObjectName(info.getObjectName(), false);
		int index,last=0;
		S3Info tmp;
		if(addPseudoEntries) {
			while((index=name.indexOf('/',last))!=-1){
				tmp=new ParentObject(name.substring(0, index+1), info);
				if(!isParent(prefix,tmp.getObjectName()) && (recursive || isDirectKid(tmp.getObjectName(),prefix)))
					map.put(tmp.getObjectName(),tmp);
				last=index+1;
			}
		}
		if(!isParent(prefix,info.getObjectName()) && (recursive || isDirectKid(info.getObjectName(),prefix)))
			map.put(info.getObjectName(),info);
	}

	private boolean isParent(String prefix, String objectName) throws S3Exception {
		if(prefix.length()==0) return false;
		prefix=improveObjectName(prefix, true);
		objectName=improveObjectName(objectName, true);
		
		
		return !(objectName.length()>prefix.length() &&  objectName.startsWith(prefix));
	}

	private static boolean isDirectKid(String name, String prefix) throws S3Exception {
		prefix=prefix.length()==0?"":improveObjectName(prefix, true);
		String sub=improveObjectName(name.substring(prefix.length()),false);
		return sub.indexOf('/')==-1;
	}
	
	private ValidUntilMap<StorageObjectWrapper> __list(String bucketName) throws S3Exception {
		try {
			
			// not cached 
			ValidUntilMap<StorageObjectWrapper> _list = bucketObjects.get(bucketName);
			if(_list==null || _list.validUntil<System.currentTimeMillis()) {
				S3Object[] kids = getS3Service().listObjects(bucketName);
				long validUntil=System.currentTimeMillis()+timeout;
				_list=new ValidUntilMap<StorageObjectWrapper>(validUntil);
				bucketObjects.put(bucketName, _list);
				for(int i=0;i<kids.length;i++){
					_list.put(kids[i].getKey(),new StorageObjectWrapper(this,kids[i],bucketName,validUntil));
				}
			}
			return _list;
			
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	
	/*private void _list(List<S3Info> list,String bucketName, boolean recursive, ObjectFilter filter) throws S3Exception {
		try {
			
			// not cached 
			ValidUntilMap<StorageObjectWrapper> _list = bucketObjects.get(bucketName);
			if(_list==null || _list.validUntil<System.currentTimeMillis()) {
				S3Object[] kids = getS3Service().listObjects(bucketName);
				long validUntil=System.currentTimeMillis()+timeout;
				_list=new ValidUntilMap<StorageObjectWrapper>(validUntil);
				bucketObjects.put(bucketName, _list);
				for(int i=0;i<kids.length;i++){
					_list.put(kids[i].getKey(),new StorageObjectWrapper(this,kids[i],validUntil));
				}
			}
			
			String name;
			int index;
			Iterator<StorageObjectWrapper> it = _list.values().iterator();
			S3Info info;
			while(it.hasNext()){
				info=it.next();
				if(!recursive) {
					name=info.getObjectName();
					index=name.indexOf('/');
					if(index!=-1 && index+1<name.length()) continue;
				}
				if(filter==null || filter.accept(info.getObjectName()))
					list.add(info);
			}
			
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	public List<S3Info> list(String bucketName, String objectName,boolean recursive) throws S3Exception {
		if(Util.isEmpty(objectName)) return list(bucketName,recursive);
		
		bucketName=improveBucketName(bucketName);
		
		List<S3Info> list=new ArrayList<S3Info>();
		_list(list, bucketName, true,new ObjectFilter(objectName,recursive));
		return list;
	}*/
	
	public List<S3Info> list(String bucketName, String objectName, boolean recursive, boolean listPseudoFolder) throws S3Exception {
		List<S3Info> list=new ArrayList<S3Info>();
		Iterator<S3Info> iit = _list(objectName,bucketName,recursive,listPseudoFolder);
		while(iit.hasNext()){
			list.add(iit.next());
		}
		return list;
	}
	

	public boolean exists(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		return get(bucketName)!=null;
	}
	
	public boolean exists(String bucketName, String objectName, boolean includePseudoFolder) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		return get(bucketName, objectName,includePseudoFolder)!=null;
	}
	
	public boolean isDirectory(String bucketName, String objectName, boolean includePseudoFolder) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		S3Info info = get(bucketName, objectName,includePseudoFolder);
		return info!=null && info.isDirectory();
	}
	
	public boolean isFile(String bucketName, String objectName, boolean includePseudoFolder) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		S3Info info = get(bucketName, objectName,includePseudoFolder);
		return info!=null && info.isFile();
	}

	public S3BucketWrapper get(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		S3BucketWrapper info=null;
		if(buckets!=null) {
			info=buckets.get(bucketName);
			if(info!=null && info.validUntil()>=System.currentTimeMillis())
				return info;
		}
		try {
			S3Bucket b = getS3Service().getBucket(bucketName);
			if(b!=null) {
				return cacheSet(bucketName,b);
			}
			return null;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	
	public S3Info get(String bucketName, String objectName, boolean includePseudoFolder) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		String diffObjName=objectName.endsWith("/") ? improveObjectName(objectName,false):improveObjectName(objectName,true);
		
		// get from cache
		ValidUntilMap<StorageObjectWrapper> objects = this.bucketObjects.get(bucketName);
		if(objects!=null) {
			// direct match
			StorageObjectWrapper obj = objects.get(objectName);
			if(obj==null) obj = objects.get(diffObjName);
			if(obj!=null && obj.validUntil()>System.currentTimeMillis())
				return obj;
		}
		
		// no bucket no object
		S3BucketWrapper bwrapper = get(bucketName);
		if(bwrapper==null) return null;
		
		
		Iterator<S3Info> it = list(bucketName, true, includePseudoFolder).iterator();
		S3Info info;
		while(it.hasNext()) {
			info=it.next();
			if(info.getObjectName().equals(objectName) || info.getObjectName().equals(diffObjName))
				return info;
		}
		return null;
	}
	


	public void delete(String bucketName, boolean force) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		S3Service s = getS3Service();
		try{
			// delete the content of the bucket
			if(force) {
				S3Object[] children = s.listObjects(bucketName);
				if(children.length>0) 
					s.deleteMultipleObjects(bucketName, toObjectKeyAndVersions(children));
			}
			s.deleteBucket(bucketName);
			bucketObjects.remove(bucketName);
			if(buckets!=null)buckets.remove(bucketName);
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
	}


	public void delete(String bucketName, String objectName, boolean force) throws S3Exception {
		if(Util.isEmpty(objectName,true)) {
			delete(bucketName, force);
			return;
		}
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);

		String nameFile=improveObjectName(objectName,false);
		String nameDir=improveObjectName(objectName,true);
		
		
		// get parent
		int index=nameFile.lastIndexOf('/');
		String parent=null;
		if(index!=-1) {
			parent=nameFile.substring(0, index);
		}
		S3Service s = getS3Service();
		try {
			
			List<S3Info> list = parent!=null?
					list(bucketName,parent,true,false):
					list(bucketName,true,false);
					
			// remove all unrelated children
			{
				Iterator<S3Info> it = list.iterator();
				S3Info info;
				while(it.hasNext()){
					info=it.next();
					if(!info.getObjectName().equals(nameFile) && !info.getObjectName().startsWith(nameDir)) {
						it.remove();
					}
				}	
			}	

			// no file and directory with this name
			if(list.size()==0) {
				throw new S3Exception("can't delete file/directory "+bucketName+"/"+objectName+", file/directory does not exist");
			}
			// do we have a directory, a file or both?
			boolean hasDir=false,hasFile=false;
			Iterator<S3Info> it = list.iterator();
			S3Info info;
			while(it.hasNext()){
				info=it.next();
				if(info.getObjectName().startsWith(nameDir)) hasDir=true;
				else if(info.getObjectName().equals(nameFile)) hasFile=true;
			}
			
			ObjectKeyAndVersion[] keys;
			
			// we have a file, but not a directory
			if(hasFile && !hasDir) {
				s.deleteObject(bucketName, nameFile);
				cacheRemove(bucketName, nameFile);
				return;
			}
			
			// we have both
			else if(hasDir && hasFile) {
				// we have a file
				if(objectName.equals(nameFile)) {
					s.deleteObject(bucketName, nameFile);
					cacheRemove(bucketName, nameFile);
					return;
				}
				keys = toObjectKeyAndVersions(list,nameFile);
			}
			// only directories
			else {
				keys = toObjectKeyAndVersions(list,null);
			}
			
			
			if(!force && (keys.length>1 || (keys.length==1 && keys[0].getKey().substring(nameDir.length()).indexOf('/')!=-1))) {
				throw new S3Exception("can't delete directory "+bucketName+"/"+objectName+", directory is not empty");
			}
			ValidUntilMap<StorageObjectWrapper> objects = bucketObjects.get(bucketName);
			if(objects!=null){
				for(int i=0;i<keys.length;i++){
					objects.remove(keys[i].getKey());
				}
			}
			s.deleteMultipleObjects(bucketName, keys);
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
	}
	
	public void copy(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName) throws S3Exception {
		srcBucketName=improveBucketName(srcBucketName);
		srcObjectName=improveObjectName(srcObjectName,false);
		trgBucketName=improveBucketName(trgBucketName);
		trgObjectName=improveObjectName(trgObjectName,false);
		
		try {
			S3Object trg = new S3Object(trgObjectName);
			getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
			cacheSet(trgBucketName,trgObjectName,trg);
			
		}
		catch (ServiceException se) {
			if(get(trgBucketName)!=null)throw toS3Exception(se);
			
			S3BucketWrapper so = get(srcBucketName);
			String loc = so.getLocation();
			
			createDirectory(trgBucketName, null, loc);
			try {
				S3Object trg = new S3Object(trgObjectName);
				getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
				cacheSet(trgBucketName,trgObjectName,trg);
			} 
			catch (ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}

	public void move(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName) throws S3Exception {
		srcBucketName=improveBucketName(srcBucketName);
		srcObjectName=improveObjectName(srcObjectName,false);
		trgBucketName=improveBucketName(trgBucketName);
		trgObjectName=improveObjectName(trgObjectName,false);
		
		try {
			S3Object trg = new S3Object(trgObjectName);
			getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
			cacheSet(trgBucketName, trgObjectName, trg);
			cacheRemove(srcBucketName, srcObjectName);
		} 
		catch (ServiceException se) {
			if(get(trgBucketName)!=null)throw toS3Exception(se);
			
			S3BucketWrapper so = get(srcBucketName);
			String loc = so.getLocation();
			
			createDirectory(trgBucketName, null, loc);
			try {
				S3Object trg = new S3Object(trgObjectName);
				getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
				cacheSet(trgBucketName, trgObjectName, trg);
				cacheRemove(srcBucketName, srcObjectName);
			} 
			catch (ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}
	

	public void write(String bucketName,String objectName, String data, String mimeType, String charset, String acl, String location) throws IOException {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		
		try {
			S3Object so = new S3Object(objectName, data);
			String ct=toContentType(mimeType,charset,null);
			if(ct!=null)so.setContentType(ct);

			_write(so, bucketName, objectName, acl, location);
			cacheSet(bucketName, objectName, so);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}
	

	public void write(String bucketName,String objectName, byte[] data, String mimeType, String acl, String location) throws IOException {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		
		try {
			S3Object so = new S3Object(objectName, data);
			if(!Util.isEmpty(mimeType))so.setContentType(mimeType);

			_write(so, bucketName, objectName, acl, location);
			cacheSet(bucketName, objectName, so);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}

	public void write(String bucketName,String objectName, File file, String acl, String location) throws IOException {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		
		try {
			S3Object so = new S3Object(file);
			String mt = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(max1000(file), null);
			if(mt!=null) so.setContentType(mt);
			
			_write(so, bucketName, objectName, acl, location);
			cacheSet(bucketName, objectName, so);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}
	
	private void _write(S3Object so,String bucketName,String objectName, String acl, String location) throws IOException {
		try {
			
			so.setName(objectName);
			
			AccessControlList a = toACL(acl,null);
			if(a!=null)so.setAcl(a);
			
			getS3Service().putObject(bucketName, so);
		}
		catch (S3ServiceException se) {
			// does the bucket exist? if so we throw the exception
			if(get(bucketName)!=null) throw toS3Exception(se);
			// if the bucket does not exist, we do create it
			createDirectory(bucketName, acl, location);
			// now we try again
			try {
				getS3Service().putObject(bucketName, so);
			}
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}

	public Struct getMetaData(String bucketName, String objectName) throws S3Exception {
		Struct sct=CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
			bucketName=improveBucketName(bucketName);
			objectName=improveObjectName(objectName);
			
			;
			
			S3Info info;
			if(Util.isEmpty(objectName)) info=get(bucketName);
			else info=get(bucketName,objectName,false);
			
			if(info==null) throw new S3Exception("there is no object ["+objectName+"] in bucket ["+bucketName+"]");
			
			StorageOwner owner=info.getOwner();
			String location=info.getLocation();
			Map<String, Object> md=info.getMetaData();
			
			Iterator<Entry<String, Object>> it = md.entrySet().iterator();
			Entry<String, Object> e;
			while (it.hasNext()) {
				e = it.next();
				sct.setEL(e.getKey().replace('-', '_'), e.getValue());
			}
			
			// owner
			if (owner!=null) sct.put("owner", owner.getDisplayName());
			if (location!=null) sct.put("location", location);
		
		return sct;
	}
	

	public void setLastModified(String bucketName, String objectName, long time) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		if(!Util.isEmpty(objectName)) {
			S3Info info=get(bucketName,objectName,false);
			if(info==null) throw new S3Exception("there is no object ["+objectName+"] in bucket ["+bucketName+"]");
			
			StorageObject so = ((StorageObjectWrapper)info).getStorageObject();
			so.setLastModifiedDate(new Date(time));
		}
	}
	
	public void setMetaData(String bucketName, String objectName, Struct metadata) throws PageException, S3Exception {
		Iterator<Entry<Key, Object>> it = metadata.entryIterator();
		Entry<Key, Object> e;
		Map<String,Object> data=new HashMap<String, Object>();
		Decision dec = CFMLEngineFactory.getInstance().getDecisionUtil();
		Cast cas = CFMLEngineFactory.getInstance().getCastUtil();
		Object value;
		while (it.hasNext()) {
			e = it.next();
			value=e.getValue();
			if(dec.isDate(value, false))
				value=cas.toDate(value, null);
			else 
				value=cas.toString(value);
			data.put(toMetaDataKey(e.getKey()), value);
		}
		
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		if(Util.isEmpty(objectName)) {
			S3BucketWrapper bw = get(bucketName);
			if(bw==null) throw new S3Exception("there is no bucket ["+bucketName+"]");
			
			S3Bucket b = bw.getBucket();
			b.addAllMetadata(data);
		}
		else {
			S3Info info=get(bucketName,objectName,false);
			if(info==null) throw new S3Exception("there is no object ["+objectName+"] in bucket ["+bucketName+"]");
			
			StorageObject so = ((StorageObjectWrapper)info).getStorageObject();
			so.addAllMetadata(data);
		}
	}
	 
	
	private String toMetaDataKey(Key key) {
		if(key.equals("content-disposition")) return "Content-Disposition";
		if(key.equals("content_disposition")) return "Content-Disposition";
		if(key.equals("content disposition")) return "Content-Disposition";
		if(key.equals("contentdisposition")) return "Content-Disposition";

		if(key.equals("content-encoding")) return "Content-Encoding";
		if(key.equals("content_encoding")) return "Content-Encoding";
		if(key.equals("content encoding")) return "Content-Encoding";
		if(key.equals("contentencoding")) return "Content-Encoding";

		if(key.equals("content-language")) return "Content-Language";
		if(key.equals("content_language")) return "Content-Language";
		if(key.equals("content language")) return "Content-Language";
		if(key.equals("contentlanguage")) return "Content-Language";

		if(key.equals("content-length")) return "Content-Length";
		if(key.equals("content_length")) return "Content-Length";
		if(key.equals("content length")) return "Content-Length";
		if(key.equals("contentlength")) return "Content-Length";

		if(key.equals("content-md5")) return "Content-MD5";
		if(key.equals("content_md5")) return "Content-MD5";
		if(key.equals("content md5")) return "Content-MD5";
		if(key.equals("contentmd5")) return "Content-MD5";

		if(key.equals("content-type")) return "Content-Type";
		if(key.equals("content_type")) return "Content-Type";
		if(key.equals("content type")) return "Content-Type";
		if(key.equals("contenttype")) return "Content-Type";
		
		if(key.equals("last-modified")) return "Last-Modified";
		if(key.equals("last_modified")) return "Last-Modified";
		if(key.equals("last modified")) return "Last-Modified";
		if(key.equals("lastmodified")) return "Last-Modified";

		if(key.equals("md5_hash")) return "md5-hash";
		if(key.equals("md5_hash")) return "md5-hash";
		if(key.equals("md5_hash")) return "md5-hash";
		if(key.equals("md5_hash")) return "md5-hash";
		
		if(key.equals("date")) return "Date";
		if(key.equals("etag")) return "ETag";
		
		return key.getString();
	}

	public void addAccessControlList(String bucketName, String objectName, Object objACL) throws S3Exception, PageException {
		try {
			bucketName=improveBucketName(bucketName);
			objectName=improveObjectName(objectName);
			
			S3Service s = getS3Service();
			S3Bucket b = service.getBucket(bucketName);
			if(b==null)
				throw new S3Exception("there is no bucket with name ["+bucketName+"]");
			
			AccessControlList acl = getACL(s,b,objectName);
			acl.grantAllPermissions(AccessControlListUtil.toGrantAndPermissions(objACL));
			b.setAcl(acl);
			setACL(s, bucketName, objectName, acl);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	
	public void setAccessControlList(String bucketName, String objectName, Object objACL) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		try {
			S3Service s = getS3Service();
			bucketName=improveBucketName(bucketName);
			S3Bucket b = service.getBucket(bucketName);
			if(b==null)
				throw new S3Exception("there is no bucket with name ["+bucketName+"]");
			
			AccessControlList oldACL = getACL(s,b,objectName);
			AccessControlList newACL = AccessControlListUtil.toAccessControlList(objACL);
			
			StorageOwner aclOwner = oldACL!= null?oldACL.getOwner() : s.getAccountOwner();
			newACL.setOwner(aclOwner);
			setACL(s,b.getName(),objectName,newACL);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}

	public Array getAccessControlList(String bucketName, String objectName) throws S3Exception {
		AccessControlList acl = getACL(bucketName,objectName);
		return AccessControlListUtil.toArray(acl.getGrantAndPermissions());
	}
	
	private AccessControlList getACL(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		String key=bucketName+"|"+(objectName==null?"":objectName);
		
		ValidUntilAccessControlList vuacl = accessControlLists.get(key);
		if(vuacl!=null && vuacl.validUntil>System.currentTimeMillis()) return vuacl.acl;
		 
		
		try {
			if(Util.isEmpty(objectName)) 
				return getS3Service().getBucketAcl(bucketName);
			return getS3Service().getObjectAcl(bucketName, objectName);
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
	}
	
	private AccessControlList getACL(S3Service s, S3Bucket bucket, String objectName) throws S3Exception {
		objectName=improveObjectName(objectName);
		
		String key=bucket.getName()+"|"+(objectName==null?"":objectName);
		ValidUntilAccessControlList vuacl = accessControlLists.get(key);
		if(vuacl!=null && vuacl.validUntil>System.currentTimeMillis()) return vuacl.acl;
		
		try {
			if(Util.isEmpty(objectName)) 
				return s.getBucketAcl(bucket);
			return s.getObjectAcl(bucket, objectName);
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
	}
	
	public void setACL(S3Service s, String bucketName, String objectName,AccessControlList acl) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		String key=bucketName+"|"+(objectName==null?"":objectName);
		
		try {
			if(Util.isEmpty(objectName)) 
				s.putBucketAcl(bucketName,acl);
			else 
				s.putObjectAcl(bucketName, objectName, acl);
			
			accessControlLists.remove(key);
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
		
	}
	
	
	
	
	private String toContentType(String mimeType, String charset, String defaultValue) {
		if(!Util.isEmpty(mimeType)) {
			return !Util.isEmpty(charset)?mimeType+"; charset="+charset:mimeType;
		}
		return defaultValue;
	}


	public String url(String bucketName,String objectName, long time) throws S3Exception {
		return getS3Service().createSignedGetUrl(bucketName, objectName, new Date(System.currentTimeMillis()+time), false);
	}
	
	
	private S3Service getS3Service() {
		
		if(service==null) {
			service=new RestS3Service(new AWSCredentials(accessKeyId, secretAccessKey));
		}
		return service;
	}
	
	public static AccessControlList toACL(String acl,AccessControlList defaultValue) {
		if(acl==null) return defaultValue;
		
		acl=acl.trim().toLowerCase();

		if("public-read".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		if("public read".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		if("public_read".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		if("publicread".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		
		if("private".equals(acl)) return AccessControlList.REST_CANNED_PRIVATE;

		if("public-read-write".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
		if("public read write".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
		if("public_read_write".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
		if("publicreadwrite".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;

		if("authenticated-read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if("authenticated read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if("authenticated_read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if("authenticatedread".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		
		if("authenticate-read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if("authenticate read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if("authenticate_read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if("authenticateread".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		
		return defaultValue;
	}

	private ObjectKeyAndVersion[] toObjectKeyAndVersions(S3Object[] src) {
		ObjectKeyAndVersion[] trg=new ObjectKeyAndVersion[src.length];
		for(int i=0;i<src.length;i++){
			trg[i]=new ObjectKeyAndVersion(src[i].getKey(), src[i].getVersionId());
		}
		return trg;
	}

	/*private ObjectKeyAndVersion[] toObjectKeyAndVersions(StorageObject[] src, String ignoreKey) {
		List<ObjectKeyAndVersion> trg=new ArrayList<ObjectKeyAndVersion>();
		for(int i=0;i<src.length;i++){
			if(ignoreKey!=null && src[i].getKey().equals(ignoreKey)) continue;
			trg.add(new ObjectKeyAndVersion(src[i].getKey()));
		}
		return trg.toArray(new ObjectKeyAndVersion[trg.size()]);
	}*/
	
	private ObjectKeyAndVersion[] toObjectKeyAndVersions(List<S3Info> src, String ignoreKey) {
		List<ObjectKeyAndVersion> trg=new ArrayList<ObjectKeyAndVersion>();
		Iterator<S3Info> it = src.iterator();
		S3Info info;
		while(it.hasNext()){
			info=it.next();
			if(ignoreKey!=null && info.getName().equals(ignoreKey)) continue;
			trg.add(new ObjectKeyAndVersion(info.getName()));
		}
		return trg.toArray(new ObjectKeyAndVersion[trg.size()]);
	}

	
	private S3Exception toS3Exception(ServiceException se) {
		String msg=se.getErrorMessage();
		if(Util.isEmpty(msg))msg=se.getMessage();
	
		S3Exception ioe = new S3Exception(msg);
		ioe.setStackTrace(se.getStackTrace());
		return ioe;
	}
	

	public static String improveBucketName(String bucketName) throws S3Exception {
		if(bucketName.startsWith("/"))bucketName=bucketName.substring(1);
		if(bucketName.endsWith("/"))bucketName=bucketName.substring(0, bucketName.length()-1);
		if(bucketName.indexOf('/')!=-1) throw new S3Exception("invalid bucket name ["+bucketName+"]");
		return bucketName;
	}

	public static String improveObjectName(String objectName) throws S3Exception {
		if(objectName==null) return null;
		if(objectName.startsWith("/"))objectName=objectName.substring(1);
		return objectName;
	}
	
	public static String improveObjectName(String objectName, boolean directory) throws S3Exception {
		if(objectName.startsWith("/"))objectName=objectName.substring(1);
		if(directory) {
			if(!objectName.endsWith("/"))objectName=objectName+"/";
		}
		else if(objectName.endsWith("/"))objectName=objectName.substring(0, objectName.length()-1);
		return objectName;
	}
	
	public static String improveLocation(String location) {
		if(location==null) return location;
		location=location.toLowerCase().trim();
		if("usa".equals(location)) return "us";
		if("u.s.".equals(location)) return "us";
		if("u.s.a.".equals(location)) return "us";
		if("united states of america".equals(location)) return "us";
		
		if("europe.".equals(location)) return "eu";
		if("euro.".equals(location)) return "eu";
		if("e.u.".equals(location)) return "eu";
		
		if("usa-west".equals(location)) return "us-west";
		
		
		return location;
	}
	

	private S3BucketWrapper cacheSet(String bucketName, S3Bucket bucket) {
		S3BucketWrapper bw = new S3BucketWrapper(bucket, System.currentTimeMillis()+timeout);
		if(buckets!=null) buckets.put(bucketName, bw);
		return bw;
		
	}
	private StorageObjectWrapper cacheSet(String bucketName, String objectName, S3Object so) {
		ValidUntilMap<StorageObjectWrapper> objects = bucketObjects.get(bucketName);
		StorageObjectWrapper sw = new StorageObjectWrapper(this,so, bucketName, System.currentTimeMillis()+timeout);
		if(objects!=null) objects.put(objectName, sw);
		return sw;
	}
	
	private void cacheRemove(String bucketName, String objectName) {
		if(buckets!=null)buckets.remove(bucketName);
		ValidUntilMap<StorageObjectWrapper> objects = bucketObjects.get(bucketName);
		if(objects!=null) objects.remove(objectName);
	}
	
	private byte[] max1000(File file) throws IOException {
		FileInputStream in = new FileInputStream(file);
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		try {
			final byte[] buffer = new byte[1000];
			int len;
			if((len = in.read(buffer)) != -1)
				out.write(buffer, 0, len);
		}
		finally {
			Util.closeEL(in,out);
		}
		return out.toByteArray();
	}
		
	public boolean getCustomCredentials() {
		return customCredentials;
	}
	/*public void setCustomCredentials(boolean customCredentials) {
		this.customCredentials=customCredentials;
	}*/
	
	class ValidUntilMap<I> extends HashMap<String, I> {
		private static final long serialVersionUID = 238079099294942075L;
		private final long validUntil;

		public ValidUntilMap(long validUntil){
			this.validUntil=validUntil;
		}
	}
	
	class ValidUntilList extends ArrayList<S3Info> {

		private static final long serialVersionUID = -1928364430347198544L;
		private final long validUntil;

		public ValidUntilList(long validUntil){
			this.validUntil=validUntil;
		}
	}
	
	class ObjectFilter {
		
		private final String objectNameWithoutSlash;
		private final String objectNameWithSlash;
		private boolean recursive;

		public ObjectFilter(String objectName, boolean recursive) throws S3Exception {
			this.objectNameWithoutSlash=improveObjectName(objectName, false);
			this.objectNameWithSlash=improveObjectName(objectName, true);
			this.recursive=recursive;
		}
		
		public boolean accept(String objectName){

			if(!objectNameWithoutSlash.equals(objectName) && !objectName.startsWith(objectNameWithSlash))
				return false;
			
			if(!recursive && objectName.length()>objectNameWithSlash.length()) {
				String sub = objectName.substring(objectNameWithSlash.length());
				int index = sub.indexOf('/');
				if(index!=-1 && index+1<sub.length()) return false;
			}
			return true;
		}
	}
	

	class ValidUntilAccessControlList {
		private final long validUntil;
		private final AccessControlList acl;
		
		public ValidUntilAccessControlList(AccessControlList acl, long validUntil){
			this.acl=acl;
			this.validUntil=validUntil;
		}
	}	
}
