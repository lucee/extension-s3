package org.lucee.extension.resource.s3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.net.s3.Properties;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Decision;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.model.StorageOwner;
import org.jets3t.service.model.container.ObjectKeyAndVersion;
import org.jets3t.service.security.AWSCredentials;
import org.lucee.extension.resource.s3.info.NotExisting;
import org.lucee.extension.resource.s3.info.ParentObject;
import org.lucee.extension.resource.s3.info.S3BucketWrapper;
import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.info.StorageObjectWrapper;
import org.lucee.extension.resource.s3.util.print;

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
	private final Map<String,ValidUntilMap<S3Info>> objects=new HashMap<String, ValidUntilMap<S3Info>>(); 
	Map<String,ValidUntilElement<AccessControlList>> accessControlLists=new HashMap<String,ValidUntilElement<AccessControlList>>();
	private Map<String,S3Info> exists=new HashMap<String,S3Info>();
	
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



	
	public S3Bucket createDirectory(String bucketName, AccessControlList acl, String location) throws S3Exception {
		//flushExists(bucketName);
		
		bucketName=improveBucketName(bucketName);
		String l = improveLocation(location);
		try{
			S3Bucket bucket;
			if(!Util.isEmpty(l,true)) {
				if(acl!=null) bucket = getS3Service().createBucket(bucketName,l, acl);
				else bucket = getS3Service().createBucket(bucketName,l);
			}
			else bucket = getS3Service().createBucket(bucketName);
			//buckets.put(bucketName, new S3BucketWrapper(bucket, validUntil()));
			flushExists(bucketName);
			return bucket;
		}
		catch(ServiceException se){
			throw toS3Exception(se, "could not create the bucket ["+bucketName+"], please consult the following website to learn about Bucket Restrictions and limitations: https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html");
		}
	}

	private long validUntil() {
		return System.currentTimeMillis()+timeout;
	}

	/**
	 * 
	 * @param bucketName name of the bucket
	 * @param objectName name of the object
	 * @param acl 
	 * @param storage only used when creating a non existing bucket
	 * @throws IOException
	 */
	public void createDirectory(String bucketName, String objectName, AccessControlList acl, String location) throws S3Exception {
		if(Util.isEmpty(objectName)) {
			createDirectory(bucketName, acl, location);
			return;
		}
		
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName, true);
		flushExists(bucketName,objectName);
		
		S3Object object = new S3Object("object");
		object.addMetadata("Content-Type", "application/x-directory");
		if(acl!=null)object.setAcl(acl);
		
		objectName=improveObjectName(objectName, true);
		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			S3Object so = getS3Service().putObject(bucketName, object);
			flushExists(bucketName, objectName);
		}
		catch (ServiceException se) {
			if(get(bucketName)!=null)throw toS3Exception(se);
			
			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				S3Object so = getS3Service().putObject(bucket, object);
				flushExists(bucketName, objectName);
			} 
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}
	
	public void createFile(String bucketName, String objectName, AccessControlList acl, String location) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName, false);

		flushExists(bucketName,objectName);
		
		S3Object object = new S3Object("object");
		if(acl!=null)object.setAcl(acl);
		
		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			getS3Service().putObject(bucketName, object);
			flushExists(bucketName, objectName);
		}
		catch (ServiceException se) {
			if(get(bucketName)!=null)throw toS3Exception(se);
			
			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				getS3Service().putObject(bucket, object);
				flushExists(bucketName, objectName);
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
		
		S3Info info = get(bucketName, objectName);
		if(info instanceof StorageObjectWrapper)
			return ((StorageObjectWrapper)info).getStorageObject();
		return null;
		/*ValidUntilMap<StorageObjectWrapper> objects = bucketObjects.get(bucketName);
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
		}*/
	}


	public List<S3Info> list(boolean recursive, boolean listPseudoFolder) throws S3Exception {
		try {
			
			// no cache for buckets
			if(timeout<=0 || buckets==null || buckets.validUntil<System.currentTimeMillis()) {
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
					Iterator<S3Info> iit = list(info.getBucketName(), "", recursive, listPseudoFolder,true).iterator();
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
	 
	public List<S3Info> list(String bucketName, boolean recursive, boolean listPseudoFolder) throws S3Exception {
		return list(bucketName, "", recursive, listPseudoFolder);
	}*/

	public List<S3Info> list(String bucketName, String objectName, boolean recursive, 
			boolean listPseudoFolder, boolean onlyChildren) throws S3Exception {
		return list(bucketName, objectName, recursive, listPseudoFolder, onlyChildren,false);
	}
	
	public List<S3Info> list(String bucketName, String objectName, boolean recursive, 
			boolean listPseudoFolder, boolean onlyChildren,boolean noCache) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		ValidUntilMap<S3Info> objects = _list(bucketName,objectName,onlyChildren, noCache);
		
		Iterator<S3Info> it = objects.values().iterator();
		Map<String,S3Info> map=new LinkedHashMap<String,S3Info>();
		S3Info info;
		while(it.hasNext()){
			info=it.next();
			add(map,info,objectName,recursive,listPseudoFolder, onlyChildren);
		}
		Iterator<S3Info> iit=map.values().iterator();
		List<S3Info> list=new ArrayList<S3Info>();
		while(iit.hasNext()) {
			list.add(iit.next());
		}
		return list;
	}

	private void add(Map<String,S3Info> map, S3Info info, String prefix, 
			boolean recursive, boolean addPseudoEntries, boolean onlyChildren) throws S3Exception {
		String nameFile=improveObjectName(info.getObjectName(), false);
		int index,last=0;
		ParentObject tmp;
		String objName;
		S3Info existing;
		
		if(addPseudoEntries) {
			while((index=nameFile.indexOf('/',last))!=-1){
				tmp=new ParentObject(nameFile.substring(0, index+1), info);
				if(doAdd(tmp,prefix,recursive,onlyChildren)) {
					objName=improveObjectName(tmp.getObjectName(),false);
					existing = map.get(objName);
					if(existing==null) {
						map.put(objName,tmp);
					}
				}
				last=index+1;
			}
		}
		
		if(doAdd(info,prefix,recursive,onlyChildren)) {
			objName=improveObjectName(info.getObjectName(),false);
			existing = map.get(objName);
			if(existing==null || existing instanceof ParentObject) map.put(objName,info);
		}
	}

	private boolean doAdd(S3Info info, String prefix, boolean recursive, boolean onlyChildren) throws S3Exception {
		prefix=improveObjectName(prefix,false);
		String name=improveObjectName(info.getObjectName(),false);
		
		// no parents
		if(prefix.length()>name.length()) return false;
		
		// only children
		if(onlyChildren && prefix.equals(name)) return false;

		// no grand ... children
		if(!recursive && !isDirectKid(info.getObjectName(),prefix)) return false;

		return true;
	}

	private static boolean isDirectKid(String name, String prefix) throws S3Exception {
		if(prefix==null) prefix="";
		if(name==null) name="";
		
		prefix=prefix.length()==0?"":improveObjectName(prefix, true);
		String sub=improveObjectName(name.substring(prefix.length()),false);
		return sub.indexOf('/')==-1;
	}

	/*private ValidUntilMap<StorageObjectWrapper> _list(String bucketName) throws S3Exception {
		try {
			// not cached 
			ValidUntilMap<StorageObjectWrapper> _list = bucketObjects.get(bucketName);
			if(_list==null || _list.validUntil<System.currentTimeMillis()) {
				S3Object[] kids = getS3Service().listObjects(bucketName);
				long validUntil=System.currentTimeMillis()+timeout;
				_list=new ValidUntilMap<StorageObjectWrapper>(validUntil);
				bucketObjects.put(bucketName, _list);
				StorageObjectWrapper tmp;
				for(int i=0;i<kids.length;i++){
					_list.put(kids[i].getKey(),tmp=new StorageObjectWrapper(this,kids[i],bucketName,validUntil,kids));
					exists.put(kids[i].getBucketName()+":"+improveObjectName(kids[i].getName(),false),tmp);
				}
			}
			return _list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}*/
	
	private ValidUntilMap<S3Info> _list(String bucketName, String objectName, boolean onlyChildren, boolean noCache) throws S3Exception {
		try {
			String key=toKey(bucketName,objectName);
			String nameDir=improveObjectName(objectName, true);
			String nameFile=improveObjectName(objectName, false);
			boolean hasObjName=!Util.isEmpty(objectName);
			
			// not cached 
			ValidUntilMap<S3Info> _list = timeout<=0 || noCache?null: objects.get(key);
			if(_list==null || _list.validUntil<System.currentTimeMillis()) {
				S3Object[] kids = hasObjName? 
						getS3Service().listObjects(bucketName,nameFile,","):
						getS3Service().listObjects(bucketName);
						
				long validUntil=System.currentTimeMillis()+timeout;
				_list=new ValidUntilMap<S3Info>(validUntil);
				objects.put(key, _list);
				
				// add bucket
				if(!hasObjName && !onlyChildren) {
					S3Bucket b = getS3Service().getBucket(bucketName);
					_list.put("", new S3BucketWrapper(b, validUntil));
				}
				
				
				StorageObjectWrapper tmp;
				String name;
				for(int i=0;i<kids.length;i++){
					name=kids[i].getName();
					tmp=new StorageObjectWrapper(this,kids[i],bucketName,validUntil);
					
					if(!hasObjName || name.equals(nameFile) || name.startsWith(nameDir))
						_list.put(kids[i].getKey(),tmp);
					
					exists.put(toKey(kids[i].getBucketName(), kids[i].getName()),tmp);
				}
			}
			return _list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}

	private String toKey(String bucketName, String objectName) {
		if(objectName==null) objectName="";
		return improveBucketName(bucketName)+":"+improveObjectName(objectName, false);
	}

	public boolean exists(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		return get(bucketName)!=null;
	}
	
	public boolean exists(String bucketName, String objectName) throws S3Exception {
		if(Util.isEmpty(objectName)) return exists(bucketName);
		S3Info info = get(bucketName, objectName);
		return info!=null && info.exists();
	}
	
	public boolean isDirectory(String bucketName, String objectName) throws S3Exception {
		if(Util.isEmpty(objectName)) return exists(bucketName); // bucket is always adirectory
		S3Info info = get(bucketName, objectName);
		if(info==null || !info.exists()) return false;
		return info.isDirectory();
	}
	
	public boolean isFile(String bucketName, String objectName) throws S3Exception {
		if(Util.isEmpty(objectName)) return false; // bucket is newer a file
		S3Info info = get(bucketName, objectName);
		if(info==null || !info.exists()) return false;
		return info.isFile();
	}
	
	public S3Info get(String bucketName, final String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		String nameFile=improveObjectName(objectName,false);
		String nameDir=improveObjectName(objectName,true);
		// cache
		S3Info info = timeout<=0?null:exists.get(toKey(bucketName,nameFile));
		if(info!=null && info.validUntil()>=System.currentTimeMillis()) {
			if(info instanceof NotExisting) return null;
			return info;
		}
		info=null;
		try {
			StorageObjectsChunk chunk = listObjectsChunkedSilent(bucketName, nameFile, 10, null);
			
			long validUntil=System.currentTimeMillis()+timeout;
			StorageObject[] objects = chunk==null?null:chunk.getObjects();
			
			if(objects==null || objects.length==0) {
				exists.put(
						toKey(bucketName,objectName), 
						new NotExisting(bucketName,objectName,null,validUntil) // we do not return this, we just store it to cache that it does not exis
					);
				return null;
			}
			
			String targetName;
			StorageObject stoObj=null;
			// direct match
			for(StorageObject so:objects) {
				targetName=so.getName();
				if(nameFile.equals(targetName) || nameDir.equals(targetName)) {
					exists.put(
							toKey(bucketName,nameFile), 
							info=new StorageObjectWrapper(this, stoObj=so, bucketName, validUntil)
					);
				}
			}
			
			// pseudo directory?
			if(info==null) {
				for(StorageObject so:objects) {
					targetName=so.getName();
					if(nameDir.length()<targetName.length() && targetName.startsWith(nameDir)) {
						exists.put(
								toKey(bucketName,nameFile), 
								info=new ParentObject(bucketName,nameDir, null,validUntil)
						);
					}
				}
			}
			
			for(StorageObject obj:objects) {
				if(stoObj!=null && stoObj.equals(obj)) continue;
				exists.put(
					toKey(obj.getBucketName(),obj.getName()), 
					new StorageObjectWrapper(this, obj, bucketName, validUntil)
				);
			}
			
			if(info==null) {
				exists.put(
					toKey(bucketName,objectName), 
					new NotExisting(bucketName,objectName,null,validUntil) // we do not return this, we just store it to cache that it does not exis
				);
			}
			return info;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/*private boolean _isDirectory(StorageObject so, StorageObject[] sisters) throws S3Exception {
		if(so.isDirectoryPlaceholder()) return true;
		
		// content length
		if(so.getContentLength()>0) return false;
		
		// meta data
		Object o = so.getMetadata("Content-Type");
		//System.out.println("- Content-Type:"+o);
		if(o instanceof String) {
			String ct=(String)o;
			if("application/x-directory".equalsIgnoreCase(ct)) return true;
			if(ct.startsWith("audio/")) return false;
			if(ct.startsWith("image/")) return false;
			if(ct.startsWith("text/")) return false;
			if(ct.startsWith("video/")) return false;
		}
		
		
		
		// when a file has "children" it is a directory
		if(sisters!=null) {
			String name=improveObjectName(so.getName(), true);
			for(StorageObject sis:sisters) {
				if(sis.getName().startsWith(name) && sis.getName().length()>name.length()) return true;
			}
		}
		return so.getName().endsWith("/"); // i don't like this, but this is a pattern used with S3
	}*/
	

	public StorageObjectsChunk listObjectsChunkedSilent(String bucketName, String objectName, int max, String priorLastKey) {
		try {
			return getS3Service().listObjectsChunked(bucketName, objectName, ",", max, priorLastKey);
		}
		catch (Exception e) {}
		return null;
	}
	
	public S3BucketWrapper get(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		 
		// buckets cache
		S3BucketWrapper info=null;
		if(buckets!=null && timeout>0) {
			info=buckets.get(bucketName);
			if(info!=null && info.validUntil()>=System.currentTimeMillis())
				return info;
		}
		
		// this will load it and cache if necessary
		List<S3Info> list = list(false, false);
		Iterator<S3Info> it = list.iterator();
		while(it.hasNext()) {
			info=(S3BucketWrapper) it.next();
			if(info.getBucketName().equals(bucketName))
				return info;
		}
		return null;
		
		/*try {
			S3Bucket b = getS3Service().getBucket(bucketName);
			if(b!=null) {
				return new S3BucketWrapper(b, System.currentTimeMillis()+timeout);
			}
			return null;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}*/
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
			flushExists(bucketName);
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
		try {
			S3Service s = getS3Service();
			List<S3Info> list = list(bucketName,nameFile,true,false,false);
			
			// no file and directory with this name
			if(list.size()==0) {
				throw new S3Exception("can't delete file/directory "+bucketName+"/"+objectName+", file/directory does not exist");
			}
			
			ObjectKeyAndVersion[] keys = toObjectKeyAndVersions(list,null);
			if(!force && 
					(keys.length>1 || (keys.length==1 && keys[0].getKey().length()>nameDir.length() && keys[0].getKey().substring(nameDir.length()).indexOf('/')!=-1))) {
				throw new S3Exception("can't delete directory "+bucketName+"/"+objectName+", directory is not empty");
			}
			
			// clear cache
			Iterator<S3Info> it = list.iterator();
			S3Info info;
			while(it.hasNext()){
				info=it.next();
				flushExists(info.getBucketName(), info.getObjectName());
			}
			// we create parent because before it maybe was a pseudi dir
			s.deleteMultipleObjects(bucketName, keys);
			flushExists(bucketName,objectName);
			createParentDirectory(bucketName,objectName,true);
		}
		catch(ServiceException se){
			throw toS3Exception(se);
		}
		finally {
			flushExists(bucketName,objectName);
		}
	}
	
	private void createParentDirectory(String bucketName, String objectName, boolean noCache) throws S3Exception {
		String parent=getParent(objectName);
		if(parent==null) return;
		if(noCache)flushExists(bucketName, parent);
		if(!exists(bucketName, parent))
			createDirectory(bucketName, parent, null, null);
	}

	private String getParent(String objectName) throws S3Exception {
		objectName=improveObjectName(objectName, false);
		int index=objectName.lastIndexOf('/');
		if(index==-1) return null;
		return objectName.substring(0, index+1);
	}

	public void copy(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName) throws S3Exception {
		srcBucketName=improveBucketName(srcBucketName);
		srcObjectName=improveObjectName(srcObjectName,false);
		trgBucketName=improveBucketName(trgBucketName);
		trgObjectName=improveObjectName(trgObjectName,false);
		flushExists(srcBucketName,srcObjectName);
		flushExists(trgBucketName,trgObjectName);
		try {
			S3Object trg = new S3Object(trgObjectName);
			getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
			flushExists(trgBucketName, trgObjectName);
			
		}
		catch (ServiceException se) {
			if(get(trgBucketName)!=null)throw toS3Exception(se);
			
			S3BucketWrapper so = get(srcBucketName);
			String loc = so.getLocation();
			
			createDirectory(trgBucketName, null, loc);
			try {
				S3Object trg = new S3Object(trgObjectName);
				getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
				flushExists(trgBucketName, trgObjectName);
			} 
			catch (ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}

	private void flushExists(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		String prefix=bucketName+":";
		buckets=null;
		_flush(exists,prefix,null);
		_flush(objects,prefix,null);
	}

	private void flushExists(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		String nameDir=improveObjectName(objectName,true);
		String nameFile=improveObjectName(objectName,false);
		String exact=bucketName+":"+nameFile;
		String prefix=bucketName+":"+nameDir;
		String prefix2=bucketName+":";
		
		_flush(exists, prefix, exact);
		_flush(objects, prefix2, exact);
	}
	
	private static void _flush(Map<String,?> map, String prefix, String exact) {
		if(map==null) return;
		Set<String> keySet = map.keySet();
		String[] keys = keySet.toArray(new String[keySet.size()]);
		for(String key:keys) {
			if((exact!=null && key.equals(exact)) || key.startsWith(prefix)){
				map.remove(key);
			}
		}
	}

	public void move(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName) throws S3Exception {
		srcBucketName=improveBucketName(srcBucketName);
		srcObjectName=improveObjectName(srcObjectName,false);
		trgBucketName=improveBucketName(trgBucketName);
		trgObjectName=improveObjectName(trgObjectName,false);
		flushExists(srcBucketName,srcObjectName);
		flushExists(trgBucketName,trgObjectName);
		
		try {
			S3Object trg = new S3Object(trgObjectName);
			getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
			
			flushExists(srcBucketName, srcObjectName);
			flushExists(trgBucketName, trgObjectName);
		} 
		catch (ServiceException se) {
			if(get(trgBucketName)!=null)throw toS3Exception(se);
			
			S3BucketWrapper so = get(srcBucketName);
			String loc = so.getLocation();
			
			createDirectory(trgBucketName, null, loc);
			try {
				S3Object trg = new S3Object(trgObjectName);
				getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
				flushExists(srcBucketName, srcObjectName);
				flushExists(trgBucketName, trgObjectName);
			} 
			catch (ServiceException e) {
				throw toS3Exception(se);
			}
		}
		createParentDirectory(srcBucketName, srcObjectName,true);
	}
	

	public void write(String bucketName,String objectName, String data, String mimeType, String charset, AccessControlList acl, String location) throws IOException {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		flushExists(bucketName,objectName);
		
		try {
			S3Object so = new S3Object(objectName, data);
			String ct=toContentType(mimeType,charset,null);
			if(ct!=null)so.setContentType(ct);

			_write(so, bucketName, objectName, acl, location);
			flushExists(bucketName, objectName);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}
	

	public void write(String bucketName,String objectName, byte[] data, String mimeType, AccessControlList acl, String location) throws IOException {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		flushExists(bucketName,objectName);
		
		try {
			S3Object so = new S3Object(objectName, data);
			if(!Util.isEmpty(mimeType))so.setContentType(mimeType);

			_write(so, bucketName, objectName, acl, location);
			flushExists(bucketName, objectName);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}

	public void write(String bucketName,String objectName, File file, AccessControlList acl, String location) throws IOException {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,false);
		flushExists(bucketName,objectName);
		
		try {
			S3Object so = new S3Object(file);
			String mt = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(max1000(file), null);
			if(mt!=null) so.setContentType(mt);
			
			_write(so, bucketName, objectName, acl, location);
			flushExists(bucketName, objectName);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}
	
	private void _write(S3Object so,String bucketName,String objectName, AccessControlList acl, String location) throws IOException {
		try {
			
			so.setName(objectName);
			
			if(acl!=null)so.setAcl(acl);
			
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
			else info=get(bucketName,objectName);
			
			if(info==null || info.isVirtual())
				throw new S3Exception("there is no object ["+objectName+"] in bucket ["+bucketName+"]");
			
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
			S3Info info=get(bucketName,objectName);
			if(info==null || info.isVirtual()) 
				throw new S3Exception("there is no object ["+objectName+"] in bucket ["+bucketName+"]");
			
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
			b.addAllMetadata(data); // INFO seems not to be possible at all
		}
		else {
			S3Info info=get(bucketName,objectName);
			
			if(info==null || info.isVirtual()) 
				throw new S3Exception("there is no object ["+objectName+"] in bucket ["+bucketName+"]");
			
			StorageObject so = ((StorageObjectWrapper)info).getStorageObject();
			so.addAllMetadata(data);
			try {
				getS3Service().updateObjectMetadata(bucketName, so);
			}
			catch (ServiceException se) {
				throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(se);
			}
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
		
		String key=toKey(bucketName,objectName);
		
		ValidUntilElement<AccessControlList> vuacl = accessControlLists.get(key);
		if(vuacl!=null && vuacl.validUntil>System.currentTimeMillis()) return vuacl.element;
		 
		
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
		
		String key=toKey(bucket.getName(),objectName);
		ValidUntilElement<AccessControlList> vuacl = accessControlLists.get(key);
		if(vuacl!=null && vuacl.validUntil>System.currentTimeMillis()) return vuacl.element;
		
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
		String key=toKey(bucketName,objectName);
		
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
	

	public static AccessControlList toACL(Properties prop, AccessControlList defaultValue) {
		try {
			Method m = prop.getClass().getMethod("getACL", new Class[0]);
			String str=CFMLEngineFactory.getInstance().getCastUtil().toString(m.invoke(prop, new Object[0]),null);
			if(Util.isEmpty(str)) return defaultValue;
			return toACL(str, defaultValue);
		}
		catch (Exception e) {}
		return defaultValue;
	}
	

	private ObjectKeyAndVersion[] toObjectKeyAndVersions(S3Object[] src) {
		ObjectKeyAndVersion[] trg=new ObjectKeyAndVersion[src.length];
		for(int i=0;i<src.length;i++){
			trg[i]=new ObjectKeyAndVersion(src[i].getKey(), src[i].getVersionId());
		}
		return trg;
	}

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
		return toS3Exception(se, null);
	}
	
	private S3Exception toS3Exception(ServiceException se, String detail) {
		String msg=se.getErrorMessage();
		if(Util.isEmpty(msg))msg=se.getMessage();
	
		S3Exception ioe = Util.isEmpty(detail)?new S3Exception(msg):new S3Exception(msg+";"+detail);
		ioe.initCause(se);
		ioe.setStackTrace(se.getStackTrace());
		return ioe;
	}
	

	public static String improveBucketName(String bucketName) {
		if(bucketName.startsWith("/"))bucketName=bucketName.substring(1);
		if(bucketName.endsWith("/"))bucketName=bucketName.substring(0, bucketName.length()-1);
		if(bucketName.indexOf('/')!=-1) throw new RuntimeException(new S3Exception("invalid bucket name ["+bucketName+"]"));
		return bucketName;
	}

	public static String improveObjectName(String objectName) {
		if(objectName==null) return null;
		if(objectName.startsWith("/"))objectName=objectName.substring(1);
		return objectName;
	}
	
	public static String improveObjectName(String objectName, boolean directory) {
		if(objectName==null) objectName="";
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
	

	class ValidUntilElement<E> {
		private final long validUntil;
		private final E element;
		//private Boolean isDirectory;
		
		public ValidUntilElement(E element, long validUntil){
			this.element=element;
			this.validUntil=validUntil;
			//this.isDirectory=isDirectory;
		}
	}
	

	class EL {
		
		private final boolean match;
		private final boolean isDirectory;
		private final boolean isObject;
		
		public EL(boolean match, boolean isDirectory, boolean isObject){
			this.match=match;
			this.isDirectory=isDirectory;
			this.isObject=isObject;
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
}