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
import java.util.List;
import java.util.Map;

import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.model.container.ObjectKeyAndVersion;
import org.jets3t.service.security.AWSCredentials;
import org.lucee.extension.resource.s3.info.S3BucketWrapper;
import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.info.StorageObjectWrapper;

public class S3 {
	
	public static final String DEFAULT_HOST="s3.amazonaws.com";
	
	private String host=DEFAULT_HOST;
	private String secretAccessKey;
	private String accessKeyId;
	private S3Service service;
	private Map<String,S3Info> info=new HashMap<String,S3Info>();

	private boolean customCredentials;

	public S3(String secretAccessKey, String accessKeyId) {
		this.secretAccessKey=secretAccessKey;
		this.accessKeyId=accessKeyId;
	}
	public S3() {
		this.secretAccessKey=secretAccessKey;
		this.accessKeyId=accessKeyId;
	}


	public void setHost(String host) {
		if(!Util.isEmpty(host,true))this.host=host;
	}


	public String getHost() {
		return host;
	}

	public void setSecretAccessKey(String secretAccessKey) {
		flush();
		this.secretAccessKey=secretAccessKey;
	}

	public void setAccessKeyId(String accessKeyId) {
		flush();
		this.accessKeyId=accessKeyId;
		
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
			if(!Util.isEmpty(l,true)) {
				if(a!=null) return getS3Service().createBucket(bucketName,l, a);
				return getS3Service().createBucket(bucketName,l);
			}
			return getS3Service().createBucket(bucketName);
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
		
		AccessControlList a = toACL(acl,null);
		
		S3Object object = new S3Object("object");
		if(a!=null)object.setAcl(a);
		
		if(!objectName.endsWith("/"))objectName+="/";
		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			getS3Service().putObject(bucketName, object);
		}
		catch (ServiceException se) {
			if(get(bucketName)!=null)throw toS3Exception(se);
			
			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				getS3Service().putObject(bucket, object);
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
			getS3Service().putObject(bucketName, object);
		}
		catch (ServiceException se) {
			if(get(bucketName)!=null)throw toS3Exception(se);
			
			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				getS3Service().putObject(bucket, object);
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
		
		try {
			return getS3Service().getObjectDetails(bucketName, objectName);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}


	public List<S3Info> list(boolean recursive) throws S3Exception {
		try {
			S3Bucket[] buckets = getS3Service().listAllBuckets();
			
			List<S3Info> list=new ArrayList<S3Info>();
			for(int i=0;i<buckets.length;i++){
				list.add(new S3BucketWrapper(buckets[i]));
				if(recursive)_list(list,buckets[i].getName(), true);
			}
			return list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}

	public List<S3Info> list(String bucketName, boolean recursive) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		if(Util.isEmpty(bucketName)) return list(recursive);
		
		List<S3Info> list=new ArrayList<S3Info>();
		_list(list, bucketName, recursive);
		return list;
	}
	
	private void _list(List<S3Info> list,String bucketName, boolean recursive) throws S3Exception {
		try {
			S3Object[] kids = getS3Service().listObjects(bucketName);
			String name;
			int index;
			for(int i=0;i<kids.length;i++){
				if(!recursive) {
					name=kids[i].getName();
					index=name.indexOf('/');
					if(index!=-1 && index+1<name.length()) continue;
				}
				list.add(new StorageObjectWrapper(this,kids[i]));
			}
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	
	public List<S3Info> list(String bucketName, String objectName,boolean recursive) throws S3Exception {
		if(Util.isEmpty(objectName)) return list(bucketName,recursive);
		
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName,true);
		
		try {
			List<S3Info> list=new ArrayList<S3Info>();
			StorageObject[] kids = getS3Service()
					.listObjectsChunked(bucketName, objectName, ",", Integer.MAX_VALUE, null, true)
					.getObjects();
			String sub;
			int index;
			for(int i=0;i<kids.length;i++){
				if(kids[i].getName().equals(objectName)) continue;
				if(!recursive) {
					sub=kids[i].getName().substring(objectName.length());
					index=sub.indexOf('/');
					if(index!=-1 && index+1<sub.length()) continue;
				}
				list.add(new StorageObjectWrapper(this,kids[i]));
			}
			return list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		
		
	}
	

	public boolean exists(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		return get(bucketName)!=null;
	}
	public boolean exists(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		return get(bucketName,objectName)!=null;
	}

	public S3BucketWrapper get(String bucketName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		try {
			S3Bucket b = getS3Service().getBucket(bucketName);
			if(b!=null) return new S3BucketWrapper(b);
			return null;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	
	public S3Info get(String bucketName, String objectName) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		objectName=improveObjectName(objectName);
		
		try {
			S3Service s = getS3Service();
			S3Bucket b = s.getBucket(bucketName);
			if(b==null) return null;
			try {
				S3Object obj = s.getObject(b, objectName);
				if(obj!=null) return new StorageObjectWrapper(this,obj);
			}
			catch (ServiceException se) {
			}
			return null;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}
	


	public void delete(String bucketName, boolean force) throws S3Exception {
		bucketName=improveBucketName(bucketName);
		
		S3Service s = getS3Service();
		try{
			// delete the content of the bucket
			if(force) {
				S3Object[] children = s.listObjects(bucketName);
				if(children.length>0)s.deleteMultipleObjects(bucketName, toObjectKeyAndVersions(children));
			}
			s.deleteBucket(bucketName);
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
		
		
		S3Service s = getS3Service();
		try {
			
			StorageObject[] kids = getS3Service()
					.listObjectsChunked(bucketName, nameFile, ",", Integer.MAX_VALUE, null, true)
					.getObjects();
			
			
			// no file and directory with this name
			if(kids.length==0)
				throw new S3Exception("can't delete file/directory "+bucketName+"/"+objectName+", file/directory does not exist");
			
			// do we have a directory, a file or both?
			boolean hasDir=false,hasFile=false;
			for(int i=0;i<kids.length;i++){
				if(kids[i].getKey().startsWith(nameDir)) hasDir=true;
				else if(kids[i].getKey().equals(nameFile)) hasFile=true;
			}
			
			ObjectKeyAndVersion[] keys;
			
			// we have a file, but not a directory
			if(hasFile && !hasDir) {
				s.deleteObject(bucketName, nameFile);
				return;
			}
			
			// we have both
			else if(hasDir && hasFile) {
				// we have a file
				if(objectName.equals(nameFile)) {
					s.deleteObject(bucketName, nameFile);
					return;
				}
				keys = toObjectKeyAndVersions(kids,nameFile);
			}
			// only directories
			else {
				keys = toObjectKeyAndVersions(kids,null);
			}
			
			
			if(!force && (keys.length>1 || (keys.length==1 && keys[0].getKey().indexOf('/')!=-1)))
				throw new S3Exception("can't delete directory "+bucketName+"/"+objectName+", directory is not empty");
			
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
			getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, new S3Object(trgObjectName), false);
		} catch (ServiceException se) {
			if(get(trgBucketName)!=null)throw toS3Exception(se);
			
			S3BucketWrapper so = get(srcBucketName);
			String loc = so.getLocation();
			
			createDirectory(trgBucketName, null, loc);
			try {
				getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, new S3Object(trgObjectName), false);
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
			getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, new S3Object(trgObjectName), false);
		} catch (ServiceException se) {
			if(get(trgBucketName)!=null)throw toS3Exception(se);
			
			S3BucketWrapper so = get(srcBucketName);
			String loc = so.getLocation();
			
			createDirectory(trgBucketName, null, loc);
			try {
				getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, new S3Object(trgObjectName), false);
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
	

	
	public void setAccessControlPolicy(String bucketName, String objectName, AccessControlPolicy acp) {
		//TODO
	}

	public AccessControlPolicy getAccessControlPolicy(String bucketName, String objectName) {
		//TODO
		return null;
	}
	
	
	/*
	
	String greeting = "Hello World!";
	S3Object helloWorldObject = new S3Object("HelloWorld2.txt");
	ByteArrayInputStream greetingIS = new ByteArrayInputStream(greeting.getBytes());
	helloWorldObject.setDataInputStream(greetingIS);
	helloWorldObject.setContentLength(
	    greeting.getBytes(Constants.DEFAULT_ENCODING).length);
	helloWorldObject.setContentType("text/plain");
*/
	
	/*public void write(String bucketName,String objectName, Resource res) throws IOException {
		try {
			S3Object so = new S3Object(objectName);
			so.setContentLength(res.length());
			
			ContentType ct = CFMLEngineFactory.getInstance().getResourceUtil().getContentType(res);
			if(ct!=null){
				so.setContentType(ct.getMimeType());
				if(ct.getCharset()!=null)so.setContentEncoding(ct.getCharset());
			}
			if(!res.exists()) 
					throw new FileNotFoundException("Cannot read from file: " + res.getAbsolutePath());
			so.setDataInputFile(file);
			//so.setMd5Hash(ServiceUtils.computeMD5Hash(res.getInputStream()));
			
			getS3Service().putObject(bucketName, so);
		}
		catch (S3ServiceException se) {
			throw toS3Exception(se);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}*/
	
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
	
	/*public static String toLocation(int storage, String defaultValue) {
		switch(storage) {// TODO add support for more location by converting the lucee interface to string
			case S3Constants.STORAGE_EU: return S3Bucket.LOCATION_EUROPE;
			case S3Constants.STORAGE_US:return S3Bucket.LOCATION_US;
			case S3Constants.STORAGE_US_WEST:return S3Bucket.LOCATION_US_WEST;
		}
		return defaultValue;
	}*/
	
	
	

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
	
	private ObjectKeyAndVersion[] toObjectKeyAndVersions(StorageObject[] src, String ignoreKey) {
		List<ObjectKeyAndVersion> trg=new ArrayList<ObjectKeyAndVersion>();
		for(int i=0;i<src.length;i++){
			if(ignoreKey!=null && src[i].getKey().equals(ignoreKey)) continue;
			trg.add(new ObjectKeyAndVersion(src[i].getKey()));
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
	

	private static String improveBucketName(String bucketName) throws S3Exception {
		if(bucketName.startsWith("/"))bucketName=bucketName.substring(1);
		if(bucketName.endsWith("/"))bucketName=bucketName.substring(0, bucketName.length()-1);
		if(bucketName.indexOf('/')!=-1) throw new S3Exception("invalid bucket name ["+bucketName+"]");
		return bucketName;
	}

	private static String improveObjectName(String objectName) throws S3Exception {
		if(objectName.startsWith("/"))objectName=objectName.substring(1);
		return objectName;
	}
	
	private static String improveObjectName(String objectName, boolean directory) throws S3Exception {
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
	
	private static byte[] max1000(File file) throws IOException {
		FileInputStream in = new FileInputStream(file);
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		try {
			final byte[] buffer = new byte[1000];
			int len;
			if((len = in.read(buffer)) != -1)
				out.write(buffer, 0, len);
		}
		finally {
			Util.closeEL(in);
			Util.closeEL(out);
		}
		return out.toByteArray();
	}
	
	public void releaseCache(String innerPath) {
		info.clear();
	}


	public void setInfo(String path, S3Info data) {
		this.info.put(path,data);
	}


	public S3Info getInfo(String path) {
		return this.info.get(path);
	}




	public AccessControlPolicy getACP(String path) {
		return null;// TODO acps.get(toKey(path));
	}

	public void setACP(String path,AccessControlPolicy acp) {
		// TODO acps.put(toKey(path),acp);
	}
	
	public boolean getCustomCredentials() {
		return customCredentials;
	}
	public void setCustomCredentials(boolean customCredentials) {
		this.customCredentials=customCredentials;
	}
}
