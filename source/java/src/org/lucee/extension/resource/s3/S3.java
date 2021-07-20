package org.lucee.extension.resource.s3;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.FactoryConfigurationError;

import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
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
import org.jets3t.service.utils.MultipartUtils;
import org.lucee.extension.resource.s3.info.AccessDeniedBukcet;
import org.lucee.extension.resource.s3.info.NotExisting;
import org.lucee.extension.resource.s3.info.ParentObject;
import org.lucee.extension.resource.s3.info.S3BucketInfo;
import org.lucee.extension.resource.s3.info.S3BucketWrapper;
import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.info.StorageObjectWrapper;
import org.lucee.extension.resource.s3.util.XMLUtil;

import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.net.s3.Properties;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Decision;

public class S3 {

	static {
		XMLUtil.validateDocumentBuilderFactory();
	}
	private static final ConcurrentHashMap<String, Object> tokens = new ConcurrentHashMap<String, Object>();

	public static final String DEFAULT_HOST = "s3.amazonaws.com";

	private static final long MAX_PART_SIZE = 1024 * 1024 * 1024;
	private static final long MAX_PART_SIZE_FOR_CHARACTERS = 900 * 1024 * 1024;

	private final String host;
	private final String secretAccessKey;
	private final String accessKeyId;
	private final boolean customCredentials;
	private final String mappingName;
	private final long timeout;

	private RestS3Service service;

	/////////////////////// CACHE ////////////////
	private ValidUntilMap<S3BucketWrapper> buckets;
	private Map<String, S3BucketExists> existBuckets;
	private final Map<String, ValidUntilMap<S3Info>> objects = new ConcurrentHashMap<String, ValidUntilMap<S3Info>>();
	Map<String, ValidUntilElement<AccessControlList>> accessControlLists = new ConcurrentHashMap<String, ValidUntilElement<AccessControlList>>();
	private Map<String, S3Info> exists = new ConcurrentHashMap<String, S3Info>();

	/////////////////////////////////////////////

	public S3(S3Properties props, long timeout) {
		this.host = props.getHost();
		this.secretAccessKey = props.getSecretAccessKey();
		this.accessKeyId = props.getAccessKeyId();
		this.timeout = timeout;
		this.customCredentials = props.getCustomCredentials();
		this.mappingName = props.getMappingName();
		// new Throwable().printStackTrace();
	}

	public String getHost() {
		return host;
	}

	private void flush() {
		if (service == null) return;
		try {
			service.shutdown();
		}
		catch (ServiceException e) {
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
		}
		service = null;
	}

	public String getAccessKeyId() {
		return accessKeyId;
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}

	public S3Bucket createDirectory(String bucketName, AccessControlList acl, String location) throws S3Exception {
		// flushExists(bucketName);

		bucketName = improveBucketName(bucketName);
		String l = improveLocation(location);
		try {
			S3Bucket bucket;
			if (!Util.isEmpty(l, true)) {
				if (acl != null) bucket = getS3Service().createBucket(bucketName, l, acl);
				else bucket = getS3Service().createBucket(bucketName, l);
			}
			else bucket = getS3Service().createBucket(bucketName);
			// buckets.put(bucketName, new S3BucketWrapper(bucket, validUntil()));
			flushExists(bucketName);
			return bucket;
		}
		catch (ServiceException se) {
			throw toS3Exception(se, "could not create the bucket [" + bucketName
					+ "], please consult the following website to learn about Bucket Restrictions and limitations: https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html");
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	private long validUntil() {
		return System.currentTimeMillis() + timeout;
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
		if (Util.isEmpty(objectName)) {
			createDirectory(bucketName, acl, location);
			return;
		}

		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, true);
		flushExists(bucketName, objectName);

		S3Object object = new S3Object("object");
		object.addMetadata("Content-Type", "application/x-directory");
		if (acl != null) object.setAcl(acl);

		objectName = improveObjectName(objectName, true);
		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			S3Object so = getS3Service().putObject(bucketName, object);
			flushExists(bucketName, objectName);
		}
		catch (ServiceException se) {
			if (exists(bucketName, true)) throw toS3Exception(se);

			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				S3Object so = getS3Service().putObject(bucket, object);
				flushExists(bucketName, objectName);
			}
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public void createFile(String bucketName, String objectName, AccessControlList acl, String location) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);

		flushExists(bucketName, objectName);

		S3Object object = new S3Object("object");
		if (acl != null) object.setAcl(acl);

		object.setName(objectName);
		// Upload the object to our test bucket in S3.
		try {
			getS3Service().putObject(bucketName, object);
			flushExists(bucketName, objectName);
		}
		catch (ServiceException se) {
			if (exists(bucketName, true)) throw toS3Exception(se);

			S3Bucket bucket = createDirectory(bucketName, acl, location);
			try {
				getS3Service().putObject(bucket, object);
				flushExists(bucketName, objectName);
			}
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
			catch (FactoryConfigurationError fce) {
				XMLUtil.validateDocumentBuilderFactory();
				throw fce;
			}
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public S3Object getData(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		try {
			return getS3Service().getObject(bucketName, objectName);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public InputStream getInputStream(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);

		try {
			return getData(bucketName, objectName).getDataInputStream();
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public StorageObject getInfo(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		S3Info info = get(bucketName, objectName);
		if (info instanceof StorageObjectWrapper) return ((StorageObjectWrapper) info).getStorageObject();
		return null;
	}

	public List<S3Info> list(boolean recursive, boolean listPseudoFolder) throws S3Exception {
		try {

			// no cache for buckets
			if (timeout <= 0 || buckets == null || buckets.validUntil < System.currentTimeMillis()) {
				S3Bucket[] s3buckets = getS3Service().listAllBuckets();
				long now = System.currentTimeMillis();
				buckets = new ValidUntilMap<S3BucketWrapper>(now + timeout);
				for (int i = 0; i < s3buckets.length; i++) {
					buckets.put(s3buckets[i].getName(), new S3BucketWrapper(s3buckets[i], now + timeout));
				}
			}

			List<S3Info> list = new ArrayList<S3Info>();
			Iterator<S3BucketWrapper> it = buckets.values().iterator();
			S3Info info;
			while (it.hasNext()) {
				info = it.next();
				list.add(info);
				if (recursive) {
					Iterator<S3Info> iit = list(info.getBucketName(), "", recursive, listPseudoFolder, true).iterator();
					while (iit.hasNext()) {
						list.add(iit.next());
					}
				}
			}
			return list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	/**
	 * list all allements in a specific bucket
	 * 
	 * @param bucketName name of the bucket
	 * @param recursive show all objects (recursive==true) or direct kids
	 * @param listPseudoFolder if recursive false also list the "folder" of objects with sub folders
	 * @return
	 * @throws S3Exception
	 * 
	 *             public List<S3Info> list(String bucketName, boolean recursive, boolean
	 *             listPseudoFolder) throws S3Exception { return list(bucketName, "", recursive,
	 *             listPseudoFolder); }
	 */

	public List<S3Info> list(String bucketName, String objectName, boolean recursive, boolean listPseudoFolder, boolean onlyChildren) throws S3Exception {
		return list(bucketName, objectName, recursive, listPseudoFolder, onlyChildren, false);
	}

	public List<S3Info> list(String bucketName, String objectName, boolean recursive, boolean listPseudoFolder, boolean onlyChildren, boolean noCache) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		ValidUntilMap<S3Info> objects = _list(bucketName, objectName, onlyChildren, noCache);

		Iterator<S3Info> it = objects.values().iterator();
		Map<String, S3Info> map = new LinkedHashMap<String, S3Info>();
		S3Info info;
		while (it.hasNext()) {
			info = it.next();
			add(map, info, objectName, recursive, listPseudoFolder, onlyChildren);
		}
		Iterator<S3Info> iit = map.values().iterator();
		List<S3Info> list = new ArrayList<S3Info>();
		while (iit.hasNext()) {
			list.add(iit.next());
		}
		return list;
	}

	public S3Object[] listObjects(String bucketName) throws S3Exception {
		try {
			return getS3Service().listObjects(improveBucketName(bucketName));
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	private void add(Map<String, S3Info> map, S3Info info, String prefix, boolean recursive, boolean addPseudoEntries, boolean onlyChildren) throws S3Exception {
		String nameFile = improveObjectName(info.getObjectName(), false);
		int index, last = 0;
		ParentObject tmp;
		String objName;
		S3Info existing;

		if (addPseudoEntries) {
			while ((index = nameFile.indexOf('/', last)) != -1) {
				tmp = new ParentObject(nameFile.substring(0, index + 1), info);
				if (doAdd(tmp, prefix, recursive, onlyChildren)) {
					objName = improveObjectName(tmp.getObjectName(), false);
					existing = map.get(objName);
					if (existing == null) {
						map.put(objName, tmp);
					}
				}
				last = index + 1;
			}
		}

		if (doAdd(info, prefix, recursive, onlyChildren)) {
			objName = improveObjectName(info.getObjectName(), false);
			existing = map.get(objName);
			if (existing == null || existing instanceof ParentObject) map.put(objName, info);
		}
	}

	private boolean doAdd(S3Info info, String prefix, boolean recursive, boolean onlyChildren) throws S3Exception {
		prefix = improveObjectName(prefix, false);
		String name = improveObjectName(info.getObjectName(), false);

		// no parents
		if (prefix.length() > name.length()) return false;

		// only children
		if (onlyChildren && prefix.equals(name)) return false;

		// no grand ... children
		if (!recursive && !isDirectKid(info.getObjectName(), prefix)) return false;

		return true;
	}

	private static boolean isDirectKid(String name, String prefix) throws S3Exception {
		if (prefix == null) prefix = "";
		if (name == null) name = "";

		prefix = prefix.length() == 0 ? "" : improveObjectName(prefix, true);
		String sub = improveObjectName(name.substring(prefix.length()), false);
		return sub.indexOf('/') == -1;
	}

	private ValidUntilMap<S3Info> _list(String bucketName, String objectName, boolean onlyChildren, boolean noCache) throws S3Exception {
		try {
			String key = toKey(bucketName, objectName);
			String nameDir = improveObjectName(objectName, true);
			String nameFile = improveObjectName(objectName, false);
			boolean hasObjName = !Util.isEmpty(objectName);

			// not cached
			ValidUntilMap<S3Info> _list = timeout <= 0 || noCache ? null : objects.get(key);
			if (_list == null || _list.validUntil < System.currentTimeMillis()) {
				S3Object[] kids = hasObjName ? getS3Service().listObjects(bucketName, nameFile, ",") : getS3Service().listObjects(bucketName);

				long validUntil = System.currentTimeMillis() + timeout;
				_list = new ValidUntilMap<S3Info>(validUntil);
				objects.put(key, _list);

				// add bucket
				if (!hasObjName && !onlyChildren) {
					S3Bucket b = getS3Service().getBucket(bucketName);
					_list.put("", new S3BucketWrapper(b, validUntil));
				}

				StorageObjectWrapper tmp;
				String name;
				for (S3Object kid: kids) {
					name = kid.getName();
					tmp = new StorageObjectWrapper(this, kid, bucketName, validUntil);

					if (!hasObjName || name.equals(nameFile) || name.startsWith(nameDir)) _list.put(kid.getKey(), tmp);
					exists.put(toKey(kid.getBucketName(), name), tmp);

					int index;
					while ((index = name.lastIndexOf('/')) != -1) {
						name = name.substring(0, index);
						exists.put(toKey(bucketName, name), new ParentObject(bucketName, name, null, validUntil));
					}
				}

			}
			return _list;
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
	}

	private String toKey(String bucketName, String objectName) {
		if (objectName == null) objectName = "";
		return improveBucketName(bucketName) + ":" + improveObjectName(objectName, false);
	}

	public boolean exists(String bucketName, boolean defaultValue) throws S3Exception {
		try {
			return exists(bucketName);
		}
		catch (Exception e) {
			return defaultValue;
		}
	}

	public boolean exists(String bucketName) throws S3Exception {
		bucketName = improveBucketName(bucketName);

		// this will load it and cache if necessary
		try {
			return get(bucketName) != null;
		}
		catch (S3Exception s3e) {
			if (isAccessDenied(s3e)) return existsNotTouchBucketItself(bucketName);
			throw s3e;
		}
	}

	private boolean isAccessDenied(S3Exception s3e) {
		String msg = s3e.getMessage();
		if (msg != null) { // i do not like that a lot, but the class of the exception is generic
			msg = msg.toLowerCase();
			if (msg.contains("access") && msg.contains("denied")) { // let's try to get the bucket in a different way
				return true;
			}
		}
		return false;
	}

	private boolean existsNotTouchBucketItself(String bucketName) throws S3Exception {

		long now = System.currentTimeMillis();
		if (existBuckets != null) {
			S3BucketExists info = existBuckets.get(bucketName);
			if (info != null) {
				if (info.validUntil >= now) {
					return info.exists;
				}
				else existBuckets.remove(bucketName);
			}
		}
		else existBuckets = new ConcurrentHashMap<String, S3BucketExists>();
		S3Service s = getS3Service();
		try { // delete the content of the bucket
				// in case bucket does not exist, it will throw an error
			s.listObjects(bucketName, "sadasdsadasdasasdasd", null, Constants.DEFAULT_OBJECT_LIST_CHUNK_SIZE);
			existBuckets.put(bucketName, new S3BucketExists(bucketName, now + timeout, true));
			return true;
		}
		catch (ServiceException se) {
			existBuckets.put(bucketName, new S3BucketExists(bucketName, now + timeout, false));
			return false;
		}
	}

	public boolean exists(String bucketName, String objectName) throws S3Exception {
		if (Util.isEmpty(objectName)) return exists(bucketName);
		S3Info info = get(bucketName, objectName);
		return info != null && info.exists();
	}

	public boolean isDirectory(String bucketName, String objectName) throws S3Exception {
		if (Util.isEmpty(objectName)) return exists(bucketName); // bucket is always adirectory
		S3Info info = get(bucketName, objectName);
		if (info == null || !info.exists()) return false;
		return info.isDirectory();
	}

	public boolean isFile(String bucketName, String objectName) throws S3Exception {
		if (Util.isEmpty(objectName)) return false; // bucket is newer a file
		S3Info info = get(bucketName, objectName);
		if (info == null || !info.exists()) return false;
		return info.isFile();
	}

	public S3Info get(String bucketName, final String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		String nameFile = improveObjectName(objectName, false);
		String nameDir = improveObjectName(objectName, true);
		// cache
		S3Info info = timeout <= 0 ? null : exists.get(toKey(bucketName, nameFile));
		if (info != null && info.validUntil() >= System.currentTimeMillis()) {
			if (info instanceof NotExisting) return null;
			return info;
		}
		info = null;
		try {
			StorageObjectsChunk chunk = listObjectsChunkedSilent(bucketName, nameFile, 10, null);

			long validUntil = System.currentTimeMillis() + timeout;
			StorageObject[] objects = chunk == null ? null : chunk.getObjects();

			if (objects == null || objects.length == 0) {
				exists.put(toKey(bucketName, objectName), new NotExisting(bucketName, objectName, null, validUntil)); // we do not return this, we just store it to cache that it
																														// does
				return null;
			}

			String targetName;
			StorageObject stoObj = null;
			// direct match
			for (StorageObject so: objects) {
				targetName = so.getName();
				if (nameFile.equals(targetName) || nameDir.equals(targetName)) {
					exists.put(toKey(bucketName, nameFile), info = new StorageObjectWrapper(this, stoObj = so, bucketName, validUntil));
				}
			}

			// pseudo directory?
			if (info == null) {
				for (StorageObject so: objects) {
					targetName = so.getName();
					if (nameDir.length() < targetName.length() && targetName.startsWith(nameDir)) {
						exists.put(toKey(bucketName, nameFile), info = new ParentObject(bucketName, nameDir, null, validUntil));
					}
				}
			}

			for (StorageObject obj: objects) {
				if (stoObj != null && stoObj.equals(obj)) continue;
				exists.put(toKey(obj.getBucketName(), obj.getName()), new StorageObjectWrapper(this, obj, bucketName, validUntil));
			}

			if (info == null) {
				exists.put(toKey(bucketName, objectName), new NotExisting(bucketName, objectName, null, validUntil) // we do not return this, we just store it to cache that it does
																													// not exis
				);
			}
			return info;
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public StorageObjectsChunk listObjectsChunkedSilent(String bucketName, String objectName, int max, String priorLastKey) {
		try {
			return getS3Service().listObjectsChunked(bucketName, objectName, ",", max, priorLastKey);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
		catch (Exception e) {
		}
		return null;
	}

	public S3BucketInfo get(String bucketName) throws S3Exception {
		bucketName = improveBucketName(bucketName);

		// buckets cache
		S3BucketWrapper info = null;
		if (buckets != null && timeout > 0) {
			info = buckets.get(bucketName);
			if (info != null && info.validUntil() >= System.currentTimeMillis()) return info;
		}

		// this will load it and cache if necessary
		List<S3Info> list;
		try {
			list = list(false, false);
		}
		catch (S3Exception s3e) {
			if (isAccessDenied(s3e) && existsNotTouchBucketItself(bucketName)) return new AccessDeniedBukcet(bucketName, timeout + System.currentTimeMillis(), s3e);
			throw s3e;
		}

		Iterator<S3Info> it = list.iterator();
		while (it.hasNext()) {
			info = (S3BucketWrapper) it.next();
			if (info.getBucketName().equals(bucketName)) return info;
		}
		return null;
	}

	public void delete(String bucketName, boolean force) throws S3Exception {
		bucketName = improveBucketName(bucketName);

		S3Service s = getS3Service();
		try {
			// delete the content of the bucket
			if (force) {
				S3Object[] children = s.listObjects(bucketName);
				if (children.length > 0) s.deleteMultipleObjects(bucketName, toObjectKeyAndVersions(children));
			}
			s.deleteBucket(bucketName);
			flushExists(bucketName);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public void delete(String bucketName, String objectName, boolean force) throws S3Exception {
		if (Util.isEmpty(objectName, true)) {
			delete(bucketName, force);
			return;
		}
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		String nameFile = improveObjectName(objectName, false);
		String nameDir = improveObjectName(objectName, true);
		try {
			S3Service s = getS3Service();
			List<S3Info> list = list(bucketName, nameFile, true, false, false);

			// no file and directory with this name
			if (list.size() == 0) {
				throw new S3Exception("can't delete file/directory " + bucketName + "/" + objectName + ", file/directory does not exist");
			}

			ObjectKeyAndVersion[] keys = toObjectKeyAndVersions(list, null);
			if (!force
					&& (keys.length > 1 || (keys.length == 1 && keys[0].getKey().length() > nameDir.length() && keys[0].getKey().substring(nameDir.length()).indexOf('/') != -1))) {
				throw new S3Exception("can't delete directory " + bucketName + "/" + objectName + ", directory is not empty");
			}

			// clear cache
			Iterator<S3Info> it = list.iterator();
			S3Info info;
			while (it.hasNext()) {
				info = it.next();
				flushExists(info.getBucketName(), info.getObjectName());
			}
			// we create parent because before it maybe was a pseudi dir
			s.deleteMultipleObjects(bucketName, keys);
			flushExists(bucketName, objectName);
			createParentDirectory(bucketName, objectName, true);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
		finally {
			flushExists(bucketName, objectName);
		}
	}

	/*
	 * removes all objects inside a bucket, unless maxAge is bigger than 0, in that case only objects
	 * are removed that exists for a longer time than max age.
	 * 
	 * @bucketName name of the bucket to clear
	 * 
	 * @maxAge max age of the objects to keep
	 */
	public void clear(String bucketName, long maxAge) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		S3Service s = getS3Service();
		try {
			S3Object[] children = s.listObjects(bucketName);
			if (children != null && children.length > 0) {
				ObjectKeyAndVersion[] filtered = toObjectKeyAndVersions(children, maxAge);
				if (filtered != null && filtered.length > 0) {
					ObjectKeyAndVersion[][] blocks = toBlocks(filtered);
					for (ObjectKeyAndVersion[] block: blocks) {
						s.deleteMultipleObjects(bucketName, block);
					}

					flushExists(bucketName);
				}
			}
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	private static ObjectKeyAndVersion[][] toBlocks(ObjectKeyAndVersion[] filtered) {
		List<ObjectKeyAndVersion[]> list = new ArrayList<>();
		int blockSize = 1000;
		if (filtered.length <= blockSize) {
			return new ObjectKeyAndVersion[][] { filtered };
		}
		boolean doBreak = false;
		int count = 0;
		int from = 0, to = 0;
		while (true) {
			if (count++ > 5) break;
			if (from + blockSize < filtered.length) {
				to = from + blockSize - 1;
			}
			else {
				to = filtered.length - 1;
				doBreak = true;
			}
			list.add(Arrays.copyOfRange(filtered, from, to + 1));
			if (doBreak) break;
			from = to + 1;
		}
		return list.toArray(new ObjectKeyAndVersion[list.size()][blockSize]);
	}

	private void createParentDirectory(String bucketName, String objectName, boolean noCache) throws S3Exception {
		String parent = getParent(objectName);
		if (parent == null) return;
		if (noCache) flushExists(bucketName, parent);
		if (!exists(bucketName, parent)) createDirectory(bucketName, parent, null, null);
	}

	private String getParent(String objectName) throws S3Exception {
		objectName = improveObjectName(objectName, false);
		int index = objectName.lastIndexOf('/');
		if (index == -1) return null;
		return objectName.substring(0, index + 1);
	}

	public void copy(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName) throws S3Exception {
		srcBucketName = improveBucketName(srcBucketName);
		srcObjectName = improveObjectName(srcObjectName, false);
		trgBucketName = improveBucketName(trgBucketName);
		trgObjectName = improveObjectName(trgObjectName, false);
		flushExists(srcBucketName, srcObjectName);
		flushExists(trgBucketName, trgObjectName);
		try {
			S3Object trg = new S3Object(trgObjectName);
			getS3Service().copyObject(srcBucketName, srcObjectName, trgBucketName, trg, false);
			flushExists(trgBucketName, trgObjectName);

		}
		catch (ServiceException se) {
			if (get(trgBucketName) != null) throw toS3Exception(se);

			S3Info so = get(srcBucketName);
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
			catch (FactoryConfigurationError fce) {
				XMLUtil.validateDocumentBuilderFactory();
				throw fce;
			}
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	private void flushExists(String bucketName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		String prefix = bucketName + ":";
		buckets = null;
		_flush(exists, prefix, null);
		_flush(objects, prefix, null);
		existBuckets.remove(bucketName);
	}

	private void flushExists(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		String nameDir = improveObjectName(objectName, true);
		String nameFile = improveObjectName(objectName, false);
		String exact = bucketName + ":" + nameFile;
		String prefix = bucketName + ":" + nameDir;
		String prefix2 = bucketName + ":";

		_flush(exists, prefix, exact);
		_flush(objects, prefix2, exact);
	}

	private static void _flush(Map<String, ?> map, String prefix, String exact) {
		if (map == null) return;

		Iterator<String> it = map.keySet().iterator();
		String key;
		while (it.hasNext()) {
			key = it.next();
			if (key == null) continue;
			if ((exact != null && key.equals(exact)) || (prefix != null && key.startsWith(prefix))) {
				map.remove(key);
			}
		}
	}

	public void move(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName) throws S3Exception {
		srcBucketName = improveBucketName(srcBucketName);
		srcObjectName = improveObjectName(srcObjectName, false);
		trgBucketName = improveBucketName(trgBucketName);
		trgObjectName = improveObjectName(trgObjectName, false);
		flushExists(srcBucketName, srcObjectName);
		flushExists(trgBucketName, trgObjectName);

		try {
			S3Object trg = new S3Object(trgObjectName);
			getS3Service().moveObject(srcBucketName, srcObjectName, trgBucketName, trg, false);

			flushExists(srcBucketName, srcObjectName);
			flushExists(trgBucketName, trgObjectName);
		}
		catch (ServiceException se) {
			if (get(trgBucketName) != null) throw toS3Exception(se);

			S3Info so = get(srcBucketName);
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
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
		createParentDirectory(srcBucketName, srcObjectName, true);
	}

	public void write(String bucketName, String objectName, String data, String mimeType, String charset, AccessControlList acl, String location) throws IOException {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);
		flushExists(bucketName, objectName);

		try {
			S3Object so = new S3Object(objectName, data);
			String ct = toContentType(mimeType, charset, null);
			if (ct != null) so.setContentType(ct);

			// if we reach that threshold we need to get more percise
			boolean split = false;
			if (data.length() >= MAX_PART_SIZE_FOR_CHARACTERS) {
				if (Util.isEmpty(charset)) {
					if (data.getBytes().length >= MAX_PART_SIZE) split = true;
				}
				else {
					if (data.getBytes(charset).length >= MAX_PART_SIZE) split = true;
				}
			}

			_write(so, bucketName, objectName, acl, location, split);
			flushExists(bucketName, objectName);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}

	public void write(String bucketName, String objectName, byte[] data, String mimeType, AccessControlList acl, String location) throws IOException {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);
		flushExists(bucketName, objectName);

		try {
			S3Object so = new S3Object(objectName, data);
			if (!Util.isEmpty(mimeType)) so.setContentType(mimeType);

			_write(so, bucketName, objectName, acl, location, data.length >= MAX_PART_SIZE);
			flushExists(bucketName, objectName);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}

	public void write(String bucketName, String objectName, File file, AccessControlList acl, String location) throws IOException {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);
		flushExists(bucketName, objectName);

		try {
			S3Object so = new S3Object(file);
			String mt = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(max1000(file), null);
			if (mt != null) so.setContentType(mt);

			_write(so, bucketName, objectName, acl, location, file.length() >= MAX_PART_SIZE);
			flushExists(bucketName, objectName);
		}
		catch (NoSuchAlgorithmException e) {
			CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}
	}

	private void _write(S3Object so, String bucketName, String objectName, AccessControlList acl, String location, boolean split) throws IOException {
		try {

			so.setName(objectName);

			if (acl != null) so.setAcl(acl);
			if (split) multiPut(bucketName, so);
			else getS3Service().putObject(bucketName, so);
		}
		catch (S3ServiceException se) {
			// does the bucket exist? if so we throw the exception
			if (get(bucketName) != null) throw toS3Exception(se);
			// if the bucket does not exist, we do create it
			createDirectory(bucketName, acl, location);
			// now we try again
			try {
				if (split) multiPut(bucketName, so);
				else getS3Service().putObject(bucketName, so);
			}
			catch (S3ServiceException e) {
				throw toS3Exception(se);
			}
		}
	}

	private void multiPut(String bucketName, S3Object so) throws IOException {
		List<StorageObject> objectsToUploadAsMultipart = new ArrayList<>();
		objectsToUploadAsMultipart.add(so);
		try {
			new MultipartUtils(MAX_PART_SIZE).uploadObjects(bucketName, getS3Service(), objectsToUploadAsMultipart, null // eventListener : Provide
																															// one to monitor the upload
			// progress
			);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
		}

	}

	public Struct getMetaData(String bucketName, String objectName) throws S3Exception {
		Struct sct = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		;

		S3Info info;
		if (Util.isEmpty(objectName)) info = get(bucketName);
		else info = get(bucketName, objectName);

		if (info == null || info.isVirtual()) throw new S3Exception("there is no object [" + objectName + "] in bucket [" + bucketName + "]");

		StorageOwner owner = info.getOwner();
		String location = info.getLocation();
		Map<String, Object> md = info.getMetaData();

		Iterator<Entry<String, Object>> it = md.entrySet().iterator();
		Entry<String, Object> e;
		while (it.hasNext()) {
			e = it.next();
			sct.setEL(e.getKey().replace('-', '_'), e.getValue());
		}

		// owner
		if (owner != null) sct.put("owner", owner.getDisplayName());
		if (location != null) sct.put("location", location);

		return sct;
	}

	public void setLastModified(String bucketName, String objectName, long time) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		if (!Util.isEmpty(objectName)) {
			S3Info info = get(bucketName, objectName);
			if (info == null || info.isVirtual()) throw new S3Exception("there is no object [" + objectName + "] in bucket [" + bucketName + "]");

			StorageObject so = ((StorageObjectWrapper) info).getStorageObject();
			so.setLastModifiedDate(new Date(time));
		}
	}

	public void setMetaData(String bucketName, String objectName, Struct metadata) throws PageException, S3Exception {
		Iterator<Entry<Key, Object>> it = metadata.entryIterator();
		Entry<Key, Object> e;
		Map<String, Object> data = new ConcurrentHashMap<String, Object>();
		Decision dec = CFMLEngineFactory.getInstance().getDecisionUtil();
		Cast cas = CFMLEngineFactory.getInstance().getCastUtil();
		Object value;
		while (it.hasNext()) {
			e = it.next();
			value = e.getValue();
			if (dec.isDate(value, false)) value = cas.toDate(value, null);
			else value = cas.toString(value);
			data.put(toMetaDataKey(e.getKey()), value);
		}

		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		if (Util.isEmpty(objectName)) {
			S3BucketInfo bw = get(bucketName);
			if (bw == null) throw new S3Exception("there is no bucket [" + bucketName + "]");
			S3Bucket b = bw.getBucket();
			b.addAllMetadata(data); // INFO seems not to be possible at all
		}
		else {
			S3Info info = get(bucketName, objectName);

			if (info == null || info.isVirtual()) throw new S3Exception("there is no object [" + objectName + "] in bucket [" + bucketName + "]");

			StorageObject so = ((StorageObjectWrapper) info).getStorageObject();
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
		if (key.equals("content-disposition")) return "Content-Disposition";
		if (key.equals("content_disposition")) return "Content-Disposition";
		if (key.equals("content disposition")) return "Content-Disposition";
		if (key.equals("contentdisposition")) return "Content-Disposition";

		if (key.equals("content-encoding")) return "Content-Encoding";
		if (key.equals("content_encoding")) return "Content-Encoding";
		if (key.equals("content encoding")) return "Content-Encoding";
		if (key.equals("contentencoding")) return "Content-Encoding";

		if (key.equals("content-language")) return "Content-Language";
		if (key.equals("content_language")) return "Content-Language";
		if (key.equals("content language")) return "Content-Language";
		if (key.equals("contentlanguage")) return "Content-Language";

		if (key.equals("content-length")) return "Content-Length";
		if (key.equals("content_length")) return "Content-Length";
		if (key.equals("content length")) return "Content-Length";
		if (key.equals("contentlength")) return "Content-Length";

		if (key.equals("content-md5")) return "Content-MD5";
		if (key.equals("content_md5")) return "Content-MD5";
		if (key.equals("content md5")) return "Content-MD5";
		if (key.equals("contentmd5")) return "Content-MD5";

		if (key.equals("content-type")) return "Content-Type";
		if (key.equals("content_type")) return "Content-Type";
		if (key.equals("content type")) return "Content-Type";
		if (key.equals("contenttype")) return "Content-Type";

		if (key.equals("last-modified")) return "Last-Modified";
		if (key.equals("last_modified")) return "Last-Modified";
		if (key.equals("last modified")) return "Last-Modified";
		if (key.equals("lastmodified")) return "Last-Modified";

		if (key.equals("md5_hash")) return "md5-hash";
		if (key.equals("md5_hash")) return "md5-hash";
		if (key.equals("md5_hash")) return "md5-hash";
		if (key.equals("md5_hash")) return "md5-hash";

		if (key.equals("date")) return "Date";
		if (key.equals("etag")) return "ETag";

		return key.getString();
	}

	public void addAccessControlList(String bucketName, String objectName, Object objACL) throws S3Exception, PageException {
		try {
			bucketName = improveBucketName(bucketName);
			objectName = improveObjectName(objectName);

			S3Service s = getS3Service();
			S3Bucket b = service.getBucket(bucketName);
			if (b == null) throw new S3Exception("there is no bucket with name [" + bucketName + "]");

			AccessControlList acl = getACL(s, b, objectName);
			acl.grantAllPermissions(AccessControlListUtil.toGrantAndPermissions(objACL));
			b.setAcl(acl);
			setACL(s, bucketName, objectName, acl);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public void setAccessControlList(String bucketName, String objectName, Object objACL) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		try {
			S3Service s = getS3Service();
			bucketName = improveBucketName(bucketName);
			S3Bucket b = service.getBucket(bucketName);
			if (b == null) throw new S3Exception("there is no bucket with name [" + bucketName + "]");

			AccessControlList oldACL = getACL(s, b, objectName);
			AccessControlList newACL = AccessControlListUtil.toAccessControlList(objACL);

			StorageOwner aclOwner = oldACL != null ? oldACL.getOwner() : s.getAccountOwner();
			newACL.setOwner(aclOwner);
			setACL(s, b.getName(), objectName, newACL);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public Array getAccessControlList(String bucketName, String objectName) throws S3Exception {
		AccessControlList acl = getACL(bucketName, objectName);
		return AccessControlListUtil.toArray(acl.getGrantAndPermissions());
	}

	private AccessControlList getACL(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		String key = toKey(bucketName, objectName);

		ValidUntilElement<AccessControlList> vuacl = accessControlLists.get(key);
		if (vuacl != null && vuacl.validUntil > System.currentTimeMillis()) return vuacl.element;

		try {
			if (Util.isEmpty(objectName)) return getS3Service().getBucketAcl(bucketName);
			return getS3Service().getObjectAcl(bucketName, objectName);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	private AccessControlList getACL(S3Service s, S3Bucket bucket, String objectName) throws S3Exception {
		objectName = improveObjectName(objectName);

		String key = toKey(bucket.getName(), objectName);
		ValidUntilElement<AccessControlList> vuacl = accessControlLists.get(key);
		if (vuacl != null && vuacl.validUntil > System.currentTimeMillis()) return vuacl.element;

		try {
			if (Util.isEmpty(objectName)) return s.getBucketAcl(bucket);
			return s.getObjectAcl(bucket, objectName);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	public void setACL(S3Service s, String bucketName, String objectName, AccessControlList acl) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);
		String key = toKey(bucketName, objectName);

		try {
			if (Util.isEmpty(objectName)) s.putBucketAcl(bucketName, acl);
			else s.putObjectAcl(bucketName, objectName, acl);

			accessControlLists.remove(key);
		}
		catch (ServiceException se) {
			throw toS3Exception(se);
		}
		catch (FactoryConfigurationError fce) {
			XMLUtil.validateDocumentBuilderFactory();
			throw fce;
		}
	}

	private String toContentType(String mimeType, String charset, String defaultValue) {
		if (!Util.isEmpty(mimeType)) {
			return !Util.isEmpty(charset) ? mimeType + "; charset=" + charset : mimeType;
		}
		return defaultValue;
	}

	public String url(String bucketName, String objectName, long time) throws S3Exception {
		return getS3Service().createSignedGetUrl(bucketName, objectName, new Date(System.currentTimeMillis() + time), false);
	}

	private S3Service getS3Service() {

		if (service == null) {
			synchronized (getToken(accessKeyId + ":" + secretAccessKey)) {
				if (service == null) {
					if (host != null && !host.isEmpty() && !host.equalsIgnoreCase(DEFAULT_HOST)) {
						final Jets3tProperties props = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME);
						props.setProperty("s3service.s3-endpoint", host);
						service = new RestS3Service(new AWSCredentials(accessKeyId, secretAccessKey), null, null, props);
					}
					else {
						service = new RestS3Service(new AWSCredentials(accessKeyId, secretAccessKey));
					}
				}
			}
		}
		return service;
	}

	private void reset() {
		synchronized (getToken(accessKeyId + ":" + secretAccessKey)) {
			// we set srvice to null, so getS3Service has to wait
			RestS3Service tmp = service;
			service = null;

			try {
				tmp.shutdown();
			}
			catch (ServiceException e) {
				e.printStackTrace(); // TODO log it
			}
		}

	}

	public static AccessControlList toACL(String acl, AccessControlList defaultValue) {
		if (acl == null) return defaultValue;

		acl = acl.trim().toLowerCase();

		if ("public-read".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		if ("public read".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		if ("public_read".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;
		if ("publicread".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ;

		if ("private".equals(acl)) return AccessControlList.REST_CANNED_PRIVATE;

		if ("public-read-write".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
		if ("public read write".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
		if ("public_read_write".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;
		if ("publicreadwrite".equals(acl)) return AccessControlList.REST_CANNED_PUBLIC_READ_WRITE;

		if ("authenticated-read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if ("authenticated read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if ("authenticated_read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if ("authenticatedread".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;

		if ("authenticate-read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if ("authenticate read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if ("authenticate_read".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;
		if ("authenticateread".equals(acl)) return AccessControlList.REST_CANNED_AUTHENTICATED_READ;

		return defaultValue;
	}

	public static AccessControlList toACL(Properties prop, AccessControlList defaultValue) {
		try {
			Method m = prop.getClass().getMethod("getACL", new Class[0]);
			String str = CFMLEngineFactory.getInstance().getCastUtil().toString(m.invoke(prop, new Object[0]), null);
			if (Util.isEmpty(str)) return defaultValue;
			return toACL(str, defaultValue);
		}
		catch (Exception e) {
		}
		return defaultValue;
	}

	private ObjectKeyAndVersion[] toObjectKeyAndVersions(S3Object[] src) {
		ObjectKeyAndVersion[] trg = new ObjectKeyAndVersion[src.length];
		for (int i = 0; i < src.length; i++) {
			trg[i] = new ObjectKeyAndVersion(src[i].getKey(), src[i].getVersionId());
		}
		return trg;
	}

	private ObjectKeyAndVersion[] toObjectKeyAndVersions(S3Object[] src, long maxAge) {
		if (maxAge <= 0) return toObjectKeyAndVersions(src);
		List<ObjectKeyAndVersion> trg = new ArrayList<>();
		long now = System.currentTimeMillis();
		for (int i = 0; i < src.length; i++) {
			if (now > (src[i].getLastModifiedDate().getTime() + maxAge)) trg.add(new ObjectKeyAndVersion(src[i].getKey(), src[i].getVersionId()));
		}
		return trg.toArray(new ObjectKeyAndVersion[trg.size()]);
	}

	private ObjectKeyAndVersion[] toObjectKeyAndVersions(List<S3Info> src, String ignoreKey) {
		List<ObjectKeyAndVersion> trg = new ArrayList<ObjectKeyAndVersion>();
		Iterator<S3Info> it = src.iterator();
		S3Info info;
		while (it.hasNext()) {
			info = it.next();
			if (ignoreKey != null && info.getName().equals(ignoreKey)) continue;
			trg.add(new ObjectKeyAndVersion(info.getName()));
		}
		return trg.toArray(new ObjectKeyAndVersion[trg.size()]);
	}

	private S3Exception toS3Exception(ServiceException se) {
		return toS3Exception(se, null);
	}

	private S3Exception toS3Exception(ServiceException se, String detail) {
		String msg = se.getErrorMessage();
		if (Util.isEmpty(msg)) msg = se.getMessage();

		S3Exception ioe = Util.isEmpty(detail) ? new S3Exception(msg) : new S3Exception(msg + ";" + detail);
		ioe.initCause(se);
		ioe.setStackTrace(se.getStackTrace());
		return ioe;
	}

	public static String improveBucketName(String bucketName) {
		if (bucketName.startsWith("/")) bucketName = bucketName.substring(1);
		if (bucketName.endsWith("/")) bucketName = bucketName.substring(0, bucketName.length() - 1);
		if (bucketName.indexOf('/') != -1) throw new RuntimeException(new S3Exception("invalid bucket name [" + bucketName + "]"));
		return bucketName;
	}

	public static String improveObjectName(String objectName) {
		if (objectName == null) return null;
		if (objectName.startsWith("/")) objectName = objectName.substring(1);
		return objectName;
	}

	public static String improveObjectName(String objectName, boolean directory) {
		if (objectName == null) objectName = "";
		if (objectName.startsWith("/")) objectName = objectName.substring(1);
		if (directory) {
			if (!objectName.endsWith("/")) objectName = objectName + "/";
		}
		else if (objectName.endsWith("/")) objectName = objectName.substring(0, objectName.length() - 1);
		return objectName;
	}

	public static String improveLocation(String location) {
		if (location == null) return location;
		location = location.toLowerCase().trim();
		if ("usa".equals(location)) return "us";
		if ("u.s.".equals(location)) return "us";
		if ("u.s.a.".equals(location)) return "us";
		if ("united states of america".equals(location)) return "us";

		if ("europe.".equals(location)) return "eu";
		if ("euro.".equals(location)) return "eu";
		if ("e.u.".equals(location)) return "eu";

		if ("usa-west".equals(location)) return "us-west";

		return location;
	}

	private byte[] max1000(File file) throws IOException {
		FileInputStream in = new FileInputStream(file);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			final byte[] buffer = new byte[1000];
			int len;
			if ((len = in.read(buffer)) != -1) out.write(buffer, 0, len);
		}
		finally {
			Util.closeEL(in, out);
		}
		return out.toByteArray();
	}

	public static Object getToken(String key) {
		Object newLock = new Object();
		Object lock = tokens.putIfAbsent(key, newLock);
		if (lock == null) {
			lock = newLock;
		}
		return lock;
	}

	public boolean getCustomCredentials() {
		return customCredentials;
	}

	public String getMappingName() {
		return mappingName;
	}

	class ValidUntilMap<I> extends ConcurrentHashMap<String, I> {
		private static final long serialVersionUID = 238079099294942075L;
		private final long validUntil;

		public ValidUntilMap(long validUntil) {
			this.validUntil = validUntil;
		}
	}

	class ValidUntilList extends ArrayList<S3Info> {

		private static final long serialVersionUID = -1928364430347198544L;
		private final long validUntil;

		public ValidUntilList(long validUntil) {
			this.validUntil = validUntil;
		}
	}

	class ValidUntilElement<E> {
		private final long validUntil;
		private final E element;
		// private Boolean isDirectory;

		public ValidUntilElement(E element, long validUntil) {
			this.element = element;
			this.validUntil = validUntil;
			// this.isDirectory=isDirectory;
		}
	}

	class EL {

		private final boolean match;
		private final boolean isDirectory;
		private final boolean isObject;

		public EL(boolean match, boolean isDirectory, boolean isObject) {
			this.match = match;
			this.isDirectory = isDirectory;
			this.isObject = isObject;
		}
	}

	class ObjectFilter {

		private final String objectNameWithoutSlash;
		private final String objectNameWithSlash;
		private boolean recursive;

		public ObjectFilter(String objectName, boolean recursive) throws S3Exception {
			this.objectNameWithoutSlash = improveObjectName(objectName, false);
			this.objectNameWithSlash = improveObjectName(objectName, true);
			this.recursive = recursive;
		}

		public boolean accept(String objectName) {

			if (!objectNameWithoutSlash.equals(objectName) && !objectName.startsWith(objectNameWithSlash)) return false;

			if (!recursive && objectName.length() > objectNameWithSlash.length()) {
				String sub = objectName.substring(objectNameWithSlash.length());
				int index = sub.indexOf('/');
				if (index != -1 && index + 1 < sub.length()) return false;
			}
			return true;
		}
	}

	public static class S3BucketExists {
		private final String name;
		private final long validUntil;
		private final boolean exists;

		public S3BucketExists(String name, long validUntil, boolean exists) {
			this.name = name;
			this.validUntil = validUntil;
			this.exists = exists;
		}
	}
}