package org.lucee.extension.resource.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.resource.s3.info.NotExisting;
import org.lucee.extension.resource.s3.info.ParentObject;
import org.lucee.extension.resource.s3.info.S3BucketWrapper;
import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.info.StorageObjectWrapper;
import org.lucee.extension.resource.s3.region.RegionFactory;
import org.lucee.extension.resource.s3.region.RegionFactory.Region;
import org.lucee.extension.resource.s3.util.XMLUtil;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;

import lucee.commons.io.log.Log;
import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.net.s3.Properties;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Query;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Creation;
import lucee.runtime.util.Strings;

public class S3 {
	private static final short CHECK_EXISTS = 1;
	private static final short CHECK_IS_DIR = 2;
	private static final short CHECK_IS_FILE = 4;

	public static final short URI_STYLE_VIRTUAL_HOST = 1;
	public static final short URI_STYLE_PATH = 2;
	public static final short URI_STYLE_S3 = 4;
	public static final short URI_STYLE_ARN = 8;
	private static long maxSize = 100 * 1024 * 1024;

	static {
		XMLUtil.validateDocumentBuilderFactory();
	}

	public static final String DEFAULT_HOST = "s3.amazonaws.com";
	public static final long DEFAULT_LIVE_TIMEOUT = 600000L;
	public static final String GOOGLE = ".googleapis.com";
	public static final String WASABI = ".wasabisys.com";
	public static final String AWS = ".amazonaws.com";
	public static final String BACKBLAZE = ".backblazeb2.com";
	public static final String DREAM_IO = ".dream.io";

	public static final String[] PROVIDERS = new String[] { AWS, WASABI, BACKBLAZE, ".digitaloceanspaces.com", DREAM_IO, GOOGLE };

	private static final ConcurrentHashMap<String, Object> tokens = new ConcurrentHashMap<String, Object>();

	private static Map<String, S3> instances = new ConcurrentHashMap<String, S3>();
	private static Map<String, S3Cache> caches = new ConcurrentHashMap<String, S3Cache>();

	private final String host;
	private final String accessKeyId;
	private final String secretAccessKey;
	private String defaultRegion;

	private final long cacheTimeout;
	private final long liveTimeout;

	private final S3Cache cache;

	private Log log;

	public static S3 getInstance(S3Properties props, long cache) {

		String keyS3 = props.getAccessKeyId() + ":" + props.getSecretAccessKey() + ":" + props.getHost() + ":" + props.getDefaultLocation() + ":" + cache;
		S3 s3 = instances.get(keyS3);
		if (s3 == null) {
			synchronized (instances) {
				s3 = instances.get(keyS3);
				if (s3 == null) {

					String keyCache = props.getAccessKeyId() + ":" + props.getSecretAccessKey() + ":" + props.getHostWithoutRegion() + ":" + cache;
					S3Cache c = caches.get(keyCache);
					if (c == null) {
						synchronized (caches) {
							c = caches.get(keyCache);
							if (c == null) {
								caches.put(keyCache, c = new S3Cache(getLog()));
							}
						}
					}
					instances.put(keyS3, s3 = new S3(c, props.getAccessKeyId(), props.getSecretAccessKey(), props.getHost(), props.getDefaultLocation(), cache,
							S3.DEFAULT_LIVE_TIMEOUT, true, getLog()));
				}
			}
		}
		return s3;
	}

	private static Log getLog() {
		try {
			return CFMLEngineFactory.getInstance().getThreadConfig().getLog("application");
		}
		catch (Exception e) {
		}
		return null;
	}

	/**
	 * 
	 * @param c
	 * @param props S3 Properties
	 * @param timeout
	 * @param defaultRegion region used to create new buckets in case no bucket is defined
	 * @param cacheRegions
	 * @param log
	 * @throws S3Exception
	 */
	private S3(S3Cache cache, String accessKeyId, String secretAccessKey, String host, String defaultLocation, long cacheTimeout, long liveTimeout, boolean cacheRegions, Log log) {
		this.cache = cache;
		this.accessKeyId = accessKeyId;
		this.secretAccessKey = secretAccessKey;
		this.host = host;
		this.cacheTimeout = cacheTimeout;
		this.liveTimeout = liveTimeout;
		if (!Util.isEmpty(defaultLocation, true)) {
			try {
				defaultRegion = toString(RegionFactory.getInstance(defaultLocation));
			}
			catch (S3Exception e) {
				defaultRegion = defaultLocation;
			}
		}

		if (cacheRegions) {
			new CacheRegions().start();
		}
		this.log = log;
	}

	public String getHost() {
		return host;
	}

	public String getAccessKeyId() {
		com.amazonaws.util.AWSRequestMetrics d;
		return accessKeyId;
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}

	/**
	 * 
	 * @param bucketName name of the bucket to create
	 * @param acl access control list
	 * @param region region (location) of the new bucket, if not defined, the default region defined
	 *            with the constructor is used
	 * @return the bucket created
	 * @throws S3Exception
	 */
	public Bucket createDirectory(String bucketName, Object acl, String targetRegion) throws S3Exception {
		// flushExists(bucketName);
		bucketName = improveBucketName(bucketName);
		targetRegion = improveLocation(targetRegion);

		CreateBucketRequest cbr = new CreateBucketRequest(bucketName);
		if (acl != null) setACL(cbr, acl);

		try {
			String region;
			if (Util.isEmpty(targetRegion)) region = defaultRegion;
			else region = targetRegion;

			Bucket b;
			AmazonS3Client client = getAmazonS3(null, region);
			try {
				b = client.createBucket(cbr);
			}
			catch (AmazonServiceException ase) {
				// TODO better way to handle this situation
				// The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'us-east-2'
				if (Util.isEmpty(targetRegion) && "AuthorizationHeaderMalformed".equals(ase.getErrorCode()) && ase.getErrorMessage().indexOf("is wrong; expecting") != -1) {
					String expectedRegion = extractExpectedRegion(ase.getErrorMessage(), ase);
					if (expectedRegion != null && !expectedRegion.equalsIgnoreCase(defaultRegion)) {
						client = getAmazonS3(null, expectedRegion);
						b = client.createBucket(cbr);
						return b;
					}
				}
				if (!Util.isEmpty(targetRegion)) throw new S3Exception("could not create bucket [" + bucketName + "] with defined region [" + targetRegion + "]", ase);
				if (!Util.isEmpty(region)) throw new S3Exception("could not create bucket [" + bucketName + "] with region [" + region + "]", ase);
				throw new S3Exception("could not create bucket [" + bucketName + "]", ase);
			}
			finally {
				client.release();
			}

			if (!Util.isEmpty(region)) {
				cache.bucketRegions.put(bucketName, RegionFactory.getInstance(region));
			}
			flushExists(bucketName, false);
			return b;
		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se, "could not create the bucket [" + bucketName
					+ "], please consult the following website to learn about Bucket Restrictions and limitations: https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html");
		}
	}

	private String extractExpectedRegion(String msg, AmazonServiceException ase) {

		// The authorization header is malformed; the region 'us-east-1' is wrong; expecting 'us-east-2'
		int startIndex = msg.indexOf("expecting '");
		if (startIndex != -1) {
			int endIndex = msg.indexOf("'", startIndex + 11);
			if (endIndex > startIndex) {
				try {
					return toString(RegionFactory.getInstance(msg.substring(startIndex + 11, endIndex)));
				}
				catch (S3Exception e) {
					if (log != null) log.error("s3", e);
					else e.printStackTrace();
				}
			}
		}
		throw ase;
	}

	private long validUntil() {
		return System.currentTimeMillis() + cacheTimeout;
	}

	/**
	 * 
	 * @param bucketName target bucket to create if not already exists
	 * @param objectName object to create
	 * @param acl access control list
	 * @param region if bucket is not already existing, it get created using that region, if it is not
	 *            defined the default region defined with the constructor is used
	 * @throws S3Exception
	 */
	public void createDirectory(String bucketName, String objectName, Object acl, final String region) throws S3Exception {
		if (Util.isEmpty(objectName)) {
			createDirectory(bucketName, acl, region);
			return;
		}

		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, true);
		flushExists(bucketName, objectName);
		try {
			// create meta-data for your folder and set content-length to 0
			ObjectMetadata md = new ObjectMetadata();
			md.setContentType("application/x-directory");
			md.setContentLength(0L);
			md.setLastModified(new Date());

			// create a PutObjectRequest passing the folder name suffixed by /
			PutObjectRequest por = new PutObjectRequest(bucketName, objectName, new ByteArrayInputStream(new byte[0]), md);
			if (acl != null) setACL(por, acl);

			// send request to S3 to create folder
			AmazonS3Client client = getAmazonS3(bucketName, region);
			try {
				client.putObject(por);
				flushExists(bucketName, objectName);
			}
			catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equals("NoSuchBucket")) {
					createDirectory(bucketName, acl, region);
					client.putObject(por);
					flushExists(bucketName, objectName);
				}
				else throw toS3Exception(ase);
			}
			finally {
				client.release();
			}

		}
		catch (AmazonServiceException ase) {
			throw toS3Exception(ase);
		}
	}

	/**
	 * 
	 * @param bucketName target bucket to create if not already exists
	 * @param objectName object to create
	 * @param acl access control list
	 * @param region if bucket is not already existing, it get created using that region, if it is not
	 *            defined the default region defined with the constructor is used
	 * @throws S3Exception
	 */
	public void createFile(String bucketName, String objectName, Object acl, String region) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);

		flushExists(bucketName, objectName);

		ObjectMetadata md = new ObjectMetadata();
		// md.setContentType("application/x-directory");
		// md.setContentLength(0L);
		md.setLastModified(new Date());
		md.setContentLength(0L);

		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest por = new PutObjectRequest(bucketName, objectName, new ByteArrayInputStream(new byte[0]), md);
		if (acl != null) setACL(por, acl);
		try {
			AmazonS3Client client = getAmazonS3(bucketName, region);
			// send request to S3 to create folder
			try {
				client.putObject(por);
				flushExists(bucketName, objectName);
			}
			catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equals("NoSuchBucket")) {
					createDirectory(bucketName, acl, region);
					client.putObject(por);
					flushExists(bucketName, objectName);
				}
				else throw toS3Exception(ase);
			}
			finally {
				client.release();
			}

		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
	}

	public S3Object getData(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			return client.getObject(bucketName, objectName);
		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
		finally {
			client.release();
		}
	}

	public InputStream getInputStream(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);

		try {
			return getData(bucketName, objectName).getObjectContent();
		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
	}

	public S3ObjectSummary getInfo(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		S3Info info = get(bucketName, objectName);
		if (info instanceof StorageObjectWrapper) {
			return fixBackBlazeBug(((StorageObjectWrapper) info).getStorageObject(), bucketName);
		}
		return null;
	}

	/**
	 * Generates a pre-signed URL for Amazon S3 operations with various custom parameters.
	 * 
	 * @param bucketName The name of the S3 bucket.
	 * @param objectName The key of the S3 object.
	 * @param expireDate The expiration date for the pre-signed URL.
	 * @param httpMethod The desired HTTP method (e.g., GET, PUT). Defaults to GET.
	 * @param sseAlgorithm The desired server-side encryption algorithm, valid values are AES256 or KMS.
	 * @param sseCustomerKey The server-side encryption customer-provided key.
	 * @param checksum The base64 encoded MD5 checksum of the object's content.
	 * @param contentType The MIME type of the object (e.g., "text/plain").
	 * @param contentDisposition Specifies presentational information for the object, like "attachment;
	 *            filename=\"filename.pdf\"".
	 * @param contentEncoding Specifies content encodings applied to the object, like gzip.
	 * @param versionId The version ID of the object if versioning is enabled.
	 * @param zeroByteContent A flag to specify if the object has zero-byte content.
	 * @param customResponseHeaders Struct of custom response headers for custom metadata prefixed with
	 *            "x-amz-meta-" (prefix is optional).
	 * 
	 * @return The generated pre-signed URL.
	 * 
	 * @throws S3Exception If there's an issue generating the pre-signed URL or invalid input
	 *             parameters.
	 */
	public URL generatePresignedURL(String bucketName, String objectName, Date expireDate, String httpMethod, String sseAlgorithm, String sseCustomerKey, String checksum,
			String contentType, String contentDisposition, String contentEncoding, String versionId, Boolean zeroByteContent, Struct customResponseHeaders) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		// http method
		HttpMethod method;
		if (Util.isEmpty(httpMethod, true)) method = HttpMethod.GET;
		else {
			String tmp = httpMethod.trim().toUpperCase();
			if ("DELETE".equals(tmp)) method = HttpMethod.DELETE;
			else if ("GET".equals(tmp)) method = HttpMethod.GET;
			else if ("HEAD".equals(tmp)) method = HttpMethod.HEAD;
			else if ("PATCH".equals(tmp)) method = HttpMethod.PATCH;
			else if ("POST".equals(tmp)) method = HttpMethod.POST;
			else if ("PUT".equals(tmp)) method = HttpMethod.PUT;
			else throw new S3Exception("invalid http method defintion [" + httpMethod + "], valid values are [DELETE, GET, HEAD, PATCH, POST, PUT]");
		}

		// sse algorithm
		SSEAlgorithm algorithm = null;
		if (!Util.isEmpty(sseAlgorithm, true)) {
			String tmp = sseAlgorithm.trim().toUpperCase();
			if ("AES256".equals(tmp)) algorithm = SSEAlgorithm.AES256;
			else if ("KMS".equals(tmp)) algorithm = SSEAlgorithm.KMS;
			else throw new S3Exception("invalid SSE Algorithm defintion [" + sseAlgorithm + "], valid values are [AES256,KMS]");
		}

		ResponseHeaderOverrides headers = null;
		SSECustomerKey key;
		// sse key
		if (Util.isEmpty(sseCustomerKey, true)) key = null;
		else key = new SSECustomerKey(sseCustomerKey.trim());

		// checksum
		if (Util.isEmpty(checksum, true)) checksum = null;
		else checksum = checksum.trim();

		// content disposition
		if (!Util.isEmpty(contentDisposition, true)) {
			if (headers == null) headers = new ResponseHeaderOverrides();
			headers.setContentDisposition(contentDisposition.trim()); // example input: attachment; filename=\"filename.pdf\"
		}

		// content encoding
		if (!Util.isEmpty(contentEncoding, true)) {
			if (headers == null) headers = new ResponseHeaderOverrides();
			headers.setContentEncoding(contentEncoding.trim()); // example input: gzip
		}

		// content type
		if (Util.isEmpty(contentType, true)) contentType = null;
		else {
			if (headers != null) {
				headers.setContentType(contentType.trim());
				contentType = null; // not necessary to set this separatly anymore
			}
			else contentType = contentType.trim();
		}

		// version id
		if (Util.isEmpty(versionId, true)) versionId = null;
		else versionId = versionId.trim();

		// . in bucket?
		boolean isDotInBucket = bucketName.indexOf('.') != -1;

		AmazonS3Client client = getAmazonS3(bucketName, null, isDotInBucket);
		try {
			GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, objectName).withMethod(method);
			if (versionId != null) {
				generatePresignedUrlRequest.withVersionId(versionId);
			}
			if (contentType != null) {
				generatePresignedUrlRequest.withContentType(contentType);
			}
			if (checksum != null) {
				generatePresignedUrlRequest.withContentMd5(checksum);
			}
			if (zeroByteContent != null) {
				generatePresignedUrlRequest.withZeroByteContent(zeroByteContent);
			}

			if (headers != null) {
				generatePresignedUrlRequest.withResponseHeaders(headers);
			}

			if (algorithm != null) {
				generatePresignedUrlRequest.withSSEAlgorithm(algorithm);
				if (key != null) {
					generatePresignedUrlRequest.withSSECustomerKey(key);
				}
			}

			if (expireDate != null) {
				if (expireDate.getTime() < System.currentTimeMillis()) throw new S3Exception("the optional expire date must be un the future");
				generatePresignedUrlRequest.withExpiration(expireDate);
			}

			if (customResponseHeaders != null && !customResponseHeaders.isEmpty()) {
				Iterator<Entry<Key, Object>> it = customResponseHeaders.entryIterator();
				Entry<Key, Object> e;
				String name;
				CFMLEngine eng = CFMLEngineFactory.getInstance();
				Strings util = CFMLEngineFactory.getInstance().getStringUtil();
				Cast caster = eng.getCastUtil();
				while (it.hasNext()) {
					e = it.next();
					name = e.getKey().getString();
					if (!util.startsWithIgnoreCase(name, "x-amz-meta-")) name = "x-amz-meta-" + name;
					generatePresignedUrlRequest.addRequestParameter(name, caster.toString(e.getValue()));
				}
			}

			return client.generatePresignedUrl(generatePresignedUrlRequest);
		}
		catch (PageException pe) {
			throw toS3Exception(pe);
		}
		catch (AmazonServiceException ase) {
			throw toS3Exception(ase);
		}
		finally {
			client.release();
		}

	}

	public String generateURI(String bucketName, String objectName, short uriStyle, boolean secure) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);
		HostData hd = toHostData(getHost());

		if (URI_STYLE_VIRTUAL_HOST == uriStyle) {
			// pattern https://bucket-name.s3.Region.amazonaws.com/key-name
			// example https://my-bucket.s3.us-west-2.amazonaws.com/puppy.png
			S3Info info = get(bucketName);
			if (info == null) throw new S3Exception("no accesible bucket with name [" + bucketName + "] ");
			String region = toString(info.getRegion());
			return new StringBuilder().append(secure ? "https://" : "http://").append(bucketName).append(".s3.").append(region).append('.').append(hd.domain).append('/')
					.append(objectName).toString();
		}
		else if (URI_STYLE_PATH == uriStyle) {
			// pattern https://s3.Region.amazonaws.com/bucket-name/key-name
			// example https://s3.us-west-2.amazonaws.com/mybucket/puppy.jpg
			S3Info info = get(bucketName);
			if (info == null) throw new S3Exception("no accesible bucket with name [" + bucketName + "] ");
			String region = toString(info.getRegion());
			return new StringBuilder().append(secure ? "https://" : "http://").append("s3.").append(region).append('.').append(hd.domain).append('/').append(bucketName).append('/')
					.append(objectName).toString();
		}
		else if (URI_STYLE_S3 == uriStyle) {
			// pattern S3://bucket-name/key-name
			return new StringBuilder().append("s3://").append(bucketName).append('/').append(objectName).toString();
		}
		else if (URI_STYLE_ARN == uriStyle) {

			// pattern arn:aws:s3:::bucket_name/key_name
			return new StringBuilder().append("arn:aws:s3:::").append(bucketName).append('/').append(objectName).toString();
		}
		else {
			throw new S3Exception("invalid URI Style definition.");
		}
	}

	// https://s3.eu-central-1.wasabisys.com/lucee-ldev0359-43d24e88acb9b5048097f8961fc5b23c/a
	public List<S3Info> list(boolean recursive, boolean listPseudoFolder) throws S3Exception {
		AmazonS3Client client = null;
		try {
			// no cache for buckets
			if (cacheTimeout <= 0 || cache.buckets == null || cache.buckets.validUntil < System.currentTimeMillis()) {
				client = getAmazonS3(null, null);

				List<Bucket> s3buckets = client.listBuckets();
				long now = System.currentTimeMillis();
				cache.buckets = new ValidUntilMap<S3BucketWrapper>(now + cacheTimeout);
				for (Bucket s3b: s3buckets) {
					cache.buckets.put(s3b.getName(), new S3BucketWrapper(this, s3b, now + cacheTimeout, log));
				}
			}

			List<S3Info> list = new ArrayList<S3Info>();
			Iterator<S3BucketWrapper> it = cache.buckets.values().iterator();
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
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
		finally {
			if (client != null) client.release();
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

	/*
	 * public List<S3ObjectSummary> listObjectSummaries(String bucketName) throws S3Exception {
	 * AmazonS3Client client = getAmazonS3(bucketName, null); try { ObjectListing objects =
	 * client.listObjects(bucketName); // Recursively delete all the objects inside given bucket
	 * List<S3ObjectSummary> summeries = new ArrayList<>(); if (objects != null &&
	 * objects.getObjectSummaries() != null) { while (true) { for (S3ObjectSummary summary:
	 * objects.getObjectSummaries()) { fixBackBlazeBug(summary, bucketName); summeries.add(summary); }
	 * if (objects.isTruncated()) { objects = client.listNextBatchOfObjects(objects); } else { break; }
	 * } } return summeries;
	 * 
	 * } catch (AmazonServiceException ase) { throw toS3Exception(ase); } finally { client.release(); }
	 * }
	 */

	public List<S3Object> listObjects(String bucketName) throws S3Exception {
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			ObjectListing objects = client.listObjects(bucketName);
			/* Recursively delete all the objects inside given bucket */
			List<S3Object> list = new ArrayList<>();
			if (objects != null) {
				List<S3ObjectSummary> summeries = objects.getObjectSummaries();
				if (summeries != null) {
					while (true) {
						for (S3ObjectSummary summary: summeries) {
							fixBackBlazeBug(summary, bucketName);
							list.add(client.getObject(bucketName, summary.getKey()));
						}
						if (objects.isTruncated()) {
							objects = client.listNextBatchOfObjects(objects);
						}
						else {
							break;
						}
					}
				}
			}
			return list;
		}
		catch (AmazonServiceException ase) {
			throw toS3Exception(ase);
		}
		finally {
			client.release();
		}
	}

	public Query listBucketsAsQuery() throws S3Exception, PageException {
		AmazonS3Client client = getAmazonS3(null, null);
		try {
			CFMLEngine eng = CFMLEngineFactory.getInstance();
			Creation creator = eng.getCreationUtil();

			final Key nameKey = creator.createKey("name");
			final Key size = creator.createKey("size");
			final Key lastModified = creator.createKey("lastModified");
			final Key ownerKey = creator.createKey("owner");
			Query qry = eng.getCreationUtil().createQuery(new Key[] { nameKey, ownerKey, lastModified }, 0, "buckets");

			List<Bucket> listBuckets = client.listBuckets();

			if (listBuckets != null && listBuckets.size() > 0) {
				int row;
				Owner owner;
				for (Bucket b: listBuckets) {
					if (b == null) continue;
					row = qry.addRow();
					qry.setAt(nameKey, row, b.getName());
					owner = b.getOwner();
					if (owner != null) qry.setAt(ownerKey, row, owner.getDisplayName());
					qry.setAt(lastModified, row, b.getCreationDate());
				}
			}

			return qry;
		}
		catch (AmazonServiceException ase) {
			throw toS3Exception(ase);
		}
		finally {
			client.release();
		}
	}

	public Query listObjectsAsQuery(String bucketName) throws S3Exception, PageException {
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			CFMLEngine eng = CFMLEngineFactory.getInstance();
			Creation creator = eng.getCreationUtil();

			final Key objectName = creator.createKey("objectName");
			final Key size = creator.createKey("size");
			final Key lastModified = creator.createKey("lastModified");
			final Key owner = creator.createKey("owner");
			Query qry = eng.getCreationUtil().createQuery(new Key[] { objectName, size, lastModified, owner }, 0, "buckets");

			ObjectListing objects = client.listObjects(bucketName);
			/* Recursively delete all the objects inside given bucket */
			List<S3Object> list = new ArrayList<>();

			if (objects != null && objects.getObjectSummaries() != null) {
				int row;
				while (true) {
					for (S3ObjectSummary summary: objects.getObjectSummaries()) {
						fixBackBlazeBug(summary, bucketName);
						row = qry.addRow();
						qry.setAt(objectName, row, summary.getKey());
						qry.setAt(lastModified, row, summary.getLastModified());
						qry.setAt(size, row, summary.getSize());
						qry.setAt(owner, row, summary.getOwner().getDisplayName());
					}
					if (objects.isTruncated()) {
						objects = client.listNextBatchOfObjects(objects);
					}
					else {
						break;
					}
				}
			}

			return qry;
		}
		catch (AmazonServiceException ase) {
			throw toS3Exception(ase);
		}
		finally {
			client.release();
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
				tmp = new ParentObject(this, nameFile.substring(0, index + 1), info, log);
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
		if (prefix.equals(improveObjectName(name, true))) return true;
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
			ValidUntilMap<S3Info> _list = cacheTimeout <= 0 || noCache ? null : cache.objects.get(key);
			if (_list == null || _list.validUntil < System.currentTimeMillis()) {
				long validUntil = System.currentTimeMillis() + cacheTimeout;
				_list = new ValidUntilMap<S3Info>(validUntil);
				cache.objects.put(key, _list);

				// add bucket
				if (!hasObjName && !onlyChildren) {
					outer: while (true) {
						AmazonS3Client client = getAmazonS3(null, null);
						try {
							for (Bucket b: client.listBuckets()) {
								// TOD is there a more direct way?
								if (b.getName().equals(bucketName)) {
									_list.put("", new S3BucketWrapper(this, b, validUntil, log));
									break outer;
								}
							}
						}
						finally {
							client.release();
						}
						throw new S3Exception("could not find bucket [" + bucketName + "]");// should never happen!
					}
				}
				AmazonS3Client client = getAmazonS3(bucketName, null);
				ObjectListing list = (hasObjName ? client.listObjects(bucketName, nameFile) : client.listObjects(bucketName));
				try {
					if (list != null && list.getObjectSummaries() != null) {
						while (true) {
							List<S3ObjectSummary> kids = list.getObjectSummaries();
							StorageObjectWrapper tmp;
							String name;
							for (S3ObjectSummary kid: kids) {
								fixBackBlazeBug(kid, bucketName);
								name = kid.getKey();

								tmp = new StorageObjectWrapper(this, kid, validUntil, log);

								if (!hasObjName || name.equals(nameFile) || name.startsWith(nameDir)) _list.put(kid.getKey(), tmp);
								cache.exists.put(toKey(bucketName, name), tmp);
								cache.harakiri.touch();
								int index;
								while ((index = name.lastIndexOf('/')) != -1) {
									name = name.substring(0, index);
									cache.exists.put(toKey(bucketName, name), new ParentObject(this, bucketName, name, validUntil, log));
									cache.harakiri.touch();
								}
							}

							if (list.isTruncated()) {
								list = client.listNextBatchOfObjects(list);
							}
							else {
								break;
							}
						}
					}
				}
				finally {
					client.release();
				}
			}
			return _list;
		}
		catch (AmazonS3Exception ase) {
			throw toS3Exception(ase);
		}
	}

	private S3ObjectSummary fixBackBlazeBug(S3ObjectSummary kid, String bucketName) {
		// the method getBucketName does not deliver the bucket name
		if (kid != null && Util.isEmpty(kid.getBucketName()) && !Util.isEmpty(bucketName)) kid.setBucketName(bucketName);
		return kid;
	}

	private String toKey(String bucketName, String objectName) throws S3Exception {
		if (objectName == null) objectName = "";
		return improveBucketName(bucketName) + ":" + improveObjectName(objectName, false);
	}

	private boolean exists(String bucketName, boolean defaultValue) throws S3Exception {
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
		// getS3Service().doesBucketExistV2(bucketName);
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
		if (cache.existBuckets != null) {
			S3BucketExists info = cache.existBuckets.get(bucketName);
			if (info != null) {
				if (info.validUntil >= now) {
					return info.exists;
				}
				else cache.existBuckets.remove(bucketName);
			}
		}
		else cache.existBuckets = new ConcurrentHashMap<String, S3BucketExists>();
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try { // delete the content of the bucket
				// in case bucket does not exist, it will throw an error
			client.listObjects(bucketName, "sadasdsadasdasasdasd");
			cache.existBuckets.put(bucketName, new S3BucketExists(bucketName, now + cacheTimeout, true));
			return true;
		}
		catch (AmazonServiceException se) {
			if (se.getErrorCode().equals("NoSuchBucket")) {
				cache.existBuckets.put(bucketName, new S3BucketExists(bucketName, now + cacheTimeout, false));
				return false;
			}
			throw toS3Exception(se);
		}
		finally {
			client.release();
		}
	}

	public boolean isDirectory(String bucketName, String objectName) throws S3Exception {
		return is(bucketName, objectName, CHECK_IS_DIR);
	}

	public boolean isFile(String bucketName, String objectName) throws S3Exception {
		return is(bucketName, objectName, CHECK_IS_FILE);
	}

	public boolean exists(String bucketName, String objectName) throws S3Exception {
		return is(bucketName, objectName, CHECK_EXISTS);
	}

	private boolean is(String bucketName, String objectName, short type) throws S3Exception {
		if (Util.isEmpty(objectName)) return type != CHECK_IS_FILE ? exists(bucketName) : false; // bucket is always a directory
		S3Info info = get(bucketName, objectName);
		if (info == null || !info.exists()) return false;
		if (CHECK_IS_DIR == type) return info.isDirectory();
		if (CHECK_IS_FILE == type) return info.isFile();
		return info.exists();
	}

	public String getContentType(String bucketName, String objectName) throws AmazonServiceException, S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			return client.getObjectMetadata(bucketName, objectName).getContentType();
		}
		finally {
			client.release();
		}
	}

	public S3Info get(String bucketName, final String objectName) throws S3Exception {
		if (Util.isEmpty(objectName)) {
			return get(bucketName);
		}
		bucketName = improveBucketName(bucketName);
		String nameFile = improveObjectName(objectName, false);
		String nameDir = improveObjectName(objectName, true);

		// cache
		S3Info info = cacheTimeout <= 0 ? null : cache.exists.get(toKey(bucketName, nameFile));
		if (info != null && info.validUntil() >= System.currentTimeMillis()) {
			if (info instanceof NotExisting) {
				return null;
			}
			return info;
		}
		info = null;

		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			long validUntil = System.currentTimeMillis() + cacheTimeout;
			ObjectListing objects = null;
			try {

				ListObjectsRequest lor = new ListObjectsRequest();
				lor.setBucketName(bucketName);
				lor.setPrefix(nameFile);
				lor.setMaxKeys(100);

				objects = client.listObjects(lor);
			}
			catch (Exception e) {
				if (e instanceof AmazonS3Exception) {
					AmazonS3Exception ase = (AmazonS3Exception) e;
					if ("NoSuchBucket".equals(ase.getErrorCode()) || "404".equals(ase.getStatusCode())) return null;
				}
				if (log != null) log.error("s3", e);
				else e.printStackTrace();
			}

			/* Recursively delete all the objects inside given bucket */
			if (objects == null || objects.getObjectSummaries() == null || objects.getObjectSummaries().size() == 0) {
				cache.exists.put(toKey(bucketName, objectName), new NotExisting(bucketName, objectName, validUntil, log)); // we do not return this, we just store it to cache that
																															// it
				// does
				return null;
			}

			String targetName;
			S3ObjectSummary stoObj = null;
			int count = 0;
			// while (true) {
			for (S3ObjectSummary summary: objects.getObjectSummaries()) {
				fixBackBlazeBug(summary, bucketName);
				count++;
				// direct match
				targetName = summary.getKey();
				if (nameFile.equals(targetName) || nameDir.equals(targetName)) {
					cache.exists.put(toKey(bucketName, nameFile), info = new StorageObjectWrapper(this, stoObj = summary, validUntil, log));
					cache.harakiri.touch();
				}

				// pseudo directory?
				// if (info == null) {
				targetName = summary.getKey();
				if (nameDir.length() < targetName.length() && targetName.startsWith(nameDir)) {
					cache.exists.put(toKey(bucketName, nameFile), info = new ParentObject(this, bucketName, nameDir, validUntil, log));
					cache.harakiri.touch();
				}

				// set the value to exist when not a match
				if (!(stoObj != null && stoObj.equals(summary))) {
					cache.exists.put(toKey(bucketName, summary.getKey()), new StorageObjectWrapper(this, summary, validUntil, log));
					cache.harakiri.touch();
				}
				// set all the parents when not exist
				// TODO handle that also a file with that name can exist at the same time
				String parent = nameFile;
				int index;
				while ((index = parent.lastIndexOf('/')) != -1) {
					parent = parent.substring(0, index);
					cache.exists.put(toKey(bucketName, parent), new ParentObject(this, bucketName, parent, validUntil, log));
					cache.harakiri.touch();
				}

			}

			/*
			 * if ( objects.isTruncated()) { objects = s.listNextBatchOfObjects(objects); } else { break; }
			 */
			// }
			if (info == null) {
				cache.exists.put(toKey(bucketName, objectName), new NotExisting(bucketName, objectName, validUntil, log) // we do not return this, we just store it to cache that it
																															// does
				// not exis
				);
				cache.harakiri.touch();
			}
			return info;
		}
		catch (AmazonServiceException ase) {
			throw toS3Exception(ase);
		}
		finally {
			client.release();
		}
	}

	public S3BucketWrapper get(String bucketName) throws S3Exception {
		bucketName = improveBucketName(bucketName);

		// buckets cache
		S3BucketWrapper info = null;
		if (cache.buckets != null && cacheTimeout > 0) {
			info = cache.buckets.get(bucketName);
			if (info != null && info.validUntil() >= System.currentTimeMillis()) return info;
		}

		// this will load it and cache if necessary
		List<S3Info> list = list(false, false);
		Iterator<S3Info> it = list.iterator();
		while (it.hasNext()) {
			info = (S3BucketWrapper) it.next();
			if (info.getBucketName().equals(bucketName)) return info;
		}
		return null;
	}

	public void delete(String bucketName, boolean force) throws S3Exception {
		// com.amazonaws.services.s3.model.AmazonS3Exception: The bucket you tried to delete is not empty

		bucketName = improveBucketName(bucketName);

		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			if (force) {
				clear(bucketName, 0);
				client.deleteBucket(bucketName);
			}
			else {
				client.deleteBucket(bucketName);
				flushExists(bucketName, true);
			}

		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
		finally {
			client.release();
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
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			boolean matchFile = false;
			boolean matchDir = false;
			boolean matchKids = false;
			ObjectListing objects = client.listObjects(bucketName, nameFile);
			List<KeyVersion> matches = new ArrayList<>();
			if (objects != null && objects.getObjectSummaries() != null) {
				while (true) {
					for (S3ObjectSummary summary: objects.getObjectSummaries()) {
						fixBackBlazeBug(summary, bucketName);
						if (summary.getKey().equals(nameFile)) {
							matchFile = true;
							matches.add(new KeyVersion(summary.getKey()));
						}
						else if (summary.getKey().startsWith(nameDir)) {
							if (summary.getKey().length() == nameDir.length()) matchDir = true;
							else matchKids = true;
							matches.add(new KeyVersion(summary.getKey()));
						}
					}
					if (objects.isTruncated()) {
						objects = client.listNextBatchOfObjects(objects);
					}
					else {
						break;
					}
				}
			}
			else {
				throw new S3Exception("can't delete file/directory " + bucketName + "/" + objectName + ", file/directory does not exist");
			}

			if (!force && matchKids) {
				throw new S3Exception("can't delete directory " + bucketName + "/" + objectName + ", directory is not empty");
			}

			// clear cache
			Iterator<KeyVersion> it = matches.iterator();
			KeyVersion kv;
			while (it.hasNext()) {
				kv = it.next();
				flushExists(bucketName, kv.getKey());
			}
			if (matches.size() > 0) {
				DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName).withKeys(matches).withQuiet(false);
				client.deleteObjects(dor);
				flushExists(bucketName, objectName);
				// we create parent because before it maybe was a pseudi dir
				createParentDirectory(bucketName, objectName, true);
			}
			else throw new S3Exception("can't delete file/directory " + bucketName + "/" + objectName + ", file/directory does not exist");
		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
		finally {
			flushExists(bucketName, objectName);
			client.release();
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
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {

			ObjectListing objects = client.listObjects(bucketName);
			if (objects != null && objects.getObjectSummaries() != null) {
				while (true) {
					List<KeyVersion> filtered = toObjectKeyAndVersions(objects.getObjectSummaries(), maxAge);
					if (filtered != null && filtered.size() > 0) {
						DeleteObjectsRequest dor = new DeleteObjectsRequest(bucketName).withKeys(filtered).withQuiet(false);
						client.deleteObjects(dor);
					}

					if (objects.isTruncated()) {
						objects = client.listNextBatchOfObjects(objects);
					}
					else {
						break;
					}
				}
			}

			/* Get list of versions in a given bucket */
			VersionListing versions = client.listVersions(new ListVersionsRequest().withBucketName(bucketName));

			/* Recursively delete all the versions inside given bucket */
			if (versions != null && versions.getVersionSummaries() != null) {
				while (true) {
					for (S3VersionSummary summary: versions.getVersionSummaries()) {
						client.deleteObject(bucketName, summary.getKey());
					}

					if (versions.isTruncated()) {
						versions = client.listNextBatchOfVersions(versions);
					}
					else {
						break;
					}
				}
			}

			flushExists(bucketName, false);
		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
		finally {
			client.release();
		}
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

	/**
	 * 
	 * @param srcBucketName source bucket name to copy
	 * @param srcObjectName source object name to copy
	 * @param trgBucketName target bucket name to copy
	 * @param trgObjectName target object name to copy
	 * @param acl Access Control list (can be null)
	 * @param targetRegion region of the target bucket (only necessary if it differs from source and
	 *            does not exist yet)
	 * @throws S3Exception
	 */
	public void copyObject(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName, Object acl, String targetRegion) throws S3Exception {
		srcBucketName = improveBucketName(srcBucketName);
		srcObjectName = improveObjectName(srcObjectName, false);
		trgBucketName = improveBucketName(trgBucketName);
		trgObjectName = improveObjectName(trgObjectName, false);

		flushExists(srcBucketName, srcObjectName);
		flushExists(trgBucketName, trgObjectName);

		AmazonS3Client srcClient = getAmazonS3(srcBucketName, null);
		AmazonS3Client trgClient = getAmazonS3(null, targetRegion);

		// AmazonS3Client client = getAmazonS3(srcBucketName, null);

		try {
			CopyObjectRequest cor = new CopyObjectRequest(srcBucketName, srcObjectName, trgBucketName, trgObjectName);

			if (acl != null) setACL(cor, acl);
			try {
				trgClient.copyObject(cor);
			}
			catch (AmazonServiceException se) {
				// could be an Pseudo folder
				if (se.getErrorCode().equals("NoSuchKey")) {
					S3Info src = get(srcBucketName, srcObjectName);
					if (src.isVirtual()) {
						throw new S3Exception("Cannot copy virtual folder", se);
					}
					throw toS3Exception(se);
				}
				else if (se.getErrorCode().equals("NoSuchBucket") && !srcClient.doesBucketExistV2(trgBucketName)) {
					boolean customACL = true;

					if (acl == null) {
						try {
							acl = srcClient.getBucketAcl(srcBucketName);
						}
						catch (AmazonServiceException ase) {
							if ("AccessDenied".equals(ase.getErrorCode())) {
								// do nothing
							}
							else throw ase;
						}
						customACL = acl == null;
					}
					CreateBucketRequest cbr = new CreateBucketRequest(trgBucketName);
					if (acl != null) setACL(cbr, acl);

					// if no target region is defined, we create the bucket with the same region as the source
					if (Util.isEmpty(targetRegion, true)) {
						targetRegion = getBucketRegion(srcBucketName, true).toString();
					}
					AmazonS3Client clientTarget = getAmazonS3(trgBucketName, targetRegion);
					AmazonS3Client clientSource = getAmazonS3(srcBucketName, null);
					try {
						try {
							clientTarget.createBucket(cbr);
						}
						catch (AmazonS3Exception e) {
							if (customACL) throw e;
							cbr = new CreateBucketRequest(trgBucketName);
							clientTarget.createBucket(cbr);
						}

						trgClient.copyObject(cor);
					}
					finally {
						clientTarget.release();
						clientSource.release();
					}
				}
				else {
					throw new S3Exception("could not copy [" + srcBucketName + "/" + srcObjectName + "] to [" + trgBucketName + "/" + trgObjectName + "]  ", se);
				}
			}
		}
		catch (AmazonServiceException se) {
			throw new S3Exception("could not copy [" + srcBucketName + "/" + srcObjectName + "] to [" + trgBucketName + "/" + trgObjectName + "]  ", se);
		}
		finally {
			srcClient.release();
			trgClient.release();
		}
	}

	private void releaseEL(AmazonS3Client client) {
		try {
			if (client != null) client.release();
		}
		catch (Exception e) {

		}
	}

	/**
	 * 
	 * @param srcBucketName source bucket name to copy
	 * @param srcObjectName source object name to copy
	 * @param trgBucketName target bucket name to copy
	 * @param trgObjectName target object name to copy
	 * @param acl Access Control list (can be null)
	 * @param targetRegion region of the target bucket (only necessary if it differs from source and
	 *            does not exist yet)
	 * @throws S3Exception
	 */
	public void moveObject(String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName, CannedAccessControlList cacl, String targetRegion)
			throws S3Exception {
		copyObject(srcBucketName, srcObjectName, trgBucketName, trgObjectName, cacl, targetRegion);
		delete(srcBucketName, srcObjectName, true);
	}

	private void flushExists(String bucketName, boolean flushRegionCache) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		String prefix = bucketName + ":";
		cache.buckets = null;
		_flush(cache.exists, prefix, null);
		_flush(cache.objects, prefix, null);
		if (cache.existBuckets != null) cache.existBuckets.remove(bucketName);
		if (flushRegionCache) {
			cache.bucketRegions.remove(bucketName);
		}
	}

	private void flushExists(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		String nameDir = improveObjectName(objectName, true);
		String nameFile = improveObjectName(objectName, false);
		String exact = bucketName + ":" + nameFile;
		String prefix = bucketName + ":" + nameDir;
		String prefix2 = bucketName + ":";
		_flush(cache.exists, prefix, exact);
		_flush(cache.objects, prefix2, exact);
	}

	private static void _flush(Map<String, ?> map, String prefix, String exact) {
		if (map == null) return;

		Iterator<?> it = map.entrySet().iterator();
		Entry<String, ?> e;
		String key;
		long now = System.currentTimeMillis();
		Boolean isS3Info = null;
		while (it.hasNext()) {
			e = (Entry<String, ?>) it.next();
			key = e.getKey();
			if (key == null) continue;
			if (isS3Info == null) isS3Info = e.getValue() instanceof S3Info;

			if ((exact != null && key.equals(exact)) || (prefix != null && key.startsWith(prefix))) {
				map.remove(key);
			}
			else if (prefix != null && prefix.startsWith(key + "/") && e.getValue() instanceof NotExisting) {
				map.remove(key);
			}
			else if (isS3Info.booleanValue() && ((S3Info) e.getValue()).validUntil() < now) {
				map.remove(key);
			}
		}
	}

	/**
	 * 
	 * @param bucketName target bucket to create if not already exists
	 * @param objectName object to create
	 * @param data object content
	 * @param acl access control list
	 * @param region if bucket is not already existing, it get created using that region, if it is not
	 *            defined the default region defined with the constructor is used
	 * @throws S3Exception
	 */

	public void write(String bucketName, String objectName, String data, String mimeType, Charset charset, Object acl, String region) throws IOException {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);
		flushExists(bucketName, objectName);

		AmazonS3Client client = getAmazonS3(bucketName, region);
		String ct = toContentType(mimeType, charset, null);
		byte[] bytes = charset == null ? data.getBytes() : data.getBytes(charset);

		// unlikely this ever happen, so we do not write extra code for this
		if (data.length() > maxSize) {

			File tmp = File.createTempFile("writeString-", ".txt");
			try {
				Util.copy(new ByteArrayInputStream(bytes), new FileOutputStream(tmp), true, true);
				write(bucketName, objectName, tmp, acl, region);
				return;
			}
			finally {
				tmp.delete();
			}
		}
		else {
			ObjectMetadata md = new ObjectMetadata();
			if (ct != null) md.setContentType(ct);
			md.setLastModified(new Date());
			// create a PutObjectRequest passing the folder name suffixed by /
			md.setContentLength(bytes.length);
			PutObjectRequest por = new PutObjectRequest(bucketName, objectName, new ByteArrayInputStream(bytes), md);

			if (acl != null) setACL(por, acl);
			try {
				// send request to S3 to create folder
				try {
					client.putObject(por);
					flushExists(bucketName, objectName);
				}
				catch (AmazonServiceException ase) {
					if (ase.getErrorCode().equals("EntityTooLarge")) {
						S3Exception s3e = toS3Exception(ase);
						if (s3e.getProposedSize() != 0 && s3e.getProposedSize() < maxSize) {
							maxSize = s3e.getProposedSize();
							write(bucketName, objectName, data, mimeType, charset, acl, region);
							return;
						}
						throw s3e;
					}
					if (ase.getErrorCode().equals("NoSuchBucket")) {
						createDirectory(bucketName, acl, region);
						write(bucketName, objectName, data, mimeType, charset, acl, region);
						return;
					}
					else throw toS3Exception(ase);
				}
			}
			catch (AmazonServiceException se) {
				throw toS3Exception(se);
			}
			finally {
				client.release();
			}
		}
	}

	/**
	 * 
	 * @param bucketName target bucket to create if not already exists
	 * @param objectName object to create
	 * @param data object content
	 * @param acl access control list
	 * @param region if bucket is not already existing, it get created using that region, if it is not
	 *            defined the default region defined with the constructor is used
	 * @throws S3Exception
	 */

	public void write(String bucketName, String objectName, byte[] data, String mimeType, Object acl, String region) throws IOException {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);

		flushExists(bucketName, objectName);
		AmazonS3Client client = getAmazonS3(bucketName, region);

		// create a PutObjectRequest passing the folder name suffixed by /
		if (data.length > maxSize) {
			File tmp = File.createTempFile("writeBytes-", ".txt");
			try {
				Util.copy(new ByteArrayInputStream(data), new FileOutputStream(tmp), true, true);
				write(bucketName, objectName, tmp, acl, region);
				return;
			}
			finally {
				tmp.delete();
			}
		}
		else {
			ObjectMetadata md = new ObjectMetadata();
			PutObjectRequest por = new PutObjectRequest(bucketName, objectName, new ByteArrayInputStream(data), md);
			String ct = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(data, null);
			if (ct != null) md.setContentType(ct);
			md.setLastModified(new Date());
			if (acl != null) setACL(por, acl);
			try {
				// send request to S3 to create folder
				try {
					client.putObject(por);
					flushExists(bucketName, objectName);
				}
				catch (AmazonServiceException ase) {
					if (ase.getErrorCode().equals("NoSuchBucket")) {
						createDirectory(bucketName, acl, region);
						client.putObject(por);
						flushExists(bucketName, objectName);
					}
					else throw toS3Exception(ase);
				}

			}
			catch (AmazonServiceException se) {
				throw toS3Exception(se);
			}
			finally {
				client.release();
			}
		}
	}

	public void write(String bucketName, String objectName, File file, Object acl, String region) throws IOException {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName, false);

		String ct = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(max1000(file), null);
		ObjectMetadata md = new ObjectMetadata();
		md.setContentLength(file.length());
		md.setContentType(ct);
		md.setLastModified(new Date());

		flushExists(bucketName, objectName);

		AmazonS3Client client = getAmazonS3(bucketName, region);
		// send request to S3 to create folder

		if (file.length() >= maxSize) {
			try {
				// Create a list of ETag objects. You retrieve ETags for each object part uploaded,
				// then, after each individual part has been uploaded, pass the list of ETags to
				// the request to complete the upload.
				List<PartETag> partETags = new ArrayList<PartETag>();

				long contentLength = file.length();
				long partSize = 100 * 1024 * 1024; // Set part size to 100 MB.

				// Initiate the multipart upload.
				InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectName, md);
				InitiateMultipartUploadResult initResponse = client.initiateMultipartUpload(initRequest);

				// Upload the file parts.
				long filePosition = 0;
				long total = 0;
				for (int i = 1; filePosition < contentLength; i++) {
					// Because the last part could be less than 100 MB, adjust the part size as needed.
					partSize = Math.min(partSize, (contentLength - filePosition));
					total += partSize;
					// Create the request to upload a part.
					UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName).withKey(objectName).withUploadId(initResponse.getUploadId())
							.withPartNumber(i).withFileOffset(filePosition).withFile(file).withPartSize(partSize);
					// TODO set ACL
					// Upload the part and add the response's ETag to our list.
					UploadPartResult uploadResult = client.uploadPart(uploadRequest);
					partETags.add(uploadResult.getPartETag());

					filePosition += partSize;
				}

				// Complete the multipart upload.
				CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, objectName, initResponse.getUploadId(), partETags);
				client.completeMultipartUpload(compRequest);
				if (acl != null) {
					setAccessControlList(client, bucketName, objectName, acl);
				}

			}
			catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equals("NoSuchBucket")) {
					createDirectory(bucketName, acl, region);
					write(bucketName, objectName, file, acl, region);
					return;
				}
				else throw toS3Exception(ase);
			}
			finally {
				client.release();
			}

		}
		else {
			// create a PutObjectRequest passing the folder name suffixed by /
			PutObjectRequest por = new PutObjectRequest(bucketName, objectName, file);
			if (acl != null) setACL(por, acl);
			por.setMetadata(md);
			try {
				client.putObject(por);
				flushExists(bucketName, objectName);
			}

			catch (AmazonServiceException ase) {
				// Error Code: EntityTooLarge

				// Your proposed upload exceeds the maximum allowed
				// size;error-code:EntityTooLarge;ProposedSize:5800000000;MaxSizeAllowed:5368709120
				if (ase.getErrorCode().equals("EntityTooLarge")) {
					S3Exception s3e = toS3Exception(ase);
					if (s3e.getProposedSize() != 0 && s3e.getProposedSize() < maxSize) {
						maxSize = s3e.getProposedSize();
						write(bucketName, objectName, file, acl, region);
						return;
					}
					throw s3e;
				}
				if (ase.getErrorCode().equals("NoSuchBucket")) {
					createDirectory(bucketName, acl, region);
					write(bucketName, objectName, file, acl, region);
					return;
				}
				else throw toS3Exception(ase);
			}
			finally {
				client.release();
			}
		}
	}

	public void write(String bucketName, String objectName, Resource res, Object acl, String region) throws IOException {

		if (res instanceof File) {
			write(bucketName, objectName, (File) res, acl, region);
			return;
		}
		String ct = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(max1000(res), null);
		// write(bucketName, objectName, res.getInputStream(), res.length(), true, ct, acl, region);

		try {
			bucketName = improveBucketName(bucketName);
			objectName = improveObjectName(objectName, false);
			flushExists(bucketName, objectName);

			// send request to S3 to create folder
			AmazonS3Client client = getAmazonS3(bucketName, region);

			if (res.length() > maxSize) {
				File tmp = File.createTempFile("writeResource-", ".txt");
				try {
					Util.copy(res.getInputStream(), new FileOutputStream(tmp), true, true);
					write(bucketName, objectName, tmp, acl, region);
					return;
				}
				finally {
					tmp.delete();
				}
			}
			else {
				InputStream is = null;
				ObjectMetadata md = new ObjectMetadata();
				md.setLastModified(new Date());
				md.setContentLength(res.length());
				md.setContentType(ct);
				// create a PutObjectRequest passing the folder name suffixed by /

				try {
					PutObjectRequest por = new PutObjectRequest(bucketName, objectName, is = res.getInputStream(), md);
					if (acl != null) setACL(por, acl);

					client.putObject(por);
					flushExists(bucketName, objectName);
				}
				catch (AmazonServiceException ase) {
					if (ase.getErrorCode().equals("EntityTooLarge")) {
						S3Exception s3e = toS3Exception(ase);
						if (s3e.getProposedSize() != 0 && s3e.getProposedSize() < maxSize) {
							maxSize = s3e.getProposedSize();
							write(bucketName, objectName, res, acl, region);
							return;
						}
						throw s3e;
					}
					if (ase.getErrorCode().equals("NoSuchBucket")) {
						createDirectory(bucketName, acl, region);
						write(bucketName, objectName, res, acl, region);
						return;
					}
					else throw toS3Exception(ase);
				}
				finally {
					Util.closeEL(is);
					client.release();
				}
			}

		}
		catch (AmazonServiceException se) {
			throw toS3Exception(se);
		}
	}

	public Struct getMetaDataStruct(String bucketName, String objectName) throws S3Exception {
		Struct sct = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		sct.setEL("bucketName", bucketName);
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			if (Util.isEmpty(objectName)) {
				// TODO
			}
			if (objectName != null) {
				sct.setEL("objectName", objectName);

				S3Object o = client.getObject(bucketName, objectName);
				ObjectMetadata md = o.getObjectMetadata();

				Map<String, Object> rmd = md.getRawMetadata();
				Iterator<Entry<String, Object>> it = rmd.entrySet().iterator();
				Entry<String, Object> e;
				while (it.hasNext()) {
					e = it.next();
					sct.setEL(e.getKey().replace('-', '_'), e.getValue());
				}
				sct.setEL("lastModified", md.getLastModified());

			}
		}
		finally {
			client.release();
		}
		// TODO better

		/*
		 * if (owner != null) { sct.put("owner", owner.getDisplayName()); sct.put("owner_id",
		 * owner.getId()); }
		 */
		return sct;
	}

	public Struct getMetaData(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);
		S3Info info = get(bucketName, objectName);
		if (info == null || info.isVirtual()) throw new S3Exception("there is no physical object [" + bucketName + "/" + objectName + "]");
		return info.getMetaData();
	}

	public ObjectMetadata getObjectMetadata(String bucketName, String objectName) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			return client.getObject(bucketName, objectName).getObjectMetadata();
		}
		finally {
			client.release();
		}
	}

	public void setLastModified(String bucketName, String objectName, long time) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		if (!Util.isEmpty(objectName)) {
			if (Util.isEmpty(objectName)) {
				S3BucketWrapper bw = get(bucketName);
				if (bw == null) throw new S3Exception("there is no bucket [" + bucketName + "]");
				Bucket b = bw.getBucket();
				b.setCreationDate(new Date(time)); // not sure if that is ok that way
			}
			else {

				S3Info info = get(bucketName, objectName);
				if (info == null || info.isVirtual()) throw new S3Exception("there is no object [" + objectName + "] in bucket [" + bucketName + "]");
				S3ObjectSummary so = ((StorageObjectWrapper) info).getStorageObject();
				fixBackBlazeBug(so, bucketName);
				so.setLastModified(new Date(time));
			}
		}
		// TDOD add for bucket?
	}

	public void setMetaData(String bucketName, String objectName, Struct metadata) throws PageException, S3Exception {
		Map<String, String> data = new HashMap<>();
		Iterator<Entry<Key, Object>> it = metadata.entryIterator();
		Entry<Key, Object> e;
		Cast cas = CFMLEngineFactory.getInstance().getCastUtil();
		while (it.hasNext()) {
			e = it.next();
			data.put(e.getKey().getString(), cas.toString(e.getValue()));
		}
		setMetaDataAsMap(bucketName, objectName, data);
	}

	public void setMetaDataAsMap(String bucketName, String objectName, Map<String, String> metadata) throws S3Exception {

		Iterator<Entry<String, String>> it = metadata.entrySet().iterator();
		Entry<String, String> e;
		ObjectMetadata metadataCopy = new ObjectMetadata();
		while (it.hasNext()) {
			e = it.next();
			metadataCopy.addUserMetadata(toMetaDataKey(e.getKey()), e.getValue());
		}

		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		if (!Util.isEmpty(objectName)) {
			AmazonS3Client client = getAmazonS3(bucketName, null);
			S3BucketWrapper bw = get(bucketName);
			if (bw == null) throw new S3Exception("there is no bucket [" + bucketName + "]");
			try {
				CopyObjectRequest request = new CopyObjectRequest(bucketName, objectName, bucketName, objectName).withNewObjectMetadata(metadataCopy);
				client.copyObject(request);
				flushExists(bucketName, objectName);
			}
			catch (AmazonServiceException se) {
				if (se.getErrorCode().equals("NoSuchKey")) { // we know at this point objectname is not empty, so we do not have to check that
					try {
						String on = oppositeObjectName(objectName);
						CopyObjectRequest request = new CopyObjectRequest(bucketName, on, bucketName, on).withNewObjectMetadata(metadataCopy);
						client.copyObject(request);
						flushExists(bucketName, on);
						return;
					}
					catch (AmazonServiceException ise) {
						S3Info src = get(bucketName, objectName);
						if (src.isVirtual()) {
							throw new S3Exception("Cannot set meta data to a virtual folder", se);
						}
						throw toS3Exception(ise);
					}
				}
				throw toS3Exception(se);
			}
			finally {
				client.release();
			}
		}
		else throw new S3Exception("cannot set metadata for a bucket"); // TOOD possible?

	}

	private String toMetaDataKey(String key) {
		if (key.equalsIgnoreCase("content-disposition")) return "Content-Disposition";
		if (key.equalsIgnoreCase("content_disposition")) return "Content-Disposition";
		if (key.equalsIgnoreCase("content disposition")) return "Content-Disposition";
		if (key.equalsIgnoreCase("contentdisposition")) return "Content-Disposition";

		if (key.equalsIgnoreCase("content-encoding")) return "Content-Encoding";
		if (key.equalsIgnoreCase("content_encoding")) return "Content-Encoding";
		if (key.equalsIgnoreCase("content encoding")) return "Content-Encoding";
		if (key.equalsIgnoreCase("contentencoding")) return "Content-Encoding";

		if (key.equalsIgnoreCase("content-language")) return "Content-Language";
		if (key.equalsIgnoreCase("content_language")) return "Content-Language";
		if (key.equalsIgnoreCase("content language")) return "Content-Language";
		if (key.equalsIgnoreCase("contentlanguage")) return "Content-Language";

		if (key.equalsIgnoreCase("content-length")) return "Content-Length";
		if (key.equalsIgnoreCase("content_length")) return "Content-Length";
		if (key.equalsIgnoreCase("content length")) return "Content-Length";
		if (key.equalsIgnoreCase("contentlength")) return "Content-Length";

		if (key.equalsIgnoreCase("content-md5")) return "Content-MD5";
		if (key.equalsIgnoreCase("content_md5")) return "Content-MD5";
		if (key.equalsIgnoreCase("content md5")) return "Content-MD5";
		if (key.equalsIgnoreCase("contentmd5")) return "Content-MD5";

		if (key.equalsIgnoreCase("content-type")) return "Content-Type";
		if (key.equalsIgnoreCase("content_type")) return "Content-Type";
		if (key.equalsIgnoreCase("content type")) return "Content-Type";
		if (key.equalsIgnoreCase("contenttype")) return "Content-Type";

		if (key.equalsIgnoreCase("last-modified")) return "Last-Modified";
		if (key.equalsIgnoreCase("last_modified")) return "Last-Modified";
		if (key.equalsIgnoreCase("last modified")) return "Last-Modified";
		if (key.equalsIgnoreCase("lastmodified")) return "Last-Modified";

		if (key.equalsIgnoreCase("md5_hash")) return "md5-hash";
		if (key.equalsIgnoreCase("md5_hash")) return "md5-hash";
		if (key.equalsIgnoreCase("md5_hash")) return "md5-hash";
		if (key.equalsIgnoreCase("md5_hash")) return "md5-hash";

		if (key.equalsIgnoreCase("date")) return "Date";
		if (key.equalsIgnoreCase("etag")) return "ETag";

		return key;
	}

	public void addAccessControlList(String bucketName, String objectName, Object objACL) throws S3Exception, PageException {
		AmazonS3Client client = getAmazonS3(bucketName, null);
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);
		AccessControlList acl = getACL(client, bucketName, objectName, false);
		acl.grantAllPermissions(AccessControlListUtil.toGrantAndPermissions(objACL));

		try {
			client.setObjectAcl(bucketName, objectName, acl);
			// is it necessary to set it for bucket as well?
		}
		catch (AmazonServiceException se) {
			if (se.getErrorCode().equals("NoSuchKey")) { // we know at this point objectname is not empty, so we do not have to check that
				try {
					client.setObjectAcl(bucketName, oppositeObjectName(objectName), acl);
					return;
				}
				catch (AmazonServiceException ise) {
					S3Info src = get(bucketName, objectName);
					if (src.isVirtual()) {
						throw new S3Exception("Cannot set Access Control List to a virtual folder", se);
					}
					throw toS3Exception(ise);
				}
			}
			throw toS3Exception(se);
		}
		finally {
			client.release();
		}

	}

	public void setAccessControlList(AmazonS3Client client, String bucketName, String objectName, Object objACL) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);

		boolean externalClient = true;
		if (client == null) {
			client = getAmazonS3(bucketName, null);
			externalClient = false;
		}

		Object newACL = AccessControlListUtil.toAccessControlList(objACL);
		AccessControlList oldACL = getACL(client, bucketName, objectName, true);
		Owner aclOwner = oldACL != null ? oldACL.getOwner() : client.getS3AccountOwner();
		if (newACL instanceof AccessControlList) ((AccessControlList) newACL).setOwner(aclOwner);

		try {
			if (!Util.isEmpty(objectName)) {
				if (newACL instanceof AccessControlList) client.setObjectAcl(bucketName, objectName, (AccessControlList) newACL);
				else client.setObjectAcl(bucketName, objectName, (CannedAccessControlList) newACL);
			}
			else {
				if (newACL instanceof AccessControlList) client.setBucketAcl(bucketName, (AccessControlList) newACL);
				else client.setBucketAcl(bucketName, (CannedAccessControlList) newACL);
			}

		}
		catch (AmazonServiceException se) {
			if (se.getErrorCode().equals("NoSuchKey")) { // we know at this point objectname is not empty, so we do not have to check that
				try {
					if (newACL instanceof AccessControlList) client.setObjectAcl(bucketName, oppositeObjectName(objectName), (AccessControlList) newACL);
					else client.setObjectAcl(bucketName, oppositeObjectName(objectName), (CannedAccessControlList) newACL);
					return;
				}
				catch (AmazonServiceException ise) {
					S3Info src = get(bucketName, objectName);
					if (src.isVirtual()) {
						throw new S3Exception("Cannot set Access Control List to a virtual folder", se);
					}

					throw toS3Exception(ise);
				}
			}
			throw toS3Exception(se);
		}
		finally {
			if (!externalClient) client.release();
		}

	}

	public Array getAccessControlList(String bucketName, String objectName) throws S3Exception {
		AccessControlList acl = getACL(null, bucketName, objectName, false);
		return AccessControlListUtil.toArray(acl.getGrantsAsList());
	}

	private AccessControlList getACL(AmazonS3Client client, String bucketName, String objectName, boolean returnNullWhenAccessDenied) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		objectName = improveObjectName(objectName);
		String key = toKey(bucketName, objectName);
		ValidUntilElement<AccessControlList> vuacl = cache.accessControlLists.get(key);
		if (vuacl != null && vuacl.validUntil > System.currentTimeMillis()) return vuacl.element;
		boolean externalClient = true;
		if (client == null) {
			client = getAmazonS3(bucketName, null);
			externalClient = false;
		}

		try {
			if (Util.isEmpty(objectName)) return client.getBucketAcl(bucketName);
			return client.getObjectAcl(bucketName, objectName);
		}
		catch (AmazonServiceException se) {
			if (returnNullWhenAccessDenied && "AccessDenied".equals(se.getErrorCode())) {
				return null;
			}
			else if ("NoSuchKey".equals(se.getErrorCode())) { // we know at this point objectname is not empty, so we do not have to check that
				try {
					return client.getObjectAcl(bucketName, oppositeObjectName(objectName));
				}
				catch (AmazonServiceException ise) {
					if (returnNullWhenAccessDenied && "AccessDenied".equals(ise.getErrorCode())) {
						return null;
					}
					throw toS3Exception(ise);
				}
			}
			throw toS3Exception(se);
		}
		finally {
			if (!externalClient) client.release();
		}
	}

	private String toContentType(String mimeType, Charset charset, String defaultValue) {
		if (!Util.isEmpty(mimeType)) {
			return charset != null ? mimeType + "; charset=" + charset.toString() : mimeType;
		}
		return defaultValue;
	}

	public URL url(String bucketName, String objectName, long time) throws S3Exception {
		AmazonS3Client client = getAmazonS3(bucketName, null);
		try {
			return client.generatePresignedUrl(bucketName, objectName, new Date(System.currentTimeMillis() + time));
		}
		finally {
			client.release();
		}
	}

	private AmazonS3Client getAmazonS3(String bucketName, String strRegion) throws S3Exception { // TODO remove
		return getAmazonS3(bucketName, strRegion, false);
	}

	private AmazonS3Client getAmazonS3(String bucketName, String strRegion, boolean pathStyleAccess) throws S3Exception {
		if (Util.isEmpty(accessKeyId) || Util.isEmpty(secretAccessKey)) throw new S3Exception("Could not found an accessKeyId/secretAccessKey");

		Region region = toRegion(bucketName, strRegion);

		return AmazonS3Client.get(accessKeyId, secretAccessKey, host, region, liveTimeout, pathStyleAccess, log);
	}

	public Region getBucketRegion(String bucketName, boolean loadIfNecessary) throws S3Exception {
		bucketName = improveBucketName(bucketName);
		Region r = cache.bucketRegions.get(bucketName);

		if (r != null) {
			if (r == RegionFactory.ERROR) return null;
			return r;
		}
		r = null;
		if (loadIfNecessary) {
			AmazonS3Client client = getAmazonS3(null, null);
			try {
				r = RegionFactory.getInstance(client.getBucketLocation(bucketName));
				cache.bucketRegions.put(bucketName, r);
			}
			catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equals("NoSuchBucket")) {
					return null;
				}
				if (log != null) log.error("s3", "failed to load region", ase);
				else ase.printStackTrace();
				// could be AccessDenied
				cache.bucketRegions.put(bucketName, RegionFactory.ERROR);
				return null;

			}
			finally {
				client.release();
			}
		}
		return r;
	}

	private Region toRegion(String bucketName, String strRegion) throws S3Exception {
		if (!Util.isEmpty(strRegion, true)) {
			return RegionFactory.getInstance(strRegion);
		}
		else if (!Util.isEmpty(bucketName)) {
			return getBucketRegion(bucketName, true);
		}
		return Util.isEmpty(defaultRegion, true) ? null : RegionFactory.getInstance(defaultRegion);
	}

	public static String toString(Region region) {
		return region.getName(); // WASABi does not work with toString()
	}

	public static CannedAccessControlList toACL(String acl, CannedAccessControlList defaultValue) {
		if (acl == null) return defaultValue;

		acl = acl.trim().toLowerCase();

		if ("public-read".equals(acl)) return CannedAccessControlList.PublicRead;
		if ("public read".equals(acl)) return CannedAccessControlList.PublicRead;
		if ("public_read".equals(acl)) return CannedAccessControlList.PublicRead;
		if ("publicread".equals(acl)) return CannedAccessControlList.PublicRead;

		if ("private".equals(acl)) return CannedAccessControlList.Private;

		if ("public-read-write".equals(acl)) return CannedAccessControlList.PublicReadWrite;
		if ("public read write".equals(acl)) return CannedAccessControlList.PublicReadWrite;
		if ("public_read_write".equals(acl)) return CannedAccessControlList.PublicReadWrite;
		if ("publicreadwrite".equals(acl)) return CannedAccessControlList.PublicReadWrite;

		if ("authenticated-read".equals(acl)) return CannedAccessControlList.AuthenticatedRead;
		if ("authenticated read".equals(acl)) return CannedAccessControlList.AuthenticatedRead;
		if ("authenticated_read".equals(acl)) return CannedAccessControlList.AuthenticatedRead;
		if ("authenticatedread".equals(acl)) return CannedAccessControlList.AuthenticatedRead;

		if ("authenticate-read".equals(acl)) return CannedAccessControlList.AuthenticatedRead;
		if ("authenticate read".equals(acl)) return CannedAccessControlList.AuthenticatedRead;
		if ("authenticate_read".equals(acl)) return CannedAccessControlList.AuthenticatedRead;
		if ("authenticateread".equals(acl)) return CannedAccessControlList.AuthenticatedRead;

		return defaultValue;
	}

	public static CannedAccessControlList toACL(String acl) throws S3Exception {
		CannedAccessControlList cacl = toACL(acl, null);
		if (cacl != null) return cacl;
		throw new S3Exception("cannot convert [" + acl + "] to a valid access control list");
	}

	public static CannedAccessControlList toACL(Properties prop, CannedAccessControlList defaultValue) {
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

	private List<KeyVersion> toObjectKeyAndVersions(List<S3ObjectSummary> summeries) {
		List<KeyVersion> trg = new ArrayList<>();
		for (S3ObjectSummary s: summeries) {
			trg.add(new KeyVersion(s.getKey()));
		}
		return trg;
	}

	private List<KeyVersion> toObjectKeyAndVersions(List<S3ObjectSummary> summeries, long maxAge) {

		if (maxAge <= 0) return toObjectKeyAndVersions(summeries);
		List<KeyVersion> trg = new ArrayList<>();
		long now = System.currentTimeMillis();
		for (S3ObjectSummary s: summeries) {
			if (now > (s.getLastModified().getTime() + maxAge)) trg.add(new KeyVersion(s.getKey()));
		}
		return trg;
	}

	private KeyVersion[] toObjectKeyAndVersions(List<S3Info> src, String ignoreKey) {
		List<KeyVersion> trg = new ArrayList<KeyVersion>();
		Iterator<S3Info> it = src.iterator();
		S3Info info;
		while (it.hasNext()) {
			info = it.next();
			if (ignoreKey != null && info.getName().equals(ignoreKey)) continue;
			trg.add(new KeyVersion(info.getName()));
		}
		return trg.toArray(new KeyVersion[trg.size()]);
	}

	public S3Exception toS3Exception(Exception e) {
		if (e instanceof AmazonServiceException) return toS3Exception((AmazonServiceException) e);
		S3Exception s3e = new S3Exception(e.getClass().getName() + ":" + e.getMessage());
		s3e.initCause(e);
		s3e.setStackTrace(e.getStackTrace());
		return s3e;
	}

	private S3Exception toS3Exception(AmazonServiceException se) {
		return toS3Exception(se, null);
	}

	public static S3Exception toS3Exception(AmazonServiceException se, String detail) {
		String msg = se.getErrorMessage();
		if (Util.isEmpty(msg, true)) msg = se.getMessage();
		if (msg.equals("'")) msg = "";
		long proposedSize = 0;
		if (se instanceof AmazonS3Exception) {
			AmazonS3Exception ase = (AmazonS3Exception) se;

			String raw = ase.getErrorResponseXml();

			// TODO read all tags dynamically and provide as a Map

			// extract message
			{
				int startIndex = raw == null ? -1 : raw.indexOf("<Message>");
				if (startIndex != -1) {
					startIndex += 9;
					int endIndex = raw.indexOf("</Message>");
					if (endIndex > startIndex) {
						String xmlMsg = raw.substring(startIndex, endIndex);
						if (!Util.isEmpty(xmlMsg, true) && !xmlMsg.equals(msg)) {
							if (!Util.isEmpty(msg, true)) msg += ";" + xmlMsg;
							else msg = xmlMsg;
							;
						}
					}
				}
			}

			// extract ProposedSize
			{
				int startIndex = raw == null ? -1 : raw.indexOf("<ProposedSize>");
				if (startIndex != -1) {
					startIndex += 14;
					int endIndex = raw.indexOf("</ProposedSize>");
					if (endIndex > startIndex) {
						String xmlProposedSize = raw.substring(startIndex, endIndex);
						if (!Util.isEmpty(xmlProposedSize, true)) {

							try {
								proposedSize = CFMLEngineFactory.getInstance().getCastUtil().toLongValue(xmlProposedSize, proposedSize);
							}
							catch (Exception e) {
								try {
									proposedSize = Long.parseLong(xmlProposedSize.trim());
								}
								catch (Exception ee) {
								}
							}
						}
					}
				}
			}

		}

		// local message
		String lm = se.getLocalizedMessage();
		if (!Util.isEmpty(lm, true) && lm.equals(msg)) msg += ";" + lm;

		// error code
		String ec = se.getErrorCode();
		if (!Util.isEmpty(ec, true)) msg += ";error-code:" + ec;

		// detail
		if (!Util.isEmpty(detail, true)) msg += ";" + detail;

		// addional details
		if (se instanceof AmazonS3Exception) {
			AmazonS3Exception ase = (AmazonS3Exception) se;

			Map<String, String> map = ase.getAdditionalDetails();
			if (map != null) {
				for (Entry<String, String> e: map.entrySet()) {
					msg += ";" + e.getKey() + ":" + e.getValue();
				}
			}
		}

		S3Exception s3e = new S3Exception(msg);
		s3e.initCause(se);
		s3e.setStackTrace(se.getStackTrace());
		s3e.setErrorCode(ec);
		if (proposedSize > 0) s3e.setProposedSize(proposedSize);
		return s3e;
	}

	public static String improveBucketName(String bucketName) throws S3Exception {
		if (bucketName.startsWith("/")) bucketName = bucketName.substring(1);
		if (bucketName.endsWith("/")) bucketName = bucketName.substring(0, bucketName.length() - 1);
		if (bucketName.indexOf('/') != -1) throw new S3Exception("invalid bucket name [" + bucketName + "]");
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

	private static String oppositeObjectName(String objectName) {
		if (objectName.endsWith("/")) return improveObjectName(objectName, false);
		return improveObjectName(objectName, true);
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

	public static byte[] max1000(File file) throws IOException {
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

	public static byte[] max1000(Resource res) throws IOException {
		InputStream in = res.getInputStream();
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

	private static void setACL(PutObjectRequest por, Object acl) {
		if (acl instanceof CannedAccessControlList) por.setCannedAcl((CannedAccessControlList) acl);
		else por.setAccessControlList((AccessControlList) acl);
	}

	private static void setACL(CreateBucketRequest por, Object acl) {
		if (acl instanceof CannedAccessControlList) por.setCannedAcl((CannedAccessControlList) acl);
		else por.setAccessControlList((AccessControlList) acl);
	}

	private static void setACL(CopyObjectRequest por, Object acl) {
		if (acl instanceof CannedAccessControlList) por.setCannedAccessControlList((CannedAccessControlList) acl);
		else por.setAccessControlList((AccessControlList) acl);
	}

	private static HostData toHostData(String host) throws S3Exception {
		int index = host.indexOf('.');
		String prefix;
		if (index != 2 || !host.substring(0, 2).equalsIgnoreCase("s3")) {
			throw new S3Exception("host name [" + host + "] is invalid, must start with [s3.]");
		}

		index = host.indexOf('.', 3);
		if (index == -1) {
			throw new S3Exception("host name [" + host + "] is invalid");
		}
		int index2 = host.indexOf('.', index + 1);

		// no region
		if (index2 == -1) {
			return new HostData(null, host.substring(3));
		}
		return new HostData(host.substring(3, index), host.substring(index + 1));
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

	private static class HostData {
		private String region;
		private String domain;

		private HostData(String region, String domain) {
			this.region = region;
			this.domain = domain;

		}

		@Override
		public String toString() {
			return "region:" + region + ";domain:" + domain + ";";
		}
	}

	private class CacheRegions extends Thread {

		@Override
		public void run() {
			AmazonS3Client client = null;
			try {
				client = getAmazonS3(null, null);
				Region r;
				for (Bucket b: client.listBuckets()) {
					try {
						r = RegionFactory.getInstance(client.getBucketLocation(b.getName()));
						if (log != null) log.trace("s3", "cache region [" + r.toString() + "] for bucket [" + b.getName() + "]");
						cache.bucketRegions.put(b.getName(), r);
						// we don't want this to make to much load
						sleep(100);
					}
					catch (Exception e) {
						// in case the bucket is gone in meantime, we don't care and don't log
						if (!(e instanceof AmazonServiceException
								&& (((AmazonServiceException) e).getErrorCode().equals("NoSuchBucket") || ((AmazonServiceException) e).getErrorCode().equals("AccessDenied")))) {
							if (log != null) log.error("s3", e);
						}
					}
				}
			}
			catch (S3Exception e1) {
				if (!"AccessDenied".equalsIgnoreCase(e1.getErrorCode())) { // in case we can not the region because of access right, we don't care.
					if (log != null) log.error("s3", e1);
					else e1.printStackTrace();
				}
			}
			catch (Exception e) {
				if (!(e.getCause() instanceof UnknownHostException)) {
					if (log != null) log.error("s3", e);
				}
			}

			finally {
				try {
					if (client != null) client.release();
				}
				catch (S3Exception e) {
					if (log != null) log.error("s3", e);
				}
			}
		}
	}

	public void shutdown() throws S3Exception {
		AmazonS3Client client = null;
		try {
			client = getAmazonS3(null, null);
			client.shutdown();
		}

		finally {
			if (client != null) client.release();
		}
	}

	public long getLiveTimeout() {
		return liveTimeout;
	}

}