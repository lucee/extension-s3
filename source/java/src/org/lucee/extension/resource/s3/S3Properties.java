package org.lucee.extension.resource.s3;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lucee.commons.io.log.Log;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.Component;
import lucee.runtime.PageContext;
import lucee.runtime.ext.function.BIF;
import lucee.runtime.listener.ApplicationContext;
import lucee.runtime.net.s3.Properties;
import lucee.runtime.type.Collection;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import lucee.runtime.type.UDF;
import lucee.runtime.type.dt.TimeSpan;

public class S3Properties {

	private static Method getComponent;
	private static Method toComponentSpecificAccess;

	private static BIF bif;
	private static Method getCustomAttributes;
	private String host = S3.DEFAULT_HOST;
	private String bucket = null;
	private String secretAccessKey;
	private String accessKeyId;
	private boolean hasCustomHost;
	private boolean hasCustomCredentials;
	private Object acl;
	private String defaultLocation;
	private long cache;
	private String mapping;
	private boolean cacheRegion = false;
	private Boolean pathStyleAccess = null;
	private Boolean ssl = null;
	private S3HttpPoolSettings httpPool = S3HttpPoolSettings.fromEnv();
	// bucket suffix as defined in this settings, null when not defined (is then inherited)
	private Map<String, String> bucketSuffixes = null;
	private Set<String> bucketSuffixExclude = null;
	// the resolved bucket suffix configuration used by S3
	private S3BucketSuffix bucketSuffix = S3BucketSuffix.EMPTY;

	public void setBucket(String bucket) {
		if (!Util.isEmpty(bucket, true)) this.bucket = bucket;
	}

	public String getBucket() {
		return bucket;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getHost() {
		if (host == null) return S3.DEFAULT_HOST;
		return host;
	}

	public String getHostWithoutRegion() {
		return S3Util.removeLocationFromHostIfNecessary(getHost());
	}

	public void setSecretAccessKey(String secretAccessKey) {
		this.secretAccessKey = secretAccessKey;
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}

	public void setDefaultLocation(String defaultLocation) {
		this.defaultLocation = defaultLocation;
	}

	public String getDefaultLocation() {
		if (defaultLocation == null) this.defaultLocation = S3Util.extractLocationFromHostIfNecessary(this.defaultLocation, this.host);
		return defaultLocation;
	}

	public void setAccessKeyId(String accessKeyId) {
		this.accessKeyId = accessKeyId;
	}

	public String getAccessKeyId() {
		return accessKeyId;
	}

	public void setCache(long cache) {
		this.cache = cache;
	}

	public long getCache() {
		return cache;
	}

	public void setCustomCredentials(boolean hasCustomCredentials) {
		this.hasCustomCredentials = hasCustomCredentials;
	}

	public boolean getCustomCredentials() {
		return hasCustomCredentials;
	}

	public void setCustomHost(boolean hasCustomHost) {
		this.hasCustomHost = hasCustomHost;
	}

	public boolean getCustomHost() {
		return hasCustomHost;
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
	}

	public String getMapping() {
		return mapping;
	}

	public boolean getCacheRegion() {
		return cacheRegion;
	}

	public void setCacheRegion(boolean cacheRegion) {
		this.cacheRegion = cacheRegion;
	}

	public Boolean getPathStyleAccess() {
		return pathStyleAccess;
	}

	public void setPathStyleAccess(Boolean pathStyleAccess) {
		this.pathStyleAccess = pathStyleAccess;
	}

	public Boolean getSsl() {
		return ssl;
	}

	public void setSsl(Boolean ssl) {
		this.ssl = ssl;
	}

	public S3HttpPoolSettings getHttpPool() {
		return httpPool;
	}

	public void setHttpPool(S3HttpPoolSettings httpPool) {
		if (httpPool != null) this.httpPool = httpPool;
	}

	/**
	 * the bucket suffixes defined in this settings (this.s3.bucketSuffixes, this.vfs.s3.bucketSuffixes
	 * or this.vfs.s3.{mapping}.bucketSuffixes)
	 *
	 * @return null if not defined, an empty map if defined as empty (that disables the suffixes
	 *         inherited from a less specific definition)
	 */
	public Map<String, String> getBucketSuffixes() {
		return bucketSuffixes;
	}

	public void setBucketSuffixes(Map<String, String> bucketSuffixes) {
		this.bucketSuffixes = bucketSuffixes;
	}

	/**
	 * the logical bucket names that do not get a suffix, as defined in this settings, null if not
	 * defined
	 */
	public Set<String> getBucketSuffixExclude() {
		return bucketSuffixExclude;
	}

	public void setBucketSuffixExclude(Set<String> bucketSuffixExclude) {
		this.bucketSuffixExclude = bucketSuffixExclude;
	}

	/**
	 * the resolved bucket suffix configuration (never null)
	 */
	public S3BucketSuffix getBucketSuffix() {
		return bucketSuffix;
	}

	public void setBucketSuffix(S3BucketSuffix bucketSuffix) {
		this.bucketSuffix = bucketSuffix == null ? S3BucketSuffix.EMPTY : bucketSuffix;
	}

	/**
	 * Resolves the bucket suffix configuration and sets it to the target. The more specific definition
	 * replaces the less specific: environment variable / system property, then Application.cfc, then
	 * the mapping (this.vfs.s3.{mapping}). Suffixes and the list of excluded buckets are replaced
	 * independent of each other.
	 *
	 * @param target the properties to set the configuration to
	 * @param app the settings from the Application.cfc, can be null
	 * @param mapping the settings of the mapping used, can be null
	 * @param region the region used for calls that do not name a region themselves, if empty the
	 *            default location of the target is used
	 * @throws S3Exception if a definition is invalid
	 */
	public static void applyBucketSuffix(S3Properties target, S3Properties app, S3Properties mapping, String region) throws S3Exception {
		Map<String, String> suffixes = S3BucketSuffix.suffixesFromEnv();
		Set<String> exclude = S3BucketSuffix.excludeFromEnv();
		if (app != null) {
			if (app.bucketSuffixes != null) suffixes = app.bucketSuffixes;
			if (app.bucketSuffixExclude != null) exclude = app.bucketSuffixExclude;
		}
		if (mapping != null) {
			if (mapping.bucketSuffixes != null) suffixes = mapping.bucketSuffixes;
			if (mapping.bucketSuffixExclude != null) exclude = mapping.bucketSuffixExclude;
		}
		if (suffixes == null || suffixes.isEmpty()) {
			target.setBucketSuffix(S3BucketSuffix.EMPTY);
			return;
		}
		if (Util.isEmpty(region, true)) region = target.getDefaultLocation();
		if (Util.isEmpty(region, true)) {
			// the same environment variables used for the default location
			region = S3Util.getSystemPropOrEnvVar("lucee.s3.location", null);
			if (Util.isEmpty(region, true)) region = S3Util.getSystemPropOrEnvVar("lucee.s3.defaultLocation", null);
			if (Util.isEmpty(region, true)) region = S3Util.getSystemPropOrEnvVar("lucee.s3.region", null);
		}
		target.setBucketSuffix(S3BucketSuffix.create(suffixes, exclude, region));
	}

	@Override
	public String toString() {
		String suffixKey = bucketSuffix.toCacheKey();

		return new StringBuilder().append("host:").append(getHost()).append(";").append("accessKeyId:").append(getAccessKeyId()).append(";").append("secretAccessKey:")
				.append(getSecretAccessKey()).append(";acl:").append(getACL()).append(";location:").append(getDefaultLocation()).append(";cacheRegion:").append(getCacheRegion())
				.append(";pathStyleAccess:").append(getPathStyleAccess()).append(";ssl:").append(getSsl()).append(";pool:").append(getHttpPool().toCacheKey()).append(";")
				.append(suffixKey.isEmpty() ? "" : suffixKey + ";").toString();
	}

	public void setACL(Object acl) {
		try {
			this.acl = AccessControlListUtil.toAccessControlList(acl);
		}
		catch (S3Exception e) {
		}
	}

	public Object getACL() {
		return this.acl;
	}

	public static Struct getApplicationData(PageContext pc) {
		ApplicationContext ac = pc.getApplicationContext();

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Struct result = eng.getCreationUtil().createStruct();

		// s3
		Properties props = ac.getS3();
		if (props != null) {
			result.setEL("s3", props.toStruct());
		}

		// modern
		if (eng.getClassUtil().isInstaneOf("lucee.runtime.listener.ModernApplicationContext", ac.getClass())) {
			try {
				if (getComponent == null || ac.getClass() != getComponent.getDeclaringClass()) {
					getComponent = ac.getClass().getMethod("getComponent", new Class[] {});
				}
				Component cfc = (Component) getComponent.invoke(ac, new Object[] {});

				Class<?> clazz = eng.getClassUtil().loadClass("lucee.runtime.ComponentSpecificAccess");
				if (toComponentSpecificAccess == null || ac.getClass() != toComponentSpecificAccess.getDeclaringClass()) {
					toComponentSpecificAccess = clazz.getMethod("toComponentSpecificAccess", new Class[] { int.class, Component.class });
				}
				Component cw = (Component) toComponentSpecificAccess.invoke(null, new Object[] { Component.ACCESS_PRIVATE, cfc });
				Iterator<Key> it = cw.keyIterator();
				Collection.Key key;
				Object value;
				while (it.hasNext()) {
					key = it.next();
					value = cw.get(key);
					if (!(value instanceof UDF)) result.setEL(key, value);
				}
			}
			catch (Exception e) {
				Log log = S3.getLog(pc != null ? pc.getConfig() : CFMLEngineFactory.getInstance().getThreadConfig());
				if (log != null) log.error("S3", e);
			}

		}
		// classic
		else if (eng.getClassUtil().isInstaneOf("lucee.runtime.listener.ClassicApplicationContext", ac.getClass())) {
			try {
				if (getCustomAttributes == null || ac.getClass() != getCustomAttributes.getDeclaringClass()) {
					getCustomAttributes = ac.getClass().getMethod("getCustomAttributes", new Class[] {});
				}
				Map<Key, Object> attrs = (Map<Key, Object>) getCustomAttributes.invoke(ac, new Object[] {});
				if (attrs != null) {
					Iterator<Entry<Key, Object>> it = attrs.entrySet().iterator();
					Entry<Key, Object> e;
					Collection.Key key;
					Object value;
					while (it.hasNext()) {
						e = it.next();
						key = e.getKey();
						value = e.getValue();
						if (!(value instanceof UDF)) result.setEL(key, value);
					}
				}
			}
			catch (Exception e) {
				Log log = S3.getLog(pc != null ? pc.getConfig() : CFMLEngineFactory.getInstance().getThreadConfig());
				if (log != null) log.error("S3", e);
			}
		}

		return result;
	}

	public static S3Properties load(PageContext pc, Struct appData, String key) throws RuntimeException {
		if (Util.isEmpty(key, true)) key = null;
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		try {

			Struct data;
			// this.s3
			if (key == null) {
				data = eng.getCastUtil().toStruct(appData.get("s3", null), null);
				if (data != null) return toS3(eng, data);
			}
			// this.vfs.s3
			data = eng.getCastUtil().toStruct(appData.get("vfs", null), null);
			if (data == null) return null;
			data = eng.getCastUtil().toStruct(data.get("s3", null), null);

			if (key != null) {
				data = eng.getCastUtil().toStruct(data.get(key, null), null);
			}
			if (data != null) {
				return toS3(eng, data);
			}
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageRuntimeException(e);
		}
		return null;
	}

	/**
	 * loads only the bucket suffix definition (bucketSuffixes and bucketSuffixExclude) from the same
	 * places as {@link #load(PageContext, Struct, String)}, an invalid definition is never ignored.
	 *
	 * @return null if there is no definition at this place
	 */
	public static S3Properties loadBucketSuffix(Struct appData, String key) throws RuntimeException {
		if (Util.isEmpty(key, true)) key = null;
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		try {
			Struct data = null;
			if (key == null) data = eng.getCastUtil().toStruct(appData.get("s3", null), null);
			if (data == null) {
				data = eng.getCastUtil().toStruct(appData.get("vfs", null), null);
				if (data != null) data = eng.getCastUtil().toStruct(data.get("s3", null), null);
				if (data != null && key != null) data = eng.getCastUtil().toStruct(data.get(key, null), null);
			}
			if (data == null) return null;
			S3Properties s3 = new S3Properties();
			Object suffixes = data.get("bucketSuffixes", null);
			if (suffixes != null) s3.setBucketSuffixes(toSuffixMap(eng, suffixes));
			Object exclude = data.get("bucketSuffixExclude", null);
			if (exclude != null) s3.setBucketSuffixExclude(toExcludeSet(exclude));
			return s3;
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageRuntimeException(e);
		}
	}

	private static S3Properties toS3(CFMLEngine eng, Struct sct) throws S3Exception {

		String host = eng.getCastUtil().toString(sct.get("host", null), null);
		if (Util.isEmpty(host)) host = eng.getCastUtil().toString(sct.get("server", null), null);
		if (Util.isEmpty(host)) host = eng.getCastUtil().toString(sct.get("endpoint", null), null);

		String bucket = eng.getCastUtil().toString(sct.get("bucket", null), null);
		if (Util.isEmpty(host)) bucket = eng.getCastUtil().toString(sct.get("bucketname", null), null);

		String sk = eng.getCastUtil().toString(sct.get("awsSecretKey", null), null);
		if (Util.isEmpty(sk)) sk = eng.getCastUtil().toString(sct.get("secretKey", null), null);

		S3Properties s3 = toS3(eng.getCastUtil().toString(sct.get("accessKeyId", null), null), sk, eng.getCastUtil().toString(sct.get("defaultLocation", null), null), host, bucket,
				eng.getCastUtil().toString(sct.get("acl", null), null), eng.getCastUtil().toBoolean(sct.get("cacheregion", null), null),
				eng.getCastUtil().toTimespan(sct.get("cache", null), null), eng.getCastUtil().toBoolean(sct.get("pathstyleaccess", null), null),
				eng.getCastUtil().toBoolean(sct.get("ssl", null), null));
		s3.setHttpPool(S3HttpPoolSettings.load(eng, sct));

		Object suffixes = sct.get("bucketSuffixes", null);
		if (suffixes != null) s3.setBucketSuffixes(toSuffixMap(eng, suffixes));
		Object exclude = sct.get("bucketSuffixExclude", null);
		if (exclude != null) s3.setBucketSuffixExclude(toExcludeSet(exclude));
		return s3;

	}

	private static Map<String, String> toSuffixMap(CFMLEngine eng, Object value) throws S3Exception {
		if (value instanceof Map) {
			Map<String, String> result = new LinkedHashMap<String, String>();
			for (Entry<?, ?> e: ((Map<?, ?>) value).entrySet()) {
				if (e.getValue() == null) throw new S3Exception("invalid bucketSuffixes definition, the suffix for the region [" + e.getKey() + "] is null");
				String key = e.getKey().toString();
				if (result.containsKey(key)) throw new S3Exception("invalid bucketSuffixes definition, duplicate region [" + key + "]");
				result.put(key, eng.getCastUtil().toString(e.getValue(), null));
			}
			return result;
		}
		if (value instanceof CharSequence) return S3BucketSuffix.parseSuffixes(value.toString());
		throw new S3Exception("invalid bucketSuffixes definition, expected a struct like {\"eu-west-1\":\"-dr\"} or a json string with that struct, but got [" + value.getClass().getName()
				+ "]");
	}

	private static Set<String> toExcludeSet(Object value) throws S3Exception {
		if (value instanceof List) {
			Set<String> result = new LinkedHashSet<String>();
			for (Object o: (List<?>) value) {
				if (o != null && !Util.isEmpty(o.toString(), true)) result.add(o.toString().trim());
			}
			return result;
		}
		if (value instanceof CharSequence) return S3BucketSuffix.parseExclude(value.toString());
		throw new S3Exception("invalid bucketSuffixExclude definition, expected an array or a comma separated list of bucket names, but got [" + value.getClass().getName() + "]");
	}

	private static S3Properties toS3(String accessKeyId, String awsSecretKey, String defaultLocation, String host, String bucket, String acl, Boolean cacheRegion, TimeSpan cache,
			Boolean pathStyleAccess, Boolean ssl) throws S3Exception {

		S3Properties s3 = new S3Properties();
		defaultLocation = S3Util.extractLocationFromHostIfNecessary(defaultLocation, host);

		if (!Util.isEmpty(accessKeyId)) s3.setAccessKeyId(accessKeyId);
		if (!Util.isEmpty(awsSecretKey)) s3.setSecretAccessKey(awsSecretKey);
		if (!Util.isEmpty(defaultLocation)) s3.setDefaultLocation(defaultLocation);
		if (!Util.isEmpty(host)) s3.setHost(host);
		if (!Util.isEmpty(bucket)) s3.setBucket(bucket);
		if (!Util.isEmpty(acl)) s3.setACL(AccessControlListUtil.toAccessControlList(acl));
		if (cacheRegion != null) s3.setCacheRegion(cacheRegion.booleanValue());
		if (cache != null) s3.setCache(cache.getMillis());
		if (pathStyleAccess != null) s3.setPathStyleAccess(pathStyleAccess);
		if (ssl != null) s3.setSsl(ssl);

		return s3;
	}

}
