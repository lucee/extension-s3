package org.lucee.extension.resource.s3;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
		return host;
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

	@Override
	public String toString() {

		return new StringBuilder().append("host:").append(getHost()).append(";").append("accessKeyId:").append(getAccessKeyId()).append(";").append("secretAccessKey:")
				.append(getSecretAccessKey()).append(";acl:").append(getACL()).append(";location:").append(getDefaultLocation()).append(";").toString();
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
				Log log = pc.getConfig().getLog("application");
				if (log != null) log.error("S3", e);
			}

		}
		// classic
		else if (eng.getClassUtil().isInstaneOf("lucee.runtime.listener.ClassicApplicationContext", ac.getClass())) {
			try {
				if (getCustomAttributes == null || ac.getClass() != getCustomAttributes.getDeclaringClass()) {
					getCustomAttributes = ac.getClass().getMethod("getCustomAttributes", new Class[] {});
				}
				Map<Key, Object> attrs = (Map<Key, Object>) getCustomAttributes.invoke(null, new Object[] {});
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
				Log log = pc.getConfig().getLog("application");
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

	private static S3Properties toS3(CFMLEngine eng, Struct sct) throws S3Exception {

		String host = eng.getCastUtil().toString(sct.get("host", null), null);
		if (Util.isEmpty(host)) host = eng.getCastUtil().toString(sct.get("server", null), null);
		if (Util.isEmpty(host)) host = eng.getCastUtil().toString(sct.get("endpoint", null), null);

		String bucket = eng.getCastUtil().toString(sct.get("bucket", null), null);
		if (Util.isEmpty(host)) bucket = eng.getCastUtil().toString(sct.get("bucketname", null), null);

		String sk = eng.getCastUtil().toString(sct.get("awsSecretKey", null), null);
		if (Util.isEmpty(sk)) sk = eng.getCastUtil().toString(sct.get("secretKey", null), null);

		return toS3(eng.getCastUtil().toString(sct.get("accessKeyId", null), null), sk, eng.getCastUtil().toString(sct.get("defaultLocation", null), null), host, bucket,
				eng.getCastUtil().toString(sct.get("acl", null), null), eng.getCastUtil().toTimespan(sct.get("cache", null), null));

	}

	private static S3Properties toS3(String accessKeyId, String awsSecretKey, String defaultLocation, String host, String bucket, String acl, TimeSpan cache) throws S3Exception {

		S3Properties s3 = new S3Properties();
		defaultLocation = S3Util.extractLocationFromHostIfNecessary(defaultLocation, host);

		if (!Util.isEmpty(accessKeyId)) s3.setAccessKeyId(accessKeyId);
		if (!Util.isEmpty(awsSecretKey)) s3.setSecretAccessKey(awsSecretKey);
		if (!Util.isEmpty(defaultLocation)) s3.setDefaultLocation(defaultLocation);
		if (!Util.isEmpty(host)) s3.setHost(host);
		if (!Util.isEmpty(bucket)) s3.setBucket(bucket);
		if (!Util.isEmpty(acl)) s3.setACL(AccessControlListUtil.toAccessControlList(acl));
		if (cache != null) s3.setCache(cache.getMillis());

		return s3;
	}

}
