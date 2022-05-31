package org.lucee.extension.resource.s3;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.ext.function.BIF;
import lucee.runtime.type.Struct;
import lucee.runtime.type.dt.TimeSpan;

public class S3Properties {

	private static BIF bif;
	private String host = S3.DEFAULT_HOST;
	private String secretAccessKey;
	private String accessKeyId;
	private boolean hasCustomHost;
	private boolean hasCustomCredentials;
	private Object acl;
	private String defaultLocation;
	private long cache;
	private String mapping;

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
		return new StringBuilder().append("host:").append(host).append(";").append("accessKeyId:").append(accessKeyId).append(";").append("secretAccessKey:")
				.append(secretAccessKey).append(";").append("custom:").append(hasCustomCredentials).append(";acl:").append(acl).append(";location:").append(this.defaultLocation)
				.append(";").toString();
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

	public static Struct getApplicationData(PageContext pc) throws RuntimeException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		try {
			if (bif == null || bif.getClass().getClassLoader() != pc.getClass().getClassLoader()) {
				bif = eng.getClassUtil().loadBIF(pc, "lucee.runtime.functions.system.GetApplicationSettings");
			}
			return (Struct) bif.invoke(pc, new Object[] { Boolean.TRUE });
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageRuntimeException(e);
		}
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

		String sk = eng.getCastUtil().toString(sct.get("awsSecretKey", null), null);
		if (Util.isEmpty(sk)) sk = eng.getCastUtil().toString(sct.get("secretKey", null), null);

		return toS3(eng.getCastUtil().toString(sct.get("accessKeyId", null), null), sk, eng.getCastUtil().toString(sct.get("defaultLocation", null), null), host,
				eng.getCastUtil().toString(sct.get("acl", null), null), eng.getCastUtil().toTimespan(sct.get("cache", null), null));

	}

	private static S3Properties toS3(String accessKeyId, String awsSecretKey, String defaultLocation, String host, String acl, TimeSpan cache) throws S3Exception {

		S3Properties s3 = new S3Properties();

		if (!Util.isEmpty(accessKeyId)) s3.setAccessKeyId(accessKeyId);
		if (!Util.isEmpty(awsSecretKey)) s3.setSecretAccessKey(awsSecretKey);
		if (!Util.isEmpty(defaultLocation)) s3.setDefaultLocation(defaultLocation);
		if (!Util.isEmpty(host)) s3.setHost(host);
		if (!Util.isEmpty(acl)) s3.setACL(AccessControlListUtil.toAccessControlList(acl));
		if (cache != null) s3.setCache(cache.getMillis());

		return s3;
	}

}
