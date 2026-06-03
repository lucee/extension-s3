package org.lucee.extension.resource.s3;

import com.amazonaws.ClientConfiguration;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.util.Util;
import lucee.runtime.type.Struct;

/**
 * HTTP connection pool settings for the AWS SDK client (per credential set).
 */
public class S3HttpPoolSettings {

	/** AWS SDK default; kept for reference. */
	public static final int SDK_DEFAULT_MAX_CONNECTIONS = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;
	/** Used when [pool.maxConnections] is not configured (extension default is 128; AWS SDK default is 50). */
	public static final int DEFAULT_MAX_CONNECTIONS = 128;
	public static final double DEFAULT_WARN_UTILIZATION = 0.8D;

	private Integer maxConnections;
	private Integer connectionTimeout;
	private Integer socketTimeout;
	private Long connectionMaxIdleMillis;
	private Double warnUtilization = DEFAULT_WARN_UTILIZATION;

	public Integer getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(Integer maxConnections) {
		this.maxConnections = maxConnections;
	}

	public Integer getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(Integer connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public Integer getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(Integer socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public Long getConnectionMaxIdleMillis() {
		return connectionMaxIdleMillis;
	}

	public void setConnectionMaxIdleMillis(Long connectionMaxIdleMillis) {
		this.connectionMaxIdleMillis = connectionMaxIdleMillis;
	}

	public Double getWarnUtilization() {
		return warnUtilization;
	}

	public void setWarnUtilization(Double warnUtilization) {
		this.warnUtilization = warnUtilization;
	}

	public int getEffectiveMaxConnections() {
		return maxConnections != null ? maxConnections.intValue() : DEFAULT_MAX_CONNECTIONS;
	}

	public void apply(ClientConfiguration config) {
		config.setMaxConnections(getEffectiveMaxConnections());
		if (connectionTimeout != null) config.setConnectionTimeout(connectionTimeout.intValue());
		if (socketTimeout != null) config.setSocketTimeout(socketTimeout.intValue());
		if (connectionMaxIdleMillis != null) config.setConnectionMaxIdleMillis(connectionMaxIdleMillis.longValue());
	}

	public String toCacheKey() {
		return (maxConnections == null ? "" : maxConnections) + ":" + (connectionTimeout == null ? "" : connectionTimeout) + ":" + (socketTimeout == null ? ""
				: socketTimeout) + ":" + (connectionMaxIdleMillis == null ? "" : connectionMaxIdleMillis) + ":" + (warnUtilization == null ? "" : warnUtilization);
	}

	public static S3HttpPoolSettings load(CFMLEngine eng, Struct sct) {
		S3HttpPoolSettings settings = fromEnv();
		if (sct == null) return settings;

		Struct pool = eng.getCastUtil().toStruct(sct.get("pool", null), null);
		if (pool != null) mergeStruct(settings, pool, eng);

		return settings;
	}

	private static void mergeStruct(S3HttpPoolSettings settings, Struct pool, CFMLEngine eng) {
		setInt(settings, "maxConnections", pool, eng);
		setInt(settings, "connectionTimeout", pool, eng);
		setInt(settings, "socketTimeout", pool, eng);
		setLong(settings, "connectionMaxIdleMillis", pool, eng);
		setDouble(settings, "warnUtilization", pool, eng);
	}

	public static S3HttpPoolSettings fromEnv() {
		S3HttpPoolSettings settings = new S3HttpPoolSettings();
		settings.maxConnections = toInteger(S3Util.getSystemPropOrEnvVar("lucee.s3.pool.maxconnections", null));
		if (settings.maxConnections == null) settings.maxConnections = toInteger(S3Util.getSystemPropOrEnvVar("lucee.s3.maxconnections", null));

		settings.connectionTimeout = toInteger(S3Util.getSystemPropOrEnvVar("lucee.s3.pool.connectiontimeout", null));
		settings.socketTimeout = toInteger(S3Util.getSystemPropOrEnvVar("lucee.s3.pool.sockettimeout", null));
		settings.connectionMaxIdleMillis = toLong(S3Util.getSystemPropOrEnvVar("lucee.s3.pool.connectionmaxidlemillis", null));

		String warn = S3Util.getSystemPropOrEnvVar("lucee.s3.pool.warnutilization", null);
		if (!Util.isEmpty(warn, true)) settings.warnUtilization = toDouble(warn);

		return settings;
	}

	private static void setInt(S3HttpPoolSettings settings, String key, Struct sct, CFMLEngine eng) {
		Object raw = sct.get(key, null);
		if (raw == null) return;
		Integer v = toInteger(eng.getCastUtil().toString(raw, null), null);
		if (v == null) return;
		if ("maxConnections".equals(key)) settings.setMaxConnections(v);
		else if ("connectionTimeout".equals(key)) settings.setConnectionTimeout(v);
		else if ("socketTimeout".equals(key)) settings.setSocketTimeout(v);
	}

	private static void setLong(S3HttpPoolSettings settings, String key, Struct sct, CFMLEngine eng) {
		Object raw = sct.get(key, null);
		if (raw == null) return;
		Long v = toLong(eng.getCastUtil().toString(raw, null));
		if (v != null) settings.setConnectionMaxIdleMillis(v);
	}

	private static void setDouble(S3HttpPoolSettings settings, String key, Struct sct, CFMLEngine eng) {
		Object raw = sct.get(key, null);
		if (raw == null) return;
		Double v = toDouble(eng.getCastUtil().toString(raw, null));
		if (v != null) settings.setWarnUtilization(v);
	}

	private static Integer toInteger(String str) {
		return toInteger(str, null);
	}

	private static Integer toInteger(String str, Integer defaultValue) {
		if (Util.isEmpty(str, true)) return defaultValue;
		try {
			return Integer.valueOf(str.trim());
		}
		catch (Exception e) {
			return S3ResourceProvider.toIntValue(str, defaultValue == null ? 0 : defaultValue.intValue());
		}
	}

	private static Long toLong(String str) {
		if (Util.isEmpty(str, true)) return null;
		try {
			return Long.valueOf(str.trim());
		}
		catch (Exception e) {
			return null;
		}
	}

	private static Double toDouble(String str) {
		if (Util.isEmpty(str, true)) return null;
		try {
			return Double.valueOf(str.trim());
		}
		catch (Exception e) {
			return null;
		}
	}
}
