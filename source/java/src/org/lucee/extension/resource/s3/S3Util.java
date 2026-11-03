package org.lucee.extension.resource.s3;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import com.amazonaws.services.s3.model.S3Object;

import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.config.Config;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Creation;

public class S3Util {
	private static final Class[] EMPTY_CLASS = new Class[0];
	private static final Object[] EMPTY_OBJ = new Object[0];
	private static Method pcGetLogNames;
	private static Method configGetLogNames;

	public static String getSystemPropOrEnvVar(String name, String defaultValue) {
		// env
		String value = System.getenv(name);
		if (!Util.isEmpty(value)) return value;

		// prop
		value = System.getProperty(name);
		if (!Util.isEmpty(value)) return value;

		// env 2
		name = name.replace('.', '_').toUpperCase();
		value = System.getenv(name);
		if (!Util.isEmpty(value)) return value;

		return defaultValue;
	}

	public static String extractLocationFromHostIfNecessary(String location, String host) {
		if (!Util.isEmpty(location, true)) return location.trim();
		if (!Util.isEmpty(host, true) && host.startsWith("s3.")) {
			for (String p: S3.PROVIDERS) {
				host = host.trim();
				if (host.endsWith(p)) {
					if (host.length() - p.length() < 3) continue;
					return host.substring(3, host.length() - p.length());
				}
			}
		}
		return null;
	}

	public static String removeLocationFromHostIfNecessary(String host) {
		if (!Util.isEmpty(host, true) && host.startsWith("s3.")) {
			for (String p: S3.PROVIDERS) {
				host = host.trim();
				if (host.endsWith(p)) {
					if (host.length() - p.length() < 3) continue;
					return "s3" + p;
				}
			}
		}
		return host;
	}

	public static String removeSecret(S3 s3, String msg) {
		return CFMLEngineFactory.getInstance().getStringUtil().replace(msg, s3.getSecretAccessKey(), "...", false, true);
	}

	public static String removeSecret(Resource res, String msg) {
		if (res instanceof S3Resource) {
			return ((S3Resource) res).removeSecret(msg);
		}
		return msg;
	}

	// java.util.Collection<String> getLogNames()
	public static java.util.Collection<String> getLogNames(PageContext pc) throws PageException {
		try {
			if (pcGetLogNames == null || pcGetLogNames.getDeclaringClass() != pc.getClass()) pcGetLogNames = pc.getClass().getMethod("getLogNames", EMPTY_CLASS);
			return (Collection<String>) pcGetLogNames.invoke(pc, EMPTY_OBJ);
		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}

	// public String[] getLogNames() {
	public static String[] getLogNames(Config config) throws PageException {
		try {
			if (configGetLogNames == null || configGetLogNames.getDeclaringClass() != config.getClass())
				configGetLogNames = config.getClass().getMethod("getLogNames", EMPTY_CLASS);
			return (String[]) configGetLogNames.invoke(config, EMPTY_OBJ);
		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}

	/**
	 * Converts an exception to the exception thrown to CFML. When the exception is (or is caused by) a
	 * S3Exception, the details are available in the catch block: errorCode (the code of the service, like
	 * NoSuchKey or AccessDenied) and additional.statusCode, additional.requestId,
	 * additional.extendedRequestId, additional.bucket and additional.objectName (those only when known).
	 * The type and the message are not changed.
	 */
	public static PageException toPageException(Throwable t) {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		PageException pe = eng.getCastUtil().toPageException(t);
		S3Exception s3e = null;
		Throwable c = t;
		for (int i = 0; c != null && i < 10; i++) {
			if (c instanceof S3Exception) {
				s3e = (S3Exception) c;
				break;
			}
			c = c.getCause();
		}
		if (s3e == null) return pe;

		String ec = s3e.getPublicErrorCode();
		if (!Util.isEmpty(ec, true)) pe.setErrorCode(ec);
		Creation creator = eng.getCreationUtil();
		Struct additional = pe.getAdditional();
		if (s3e.getStatusCode() > 0) additional.setEL(creator.createKey("statusCode"), Double.valueOf(s3e.getStatusCode()));
		if (!Util.isEmpty(s3e.getRequestId(), true)) additional.setEL(creator.createKey("requestId"), s3e.getRequestId());
		if (!Util.isEmpty(s3e.getExtendedRequestId(), true)) additional.setEL(creator.createKey("extendedRequestId"), s3e.getExtendedRequestId());
		if (!Util.isEmpty(s3e.getBucketName(), true)) additional.setEL(creator.createKey("bucket"), s3e.getBucketName());
		if (!Util.isEmpty(s3e.getObjectName(), true)) additional.setEL(creator.createKey("objectName"), s3e.getObjectName());
		return pe;
	}

	public static void closeS3Object(S3Object obj) {
		if (obj == null) return;
		try {
			obj.close();
		}
		catch (IOException e) {
			// connection release is best-effort
		}
	}

	public static void closeS3Objects(List<S3Object> objects) {
		if (objects == null || objects.isEmpty()) return;
		for (S3Object obj: objects) {
			closeS3Object(obj);
		}
	}
}
