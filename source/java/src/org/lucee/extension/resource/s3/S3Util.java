package org.lucee.extension.resource.s3;

import java.lang.reflect.Method;
import java.util.Collection;

import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.config.Config;
import lucee.runtime.exp.PageException;

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
}
