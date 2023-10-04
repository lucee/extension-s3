package org.lucee.extension.resource.s3;

import lucee.loader.util.Util;

public class S3Util {
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
}
