/**
 * Copyright (c) 2015, Lucee Assosication Switzerland
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either 
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public 
 * License along with this library.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package org.lucee.extension.resource.s3;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.services.s3.model.CannedAccessControlList;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceLock;
import lucee.commons.io.res.ResourceProvider;
import lucee.commons.io.res.Resources;
import lucee.commons.lang.types.RefString;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.net.s3.Properties;

public final class S3ResourceProvider implements ResourceProvider {

	private static final long serialVersionUID = 1861539395523424633L;

	private int socketTimeout = -1;
	private int lockTimeout = 20000;
	private int cache = 10000;
	private ResourceLock lock;
	private String scheme = "s3";
	private Map arguments;
	private static Map<String, S3> s3s = new ConcurrentHashMap<String, S3>();

	/**
	 * initalize ram resource
	 * 
	 * @param scheme
	 * @param arguments
	 * @return RamResource
	 */
	@Override
	public ResourceProvider init(String scheme, Map arguments) {
		if (!Util.isEmpty(scheme)) this.scheme = scheme;

		if (arguments != null) {
			this.arguments = arguments;
			// socket-timeout
			String strTimeout = (String) arguments.get("socket-timeout");
			if (strTimeout != null) {
				socketTimeout = toIntValue(strTimeout, socketTimeout);
			}
			// lock-timeout
			strTimeout = (String) arguments.get("lock-timeout");
			if (strTimeout != null) {
				lockTimeout = toIntValue(strTimeout, lockTimeout);
			}
			// cache
			String strCache = (String) arguments.get("cache");
			if (strCache != null) {
				cache = toIntValue(strCache, cache);
			}
		}

		return this;
	}

	public static S3 getS3(S3Properties props, long cache) {
		String key = props.toString() + ":" + cache;
		S3 s3 = s3s.get(key);
		if (s3 == null) {
			s3s.put(key, s3 = new S3(props, cache, S3.DEFAULT_LIVE_TIMEOUT, null, true, CFMLEngineFactory.getInstance().getThreadConfig().getLog("application")));
		}
		return s3;
	}

	private int toIntValue(String str, int defaultValue) {
		try {
			return Integer.parseInt(str);
		}
		catch (Throwable t) {
			if (t instanceof ThreadDeath) throw (ThreadDeath) t;
			return defaultValue;
		}
	}

	@Override
	public String getScheme() {
		return scheme;
	}

	@Override
	public Resource getResource(String path) {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		path = engine.getResourceUtil().removeScheme(scheme, path);

		S3Properties props = new S3Properties();
		// S3 s3 = new S3(cache);
		RefString location = engine.getCreationUtil().createRefString(null);

		path = loadWithNewPattern(props, location, path, true);

		return new S3Resource(engine, getS3(props, cache), props, location.getValue(), this, path);
	}

	public static String loadWithNewPattern(S3Properties properties, RefString storage, String path, boolean errorWhenNoCred) {
		PageContext pc = CFMLEngineFactory.getInstance().getThreadPageContext();

		boolean hasCustomHost = false;
		boolean hasCustomCredentials = false;
		String accessKeyId, host, secretAccessKey;
		String defaultLocation;
		CannedAccessControlList defaultACL;
		{
			Properties prop = null;
			if (pc != null) prop = pc.getApplicationContext().getS3();

			if (prop != null) {
				accessKeyId = prop.getAccessKeyId();
				host = prop.getHost();
				secretAccessKey = prop.getSecretAccessKey();
				defaultLocation = prop.getDefaultLocation();
				defaultACL = S3.toACL(prop, null);
			}
			else {
				accessKeyId = null;
				secretAccessKey = null;
				host = S3.DEFAULT_HOST;
				defaultLocation = null;
				defaultACL = null;
			}
		}
		if (defaultACL != null) properties.setACL(defaultACL);

		storage.setValue(defaultLocation);

		int atIndex = path.indexOf('@');
		int slashIndex = path.indexOf('/');
		if (slashIndex == -1) {
			slashIndex = path.length();
			path += "/";
		}
		int index;

		// key/id
		if (atIndex != -1) {
			index = path.indexOf(':');
			if (index != -1 && index < atIndex) {
				hasCustomCredentials = true;
				accessKeyId = path.substring(0, index);
				secretAccessKey = path.substring(index + 1, atIndex);
				index = secretAccessKey.indexOf(':');
				if (index != -1) {
					String strLocation = secretAccessKey.substring(index + 1).trim().toLowerCase();
					secretAccessKey = secretAccessKey.substring(0, index);
					// print.out("storage:"+strStorage);
					storage.setValue(S3.improveLocation(strLocation));
				}
			}
			else accessKeyId = path.substring(0, atIndex);
		}
		path = prettifyPath(path.substring(atIndex + 1));
		index = path.indexOf('/');
		properties.setHost(host);
		if (index == -1) {
			if (isS3Host(path, host)) {
				properties.setHost(path);
				path = "/";
			}
		}
		else {
			String _host = path.substring(0, index);
			if (isS3Host(_host, host)) {
				hasCustomHost = true;
				properties.setHost(_host);
				path = path.substring(index);
			}
		}

		// env var
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretaccesskey", null);
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretkey", null);

		if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskeyid", null);
		if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskey", null);

		if (errorWhenNoCred && (Util.isEmpty(accessKeyId, true) || Util.isEmpty(secretAccessKey, true)))
			throw new RuntimeException(new S3Exception("Could not found accessKeyId / secretAccessKey."

					+ " You can define the credentials as part of the path itself (s3://{accessKeyId}:{secretAccessKey}/)."

					+ " In the Application.cfc as follows (this.s3.accessKeyId={accessKeyId};this.s3.awsSecretKey={secretAccessKey};)."

					+ " As enviroment variable (LUCEE_S3_ACCESSKEYID={accessKeyId}; LUCEE_S3_SECRETACCESSKEY={secretAccessKey})."

					+ " As system properties (lucee.s3.accesskeyid={accessKeyId}; lucee.s3.secretaccesskey={secretAccessKey})."));

		properties.setSecretAccessKey(secretAccessKey);
		properties.setAccessKeyId(accessKeyId);
		properties.setCustomCredentials(hasCustomCredentials);
		properties.setCustomHost(hasCustomHost);
		return path;
	}

	private static boolean isS3Host(String path, String host) {
		if (path.equalsIgnoreCase(host)) return true;

		String pathLC = path.toLowerCase();

		if (!pathLC.startsWith("s3.")) return false;

		for (String provider: S3.PROVIDERS) {
			if (pathLC.endsWith(provider)) return true;
		}
		return false;
	}

	private static String prettifyPath(String path) {
		path = path.replace('\\', '/');
		return CFMLEngineFactory.getInstance().getStringUtil().replace(path, "//", "/", false, false);
		// TODO /aaa/../bbb/
	}

	@Override
	public boolean isAttributesSupported() {
		return false;
	}

	@Override
	public boolean isCaseSensitive() {
		return true;
	}

	@Override
	public boolean isModeSupported() {
		return false;
	}

	@Override
	public void lock(Resource res) throws IOException {
		lock.lock(res);
	}

	@Override
	public void read(Resource res) throws IOException {
		lock.read(res);
	}

	@Override
	public void setResources(Resources res) {
		lock = res.createResourceLock(lockTimeout, true);
	}

	@Override
	public void unlock(Resource res) {
		lock.unlock(res);
	}

	/**
	 * @return the socketTimeout
	 */
	public int getSocketTimeout() {
		return socketTimeout;
	}

	/**
	 * @return the lockTimeout
	 */
	public int getLockTimeout() {
		return lockTimeout;
	}

	/**
	 * @return the cache
	 */
	public int getCache() {
		return cache;
	}

	@Override
	public Map getArguments() {
		return arguments;
	}

	public char getSeparator() {
		return '/';
	}
}