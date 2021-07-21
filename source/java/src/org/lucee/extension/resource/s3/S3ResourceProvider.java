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

import org.jets3t.service.acl.AccessControlList;
import org.lucee.extension.resource.s3.util.ApplicationSettings;
import org.lucee.extension.resource.s3.util.ApplicationSettings.S3PropertiesCollection;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceLock;
import lucee.commons.io.res.ResourceProvider;
import lucee.commons.io.res.Resources;
import lucee.commons.lang.types.RefString;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Excepton;

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
		if (Util.isEmpty(props.getAccessKeyId())) {
			CFMLEngine eng = CFMLEngineFactory.getInstance();
			Excepton util = eng.getExceptionUtil();
			throw util.createPageRuntimeException(eng.getCastUtil().toPageException(new S3Exception("there are no credentials available for this this S3 path. ")));
		}
		String key = props.toString() + ":" + cache;
		S3 s3 = s3s.get(key);
		if (s3 == null) {
			s3s.put(key, s3 = new S3(props, cache));
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
		PageContext pc = CFMLEngineFactory.getInstance().getThreadPageContext();

		path = loadWithNewPattern(pc, props, location, path);

		return new S3Resource(engine, getS3(props, props.getCache() == null ? cache : props.getCache().getMillis()), props, location.getValue(), this, path);
	}

	public static String loadWithNewPattern(PageContext pc, S3Properties properties, RefString storage, final String rawPath) throws RuntimeException {
		String path = rawPath;
		boolean hasCustomCredentials = false;
		String accessKeyId, host, secretAccessKey;
		String defaultLocation;
		AccessControlList defaultACL;
		S3PropertiesCollection propColl = null;

		{

			if (pc != null) {
				try {
					propColl = ApplicationSettings.readS3PropertiesCollection(pc);
				}
				catch (PageException e) {
					throw CFMLEngineFactory.getInstance().getExceptionUtil().createPageRuntimeException(e);
				}
			}

			if (propColl != null && propColl.getDefault() != null) {
				S3Properties s3prop = propColl.getDefault();
				accessKeyId = s3prop.getAccessKeyId();
				secretAccessKey = s3prop.getSecretAccessKey();
				host = s3prop.getHost();
				defaultLocation = s3prop.getLocation();
				defaultACL = s3prop.getACL();
				if (defaultACL == null) defaultACL = AccessControlList.REST_CANNED_PUBLIC_READ;
				properties.setCache(s3prop.getCache());
			}
			else {
				accessKeyId = null;
				secretAccessKey = null;
				host = S3.DEFAULT_HOST;
				defaultLocation = null;
				defaultACL = AccessControlList.REST_CANNED_PUBLIC_READ;
			}
		}
		properties.setACL(defaultACL);
		storage.setValue(defaultLocation);

		int atIndex = path.indexOf('@');
		int slashIndex = path.indexOf('/');
		if (slashIndex == -1) {
			slashIndex = path.length();
			path += "/";
		}
		int index;
		String mappingName = null;
		// key/id
		if (atIndex != -1) {
			index = path.indexOf(':');
			// read properties from url
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
			// get properties from mapping
			else {
				mappingName = path.substring(0, atIndex);
				S3Properties mapping;
				if (propColl != null && (mapping = propColl.getMapping(mappingName)) != null) {
					accessKeyId = mapping.getAccessKeyId();
					secretAccessKey = mapping.getSecretAccessKey();
					host = mapping.getHost();
					defaultLocation = mapping.getLocation();
					defaultACL = mapping.getACL();
					if (defaultACL == null) defaultACL = AccessControlList.REST_CANNED_PUBLIC_READ;
					properties.setCache(mapping.getCache());
					properties.setMappingName(mappingName);
				}
				else {
					accessKeyId = null;
					secretAccessKey = null;
					host = S3.DEFAULT_HOST;
					defaultLocation = null;
					defaultACL = AccessControlList.REST_CANNED_PUBLIC_READ;

				}
				//
			}
		}
		path = prettifyPath(path.substring(atIndex + 1));
		index = path.indexOf('/');
		properties.setHost(host);
		if (index == -1) {
			if (path.equalsIgnoreCase(S3.DEFAULT_HOST) || path.equalsIgnoreCase(host)) {
				properties.setHost(path);
				path = "/";
			}
		}
		else {
			String _host = path.substring(0, index);
			if (_host.equalsIgnoreCase(S3.DEFAULT_HOST) || _host.equalsIgnoreCase(host)) {
				properties.setHost(_host);
				path = path.substring(index);
			}
		}

		// get from system.properties/env.var
		if (Util.isEmpty(accessKeyId, true)) {
			if (Util.isEmpty(mappingName)) {
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskeyid", null);
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3.accesskeyid", null);
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.awsaccesskeyid", null);
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretkey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3.secretkey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretaccesskey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.awssecretkey", null);

				if (Util.isEmpty(secretAccessKey, true) && Util.isEmpty(accessKeyId, true)) {
					CFMLEngine eng = CFMLEngineFactory.getInstance();
					throw eng.getExceptionUtil().createPageRuntimeException(eng.getCastUtil().toPageException(eng.getExceptionUtil().createApplicationException(
							"there are no default credentials available for this this S3 path [s3://" + rawPath + "].",
							"Default credentials can be defined in the Application.cfc (like this.vfs.s3.accessKeyId = \"...\"; this.vfs.s3.awsSecretKey = \"...\";), "
									+ "in the enviroment variables (like LUCEE_VFS_S3_ACCESSKEYID=...;LUCEE_VFS_S3_SECRETKEY=...;), "
									+ "in the system properties variables (like lucee.vfs.s3.accesskeyid=...;lucee.vfs.s3.accesskeyid=...;). The S3 Extension now also supports to define multiple endpoints you can use all at the same time.")));
				}
			}
			else {
				String mnlc = mappingName.toLowerCase();
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3." + mnlc + ".accesskeyid", null);
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3." + mnlc + ".awsaccesskeyid", null);
				if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3." + mnlc + ".accesskey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3." + mnlc + ".secretaccesskey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3." + mnlc + ".secretkey", null);
				if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.vfs.s3." + mnlc + ".awssecretkey", null);

				if (Util.isEmpty(secretAccessKey, true) && Util.isEmpty(accessKeyId, true)) {
					CFMLEngine eng = CFMLEngineFactory.getInstance();
					throw eng.getExceptionUtil().createPageRuntimeException(eng.getCastUtil().toPageException(eng.getExceptionUtil().createApplicationException(
							"there are no credentials available for this this S3 path [s3://" + rawPath + "] for the name [" + mappingName + "].",
							"Credentials can be defined in the Application.cfc (like this.vfs.s3[\"" + mappingName + "\"].accessKeyId = \"...\"; this.vfs.s3[\"" + mappingName
									+ " \"].awsSecretKey = \"...\";), " + "in the enviroment variables (like LUCEE_VFS_S3_" + mappingName.toUpperCase()
									+ "_ACCESSKEYID=...;LUCEE_VFS_S3_" + mappingName.toUpperCase() + "_SECRETKEY=...;), " + "in the system properties variables (like lucee.vfs.s3."
									+ mappingName.toLowerCase() + ".accesskeyid=...;lucee.vfs.s3." + mappingName.toLowerCase() + ".secretkey=...;)")));
				}
				properties.setMappingName(mappingName);
			}
		}
		properties.setSecretAccessKey(secretAccessKey);
		properties.setAccessKeyId(accessKeyId);
		properties.setCustomCredentials(hasCustomCredentials);
		return path;
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