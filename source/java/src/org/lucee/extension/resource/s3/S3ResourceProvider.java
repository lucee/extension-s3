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

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceLock;
import lucee.commons.io.res.ResourceProvider;
import lucee.commons.io.res.Resources;
import lucee.commons.lang.types.RefString;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.type.Struct;

public final class S3ResourceProvider implements ResourceProvider {

	private static final long serialVersionUID = 1861539395523424633L;

	private int socketTimeout = -1;
	private int lockTimeout = 20000;
	private int cache = 10000;
	private ResourceLock lock;
	private String scheme = "s3";
	private Map arguments;

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

		/*
		 * print.e("------------------------------b-"); print.e("raw:" + path); print.e("path:" + path);
		 * print.e("AccessKeyId:" + props.getAccessKeyId()); print.e("SecretAccessKey:" +
		 * props.getSecretAccessKey()); print.e("Host:" + props.getHost()); print.e("Location:" +
		 * props.getDefaultLocation()); print.e("ACL"); print.e(props.getACL());
		 * print.e("CustomCredentials:" + props.getCustomCredentials()); print.e("CustomHost:" +
		 * props.getCustomHost());
		 */

		return new S3Resource(engine, S3.getInstance(props, cache), props, location.getValue(), this, path);
	}

	public static String loadWithNewPattern(S3Properties properties, RefString storage, String path, boolean errorWhenNoCred) {
		PageContext pc = null;
		try {
			pc = CFMLEngineFactory.getInstance().getThreadPageContext();
		}
		catch (Exception e) {
		}

		boolean hasCustomHost = false;
		boolean hasCustomCredentials = false;
		String accessKeyId, host, secretAccessKey, defaultLocation, bucket, mapping = null;
		Object defaultACL;
		Struct appData = null;

		// env var / system prop
		{
			secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretaccesskey", null);
			if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretkey", null);

			accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskeyid", null);
			if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskey", null);

			host = S3Util.getSystemPropOrEnvVar("lucee.s3.host", null);
			if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.server", null);
			if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.provider", null);

			defaultLocation = S3Util.getSystemPropOrEnvVar("lucee.s3.location", null);
			if (Util.isEmpty(defaultLocation, true)) defaultLocation = S3Util.getSystemPropOrEnvVar("lucee.s3.defaultLocation", null);
			if (Util.isEmpty(defaultLocation, true)) defaultLocation = S3Util.getSystemPropOrEnvVar("lucee.s3.region", null);

			defaultLocation = S3Util.extractLocationFromHostIfNecessary(defaultLocation, host);

			bucket = S3Util.getSystemPropOrEnvVar("lucee.s3.bucket", null);
			if (Util.isEmpty(bucket, true)) bucket = S3Util.getSystemPropOrEnvVar("lucee.s3.bucketname", null);

			defaultACL = S3Util.getSystemPropOrEnvVar("lucee.s3.acl", null);
			if (defaultACL == null) defaultACL = S3Util.getSystemPropOrEnvVar("lucee.s3.accesscontrollist", null);
		}

		// Application Context Data
		{
			S3Properties prop = null;
			if (pc != null) {
				appData = S3Properties.getApplicationData(pc);
				prop = appData != null ? S3Properties.load(pc, appData, null) : null;// pc.getApplicationContext().getS3();
			}

			if (prop != null) {
				// we only update the credntials if they are complete
				if (!Util.isEmpty(prop.getAccessKeyId()) && !Util.isEmpty(prop.getSecretAccessKey())) {
					accessKeyId = prop.getAccessKeyId();
					secretAccessKey = prop.getSecretAccessKey();
					// because it is unlikely that you have the same credntials for a different host, host is always
					// part of the credntials and cannot be defined standalone
					if (!Util.isEmpty(prop.getHost())) host = prop.getHost();
					if (!Util.isEmpty(prop.getBucket())) bucket = prop.getBucket();

				}
				if (!Util.isEmpty(prop.getDefaultLocation())) defaultLocation = prop.getDefaultLocation();
				if (prop.getACL() != null) defaultACL = prop.getACL();

				defaultLocation = S3Util.extractLocationFromHostIfNecessary(defaultLocation, host);

			}
		}
		if (defaultACL != null) properties.setACL(defaultACL);
		storage.setValue(defaultLocation);

		// Path Data
		int atIndex = path.indexOf('@');
		int colonIndex = path.indexOf(':');
		int slashIndex = path.indexOf('/');
		if (slashIndex == -1) {
			slashIndex = path.length();
			path += "/";
		}
		int index;

		// {accessKeyId}:{secretAccessKey}:{defaultLocation}@{host}/
		if (atIndex != -1) {
			index = path.indexOf(':');
			accessKeyId = null;
			secretAccessKey = null;
			defaultLocation = null;
			defaultACL = null;
			host = null;
			mapping = null;
			if (index != -1 && index < atIndex) {
				hasCustomCredentials = true;
				accessKeyId = path.substring(0, index);
				secretAccessKey = path.substring(index + 1, atIndex);
				index = secretAccessKey.indexOf(':');
				if (index != -1) {
					String strLocation = secretAccessKey.substring(index + 1).trim().toLowerCase();
					secretAccessKey = secretAccessKey.substring(0, index);
					storage.setValue(S3.improveLocation(strLocation));
				}
			}
			else {
				accessKeyId = path.substring(0, atIndex);
				S3Properties prop = appData != null ? S3Properties.load(pc, appData, accessKeyId) : null;
				if (prop != null) {
					mapping = accessKeyId;
					accessKeyId = prop.getAccessKeyId();
					host = prop.getHost();
					bucket = prop.getBucket();
					secretAccessKey = prop.getSecretAccessKey();
					defaultLocation = prop.getDefaultLocation();
					defaultACL = prop.getACL();
					defaultLocation = S3Util.extractLocationFromHostIfNecessary(defaultLocation, host);

				}
			}
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

		if (errorWhenNoCred && (Util.isEmpty(accessKeyId, true) || Util.isEmpty(secretAccessKey, true)))
			throw new RuntimeException(new S3Exception("Could not found accessKeyId / secretAccessKey."

					+ " You can define the credentials as part of the path itself in various ways s3://{accessKeyId}:{secretAccessKey}:{defaultLocation}@{host}/,"
					+ " s3://{accessKeyId}:{secretAccessKey}:{defaultLocation}@/, s3://{accessKeyId}:{secretAccessKey}:{defaultLocation}@{host}/)."

					+ " In the Application.cfc as follows (this.s3.accessKeyId={accessKeyId};this.s3.awsSecretKey={secretAccessKey};this.s3.host={host};this.s3.defaultLocation={defaultLocation}; or"
					+ " this.vfs.s3.accessKeyId={accessKeyId};this.s3.vfs.awsSecretKey={secretAccessKey};this.s3.vfs.host={host};this.s3.vfs.defaultLocation={defaultLocation};) ."

					+ " As enviroment variable (LUCEE_S3_ACCESSKEYID={accessKeyId}; LUCEE_S3_SECRETACCESSKEY={secretAccessKey}; LUCEE_S3_HOST={host}; LUCEE_S3_LOCATION={defaultLocation})."

					+ " As system properties (lucee.s3.accesskeyid={accessKeyId}; lucee.s3.secretaccesskey={secretAccessKey}; lucee.s3.host={host}; lucee.s3.location={defaultLocation})."

					+ " It is also possible to use a mapping as part of the path like this (s3://myMapping@/) and then define that mapping in the Application.cfc like this "

					+ "(this.vfs.s3.myMapping.accessKeyId={accessKeyId};this.vfs.s3.myMapping.awsSecretKey={secretAccessKey};)"

			));

		properties.setSecretAccessKey(secretAccessKey);
		properties.setAccessKeyId(accessKeyId);
		properties.setCustomCredentials(hasCustomCredentials);
		properties.setCustomHost(hasCustomHost);
		properties.setMapping(mapping);
		if (defaultACL != null) properties.setACL(defaultACL);
		if (defaultLocation != null) properties.setDefaultLocation(defaultLocation);
		if (!hasCustomHost && !Util.isEmpty(host, true)) properties.setHost(host);

		/*
		 * if (!Util.isEmpty(bucket, true)) { if (Util.isEmpty(path, true)) { path = "/" + bucket + "/"; }
		 * else if (path.startsWith("/")) { path = "/" + bucket + path; } else { path = "/" + bucket + "/" +
		 * path; } }
		 */

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
		return Util.replace(path, "//", "/", false);
		// return CFMLEngineFactory.getInstance().getStringUtil().replace(path, "//", "/", false, false);
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