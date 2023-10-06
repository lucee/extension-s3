/**
 *
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
 **/
package org.lucee.extension.resource.s3.function;

import java.nio.charset.Charset;

import org.lucee.extension.resource.s3.S3Properties;
import org.lucee.extension.resource.s3.S3Resource;
import org.lucee.extension.resource.s3.S3ResourceProvider;
import org.lucee.extension.resource.s3.S3Util;

import lucee.commons.io.res.Resource;
import lucee.commons.io.res.ResourceProvider;
import lucee.commons.lang.types.RefString;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.ext.function.BIF;
import lucee.runtime.listener.ApplicationContext;
import lucee.runtime.net.s3.Properties;

public abstract class S3Function extends BIF {

	private static final long serialVersionUID = 5591490809164267541L;
	private static final long DEFAULT_TIMEOUT = 10000L;

	protected static S3Properties toS3Properties(PageContext pc, String accessKeyId, String secretAccessKey, String host) throws PageException, RuntimeException {

		// directly
		if (!Util.isEmpty(accessKeyId, true) && !Util.isEmpty(secretAccessKey, true)) {
			S3Properties props = new S3Properties();
			props.setSecretAccessKey(secretAccessKey);
			props.setAccessKeyId(accessKeyId);
			if (!Util.isEmpty(host, true)) {
				props.setHost(host);
				props.setCustomHost(true);
			}
			props.setCustomCredentials(true);
			return props;
		}

		// application context
		ApplicationContext ac = pc.getApplicationContext();
		if (ac != null) {
			Properties props = ac.getS3();
			if (props != null) {
				accessKeyId = props.getAccessKeyId();
				secretAccessKey = props.getSecretAccessKey();
				if (!Util.isEmpty(accessKeyId, true) && !Util.isEmpty(secretAccessKey, true)) {
					S3Properties s3props = new S3Properties();
					s3props.setSecretAccessKey(secretAccessKey);
					s3props.setAccessKeyId(accessKeyId);
					s3props.setCustomCredentials(false);
					if (props.getHost() != null) {
						s3props.setHost(props.getHost());
						s3props.setCustomHost(true);
					}
					else s3props.setCustomHost(false);

					if (props.getDefaultLocation() != null) {
						s3props.setDefaultLocation(props.getDefaultLocation());
					}

					return s3props;
				}
			}
		}

		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretaccesskey", null);
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretkey", null);

		if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskeyid", null);
		if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskey", null);

		if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.host", null);
		if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.server", null);
		if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.provider", null);

		if (Util.isEmpty(secretAccessKey, true) || Util.isEmpty(accessKeyId, true)) throw CFMLEngineFactory.getInstance().getExceptionUtil().createApplicationException(
				"missing S3 credentials",
				"you can define the credentials as argument for the function [accessKeyId, secretAccessKey], in the application.cfc [this.s3.accessKeyId, this.s3.secretAccessKey] or in the system properties/environment variables [lucee.s3.secretaccesskey,lucee.s3.accesskeyid]");

		S3Properties props = new S3Properties();
		props.setSecretAccessKey(secretAccessKey);
		props.setAccessKeyId(accessKeyId);
		props.setCustomCredentials(false);
		if (!Util.isEmpty(host, true)) {
			props.setHost(host);
			props.setCustomHost(true);
		}
		else props.setCustomHost(false);

		return props;
	}

	protected static long toTimeout(double timeout) {
		if (timeout > 0D) return CFMLEngineFactory.getInstance().getCastUtil().toLongValue(timeout);
		return DEFAULT_TIMEOUT;
	}

	protected static Charset toCharset(PageContext pc, String charset) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		if (!eng.getStringUtil().isEmpty(charset, true)) return CFMLEngineFactory.getInstance().getCastUtil().toCharset(charset);
		return pc.getResourceCharset();
	}

	public static S3Resource toS3Resource(PageContext pc, String url, String functionName) throws PageException {
		Resource res = CFMLEngineFactory.getInstance().getResourceUtil().toResourceNotExisting(pc, url);
		ResourceProvider provider = res.getResourceProvider();
		if (!provider.getScheme().equalsIgnoreCase("s3") || !res.exists()) throw CFMLEngineFactory.getInstance().getExceptionUtil().createFunctionException(pc, functionName, 1,
				"url", "file [" + S3Util.removeSecret(res, url) + "] does not exist.", null);

		return (S3Resource) res;
	}

	public boolean isEmpty(Object object) {
		if (object instanceof CharSequence) Util.isEmpty(object.toString(), true);
		return object == null;
	}

	PropsAndEndpoint extractFromPath(CFMLEngine eng, String bucketName, String objectName, String accessKeyId, String secretAccessKey, String host) {
		S3Properties props = null;
		// handle if the bucketName is a VFS path
		if (Util.isEmpty(objectName) && bucketName != null && bucketName.toLowerCase().startsWith("s3://")) {
			props = new S3Properties();
			props.setAccessKeyId(accessKeyId);
			props.setSecretAccessKey(secretAccessKey);
			props.setHost(host);

			RefString location = eng.getCreationUtil().createRefString(null);
			String[] bo = S3Resource.toBO(S3ResourceProvider.loadWithNewPattern(props, location, bucketName.substring(5), Util.isEmpty(accessKeyId)));
			bucketName = bo[0];
			objectName = bo[1];

			if (objectName != null && objectName.endsWith("/")) objectName = objectName.substring(0, objectName.length() - 1);

			// explicit credentals overrule in path credentials!
			if (!Util.isEmpty(accessKeyId)) {
				props.setAccessKeyId(accessKeyId);
				props.setSecretAccessKey(secretAccessKey);
				props.setHost(host);
			}
		}
		return new PropsAndEndpoint(props, bucketName, objectName);
	}

	static class PropsAndEndpoint {
		S3Properties props;
		String bucketName;
		String objectName;

		public PropsAndEndpoint(S3Properties props, String bucketName, String objectName) {
			this.props = props;
			this.bucketName = bucketName;
			this.objectName = objectName;
		}
	}
}
