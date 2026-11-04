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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.lucee.extension.resource.s3.ObjectOptions;
import org.lucee.extension.resource.s3.S3Exception;
import org.lucee.extension.resource.s3.S3HttpPoolSettings;
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
import lucee.runtime.type.Collection;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;

public abstract class S3Function extends BIF {

	private static final long serialVersionUID = 5591490809164267541L;
	private static final long DEFAULT_TIMEOUT = 10000L;

	/**
	 * converts the argument "range" of s3Read, s3ReadBinary and s3Download to {start, end}, end is
	 * Long.MAX_VALUE for an open range.
	 * 
	 * @param range a string like "0-1023", "1024-" (a leading "bytes=" is accepted) or a struct with the
	 *            keys start (optional, default 0) and end (optional)
	 * @return null if no range is defined
	 */
	protected static long[] toRange(Object range) throws PageException {
		if (range == null) return null;
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();

		long start;
		long end = Long.MAX_VALUE;
		if (range instanceof Struct) {
			Struct sct = (Struct) range;
			Object s = sct.get(cast.toKey("start"), null);
			Object e = sct.get(cast.toKey("end"), null);
			if (s == null && e == null) throw eng.getExceptionUtil().createApplicationException("the struct for the argument [range] needs the key [start] and/or [end]");
			start = s == null || Util.isEmpty(cast.toString(s, ""), true) ? 0 : toRangeValue(cast, s, "start");
			if (e != null && !Util.isEmpty(cast.toString(e, ""), true)) end = toRangeValue(cast, e, "end");
		}
		else {
			String str = cast.toString(range).trim();
			if (str.isEmpty()) return null;
			if (str.regionMatches(true, 0, "bytes=", 0, 6)) str = str.substring(6).trim();
			int index = str.indexOf('-');
			if (index < 1) throw eng.getExceptionUtil().createApplicationException("invalid value [" + range + "] for the argument [range], use a format like [0-1023] or [1024-]");
			start = toRangeValue(cast, str.substring(0, index).trim(), "start");
			String strEnd = str.substring(index + 1).trim();
			if (!strEnd.isEmpty()) end = toRangeValue(cast, strEnd, "end");
		}
		if (end < start) throw eng.getExceptionUtil().createApplicationException("invalid value for the argument [range], the end [" + end + "] is smaller than the start [" + start + "]");
		return new long[] { start, end };
	}

	private static long toRangeValue(Cast cast, Object value, String name) throws PageException {
		double d = cast.toDoubleValue(value, Double.NaN);
		if (Double.isNaN(d) || d < 0 || d != Math.floor(d))
			throw CFMLEngineFactory.getInstance().getExceptionUtil().createApplicationException("invalid value [" + value + "] for the [" + name + "] of the argument [range], it needs to be a whole number of 0 or more");
		return (long) d;
	}

	/**
	 * Converts the arguments "metadata", "headers" and "storageClass" of the write and copy functions
	 * to object options.
	 * 
	 * @param pos position (1 based) of the argument "metadata", "headers" and "storageClass" are the
	 *            following arguments
	 * @return the options or null if none of the arguments is defined
	 */
	static ObjectOptions toObjectOptions(PageContext pc, String functionName, int pos, Object metadata, Object headers, Object storageClass) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		ObjectOptions opts = null;

		if (!isNullOrBlank(metadata)) {
			Struct sct = cast.toStruct(metadata, null);
			if (sct == null) throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos, "metadata", "metadata must be a struct", null);
			Map<String, String> map = new LinkedHashMap<String, String>();
			for (Collection.Key k: sct.keys()) {
				Object v = sct.get(k, null);
				if (v == null) throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos, "metadata", "the value of the key [" + k.getString() + "] is null", null);
				map.put(k.getString(), cast.toString(v));
			}
			opts = new ObjectOptions();
			try {
				opts.setUserMetadata(map);
			}
			catch (IllegalArgumentException iae) {
				throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos, "metadata", iae.getMessage(), null);
			}
		}

		if (!isNullOrBlank(headers)) {
			Struct sct = cast.toStruct(headers, null);
			if (sct == null) throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos + 1, "headers", "headers must be a struct", null);
			if (opts == null) opts = new ObjectOptions();
			for (Collection.Key k: sct.keys()) {
				Object v = sct.get(k, null);
				if (v == null) throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos + 1, "headers", "the value of the key [" + k.getString() + "] is null", null);
				String name = k.getString().toLowerCase().replace("-", "").replace("_", "");
				if ("contenttype".equals(name)) opts.setContentType(cast.toString(v));
				else if ("cachecontrol".equals(name)) opts.setCacheControl(cast.toString(v));
				else if ("contentdisposition".equals(name)) opts.setContentDisposition(cast.toString(v));
				else if ("contentencoding".equals(name)) opts.setContentEncoding(cast.toString(v));
				else if ("contentlanguage".equals(name)) opts.setContentLanguage(cast.toString(v));
				else if ("expires".equals(name)) {
					Date date = cast.toDateTime(v, pc.getTimeZone(), null);
					if (date == null)
						throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos + 1, "headers", "the value of the key [" + k.getString() + "] is not a valid date", null);
					opts.setExpires(new Date(date.getTime()));
				}
				else throw eng.getExceptionUtil().createFunctionException(pc, functionName, pos + 1, "headers", "the header [" + k.getString()
						+ "] is not supported, supported headers are [Cache-Control, Content-Disposition, Content-Encoding, Content-Language, Content-Type, Expires], use the argument \"metadata\" for custom metadata",
						null);
			}
		}

		if (!isNullOrBlank(storageClass)) {
			String sc = cast.toString(storageClass).trim().toUpperCase();
			if (opts == null) opts = new ObjectOptions();
			opts.setStorageClass(sc);
		}
		return opts;
	}

	/**
	 * Takes the mime type of the function (if any) and the content type of the headers (if any) and
	 * returns the options that define the content type: for text, the mime type is stored together with
	 * the charset, for everything else it is stored as is.
	 * 
	 * @param mimeType value of the argument "mimeType", can be null
	 * @param opts options from the arguments "metadata", "headers" and "storageClass", can be null
	 * @param text is the content text, so that a charset gets added to the mime type
	 * @return an array with the options and the mime type to use for text (null if not defined)
	 */
	static Object[] resolveContentType(PageContext pc, String functionName, int mimeTypePos, String mimeType, ObjectOptions opts, boolean text) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		if (eng.getStringUtil().isEmpty(mimeType, true)) mimeType = null;
		else mimeType = mimeType.trim();
		String headerType = opts == null ? null : opts.getContentType();

		if (mimeType != null && headerType != null && !mimeType.equalsIgnoreCase(headerType.trim())) {
			throw eng.getExceptionUtil().createFunctionException(pc, functionName, mimeTypePos, "mimeType",
					"the argument [mimeType] and the header [Content-Type] define different content types, define only one of them", null);
		}
		if (mimeType == null && headerType != null && text && headerType.toLowerCase().indexOf("charset") == -1) {
			// the charset is added to the mime type
			mimeType = headerType.trim();
			opts = opts.copy();
			opts.setContentType(null);
		}
		else if (mimeType != null && !text) {
			// a mime type is not applied to anything but text by the extension, so we handle it as content type
			if (opts == null) opts = new ObjectOptions();
			else opts = opts.copy();
			opts.setContentType(mimeType);
			mimeType = null;
		}
		else if (mimeType != null && headerType != null) {
			opts = opts.copy();
			opts.setContentType(null);
		}
		return new Object[] { opts, mimeType };
	}

	private static boolean isNullOrBlank(Object obj) {
		if (obj == null) return true;
		if (obj instanceof CharSequence) return obj.toString().trim().length() == 0;
		return false;
	}

	protected static S3Properties toS3Properties(PageContext pc, String accessKeyId, String secretAccessKey, String host) throws PageException, RuntimeException {

		// directly supplied credentials — skip Application.cfc lookup
		if (!Util.isEmpty(accessKeyId, true) && !Util.isEmpty(secretAccessKey, true)) {
			S3Properties props = new S3Properties();
			props.setSecretAccessKey(secretAccessKey);
			props.setAccessKeyId(accessKeyId);
			if (!Util.isEmpty(host, true)) {
				props.setHost(host);
				props.setCustomHost(true);
			}
			props.setCustomCredentials(true);
			props.setHttpPool(S3HttpPoolSettings.fromEnv());
			applyBucketSuffix(pc, props);
			return props;
		}

		// application context — load the full S3Properties (includes ssl, pathStyleAccess, etc.)
		Struct appData = null;
		try {
			appData = S3Properties.getApplicationData(pc);
		}
		catch (Exception e) {
			// fall through to env var / system property lookup
		}
		// an invalid bucket suffix definition is reported and never ignored (the lookup below
		// would otherwise end up with a misleading "missing credentials" error)
		if (appData != null) S3Properties.loadBucketSuffix(appData, null);
		S3Properties appProps = null;
		try {
			if (appData != null) appProps = S3Properties.load(pc, appData, null);
		}
		catch (Exception e) {
			// fall through to env var / system property lookup
		}
		if (appProps != null && !Util.isEmpty(appProps.getAccessKeyId(), true) && !Util.isEmpty(appProps.getSecretAccessKey(), true)) {
			applyBucketSuffix(pc, appProps);
			return appProps;
		}

		// env var / system property fallback
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretaccesskey", null);
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = S3Util.getSystemPropOrEnvVar("lucee.s3.secretkey", null);

		if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskeyid", null);
		if (Util.isEmpty(accessKeyId, true)) accessKeyId = S3Util.getSystemPropOrEnvVar("lucee.s3.accesskey", null);

		if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.host", null);
		if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.server", null);
		if (Util.isEmpty(host, true)) host = S3Util.getSystemPropOrEnvVar("lucee.s3.provider", null);

		String strCacheRegion = S3Util.getSystemPropOrEnvVar("lucee.s3.cacheregion", null);
		Boolean cacheRegion = Util.isEmpty(strCacheRegion, true) ? null : CFMLEngineFactory.getInstance().getCastUtil().toBoolean(strCacheRegion.trim(), null);

		String strPathStyleAccess = S3Util.getSystemPropOrEnvVar("lucee.s3.pathstyleaccess", null);
		Boolean pathStyleAccess = Util.isEmpty(strPathStyleAccess, true) ? null : CFMLEngineFactory.getInstance().getCastUtil().toBoolean(strPathStyleAccess.trim(), null);

		String strSsl = S3Util.getSystemPropOrEnvVar("lucee.s3.ssl", null);
		Boolean ssl = Util.isEmpty(strSsl, true) ? null : CFMLEngineFactory.getInstance().getCastUtil().toBoolean(strSsl.trim(), null);

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
		if (cacheRegion != null) props.setCacheRegion(cacheRegion.booleanValue());
		if (pathStyleAccess != null) props.setPathStyleAccess(pathStyleAccess);
		if (ssl != null) props.setSsl(ssl);
		props.setHttpPool(S3HttpPoolSettings.fromEnv());

		// the credentials (and the host) come from the environment, the other settings of this.s3
		// still apply, same as in S3ResourceProvider.loadWithNewPattern() (LDEV-6848)
		if (appProps != null) {
			if (appProps.getPathStyleAccess() != null) props.setPathStyleAccess(appProps.getPathStyleAccess());
			if (appProps.getSsl() != null) props.setSsl(appProps.getSsl());
			if (!Util.isEmpty(appProps.getDefaultLocation(), true)) props.setDefaultLocation(appProps.getDefaultLocation());
			if (appProps.getACL() != null) props.setACL(appProps.getACL());
			if (appProps.getHttpPool() != null) props.setHttpPool(appProps.getHttpPool());
		}
		applyBucketSuffix(pc, props);

		return props;
	}

	/**
	 * sets the bucket suffix configuration (environment variable / system property and
	 * Application.cfc) to the properties, does nothing when nothing is configured
	 */
	protected static void applyBucketSuffix(PageContext pc, S3Properties props) throws PageException {
		try {
			S3Properties app = null;
			if (pc != null) {
				Struct appData = S3Properties.getApplicationData(pc);
				if (appData != null) app = S3Properties.loadBucketSuffix(appData, null);
			}
			S3Properties.applyBucketSuffix(props, app, null, null);
		}
		catch (S3Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
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
		if (object instanceof CharSequence) return Util.isEmpty(object.toString(), true);
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

			// explicit credentials overrule the path and the Application.cfc settings. Only the bucket and object
			// come from the path; region, pathStyleAccess, ssl, ... loaded from Application.cfc belong to other
			// credentials (possibly another provider) and must not be mixed in, same as toS3Properties() does.
			if (!Util.isEmpty(accessKeyId, true) && !Util.isEmpty(secretAccessKey, true)) {
				props = new S3Properties();
				props.setAccessKeyId(accessKeyId);
				props.setSecretAccessKey(secretAccessKey);
				if (!Util.isEmpty(host, true)) {
					props.setHost(host);
					props.setCustomHost(true);
				}
				props.setCustomCredentials(true);
				props.setHttpPool(S3HttpPoolSettings.fromEnv());
				try {
					applyBucketSuffix(eng.getThreadPageContext(), props);
				}
				catch (PageException e) {
					throw eng.getCastUtil().toPageRuntimeException(e);
				}
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
