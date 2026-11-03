package org.lucee.extension.resource.s3.function;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.ObjectOptions;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Write extends S3Function {

	private static final long serialVersionUID = -1542466765971539713L;

	public static Object call(PageContext pc, String bucketName, String objectName, Object value, String charset, String mimeType, Object objACL, String location,
			String accessKeyId, String secretAccessKey, String host, double timeout) throws PageException {
		return call(pc, bucketName, objectName, value, charset, mimeType, objACL, location, accessKeyId, secretAccessKey, host, timeout, null);
	}

	public static Object call(PageContext pc, String bucketName, String objectName, Object value, String charset, String mimeType, Object objACL, String location,
			String accessKeyId, String secretAccessKey, String host, double timeout, ObjectOptions opts) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();

		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}

		Object acl = null;
		try {
			acl = objACL != null ? AccessControlListUtil.toAccessControlList(objACL) : null;
		}
		catch (S3Exception s3e) {
			throw eng.getCastUtil().toPageException(s3e);
		}
		if (eng.getStringUtil().isEmpty(mimeType, true)) mimeType = null;
		if (eng.getStringUtil().isEmpty(location, true)) location = null;

		try {
			// create S3 Instance
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());

			value = toResource(pc, value);

			// binary
			if (eng.getDecisionUtil().isBinary(value)) {
				Object[] ct = resolveContentType(pc, "S3Write", 5, mimeType, opts, false);
				s3.write(bucketName, objectName, eng.getCastUtil().toBinary(value), (String) ct[1], acl, location, (ObjectOptions) ct[0]);
			}
			else if (value instanceof File) {
				Object[] ct = resolveContentType(pc, "S3Write", 5, mimeType, opts, false);
				s3.write(bucketName, objectName, (File) value, acl, location, (ObjectOptions) ct[0]);
			}
			else if (value instanceof Resource) {
				Object[] ct = resolveContentType(pc, "S3Write", 5, mimeType, opts, false);
				s3.write(bucketName, objectName, (Resource) value, acl, location, (ObjectOptions) ct[0]);
			}
			else {
				if (eng.getStringUtil().isEmpty(charset, true)) charset = null;
				Charset cs = eng.getCastUtil().toCharset(charset, null);
				Object[] ct = resolveContentType(pc, "S3Write", 5, mimeType, opts, true);
				ObjectOptions textOpts = (ObjectOptions) ct[0];
				String textType = (String) ct[1];
				// text without a defined type is plain text (the charset is added to it)
				if (textType == null && (textOpts == null || textOpts.getContentType() == null)) textType = "text/plain";
				s3.write(bucketName, objectName, eng.getCastUtil().toString(value), textType, cs, acl, location, textOpts);
			}
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
		return null;
	}

	public static Object toResource(PageContext pc, Object value) {
		// an empty string is text (it would resolve to the current directory)
		if (value instanceof CharSequence && value.toString().isEmpty()) return value;
		Resource res = toResource(pc, value, true, null);
		if (res != null) {
			// a string is only a file when it names an existing file, a directory can not be written as an object
			if (value instanceof CharSequence && !res.isFile()) return value;
			return res;
		}
		return value;
	}

	public static Resource toResource(PageContext pc, Object value, boolean needToExist, Resource defaultValue) {
		if (value instanceof CharSequence) {
			String str = value.toString();
			if (needToExist) {
				if (str.length() <= 10240) {

					try {
						return CFMLEngineFactory.getInstance().getResourceUtil().toResourceExisting(pc, str);
					}
					catch (Exception e) {
					}

				}
			}
			else return CFMLEngineFactory.getInstance().getCastUtil().toResource(str, defaultValue);
		}
		else if (value instanceof Resource) return (Resource) value;
		else if (value instanceof File) return CFMLEngineFactory.getInstance().getCastUtil().toResource(value, defaultValue);

		// getResource
		try {
			Method m = value.getClass().getMethod("getResource", new Class[0]);
			if (m != null) {
				Object obj = m.invoke(value, new Object[0]);
				if (obj instanceof Resource) return (Resource) obj;
			}
		}
		catch (Exception e) {
		}

		// getFile
		try {
			Method m = value.getClass().getMethod("getFile", new Class[0]);
			if (m != null) {
				Object obj = m.invoke(value, new Object[0]);
				if (obj instanceof File) return CFMLEngineFactory.getInstance().getCastUtil().toResource(obj, defaultValue);
			}
		}
		catch (Exception e) {
		}

		return defaultValue;
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();

		// arguments after the timeout: metadata, headers, storageClass
		ObjectOptions opts = null;
		int len = args.length;
		if (len > 11) {
			if (len > 14) throw engine.getExceptionUtil().createFunctionException(pc, "S3Write", 3, 14, len);
			opts = toObjectOptions(pc, "S3Write", 12, args[11], len > 12 ? args[12] : null, len > 13 ? args[13] : null);
			// ignore the arguments not defined at the end, so the number of arguments matches what the user defined
			len = 11;
			while (len > 3 && args[len - 1] == null)
				len--;
		}

		if (len == 11) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], cast.toString(args[6]),
					cast.toString(args[7]), cast.toString(args[8]), cast.toString(args[9]), cast.toDoubleValue(args[10]), opts);
		}
		if (len == 10) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], cast.toString(args[6]),
					cast.toString(args[7]), cast.toString(args[8]), cast.toString(args[9]), 0, opts);
		}
		if (len == 9) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], cast.toString(args[6]),
					cast.toString(args[7]), cast.toString(args[8]), null, 0, opts);
		}
		if (len == 8) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], cast.toString(args[6]),
					cast.toString(args[7]), null, null, 0, opts);
		}
		if (len == 7) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], cast.toString(args[6]), null, null,
					null, 0, opts);
		}
		if (len == 6) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], null, null, null, null, 0, opts);
		}
		if (len == 5) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), null, null, null, null, null, 0, opts);
		}
		if (len == 4) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), null, null, null, null, null, null, 0, opts);
		}
		if (len == 3) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], null, null, null, null, null, null, null, 0, opts);
		}
		throw engine.getExceptionUtil().createFunctionException(pc, "S3Write", 3, 14, args.length);
	}
}