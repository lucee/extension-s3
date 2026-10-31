package org.lucee.extension.resource.s3.function;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Conditions;
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
			String accessKeyId, String secretAccessKey, String host, double timeout, S3Conditions conditions) throws PageException {
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
				s3.write(bucketName, objectName, eng.getCastUtil().toBinary(value), mimeType, acl, location, conditions);
			}
			else if (value instanceof File) {
				s3.write(bucketName, objectName, (File) value, acl, location, conditions);
			}
			else if (value instanceof Resource) {
				s3.write(bucketName, objectName, (Resource) value, acl, location, conditions);
			}
			else {
				if (eng.getStringUtil().isEmpty(charset, true)) charset = null;
				Charset cs = eng.getCastUtil().toCharset(charset, null);
				s3.write(bucketName, objectName, eng.getCastUtil().toString(value), mimeType, cs, acl, location, conditions);
			}
		}
		catch (Exception e) {
			throw toPageException(e);
		}
		return null;
	}

	public static Object toResource(PageContext pc, Object value) {
		Resource res = toResource(pc, value, true, null);
		if (res != null) return res;
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
		if (args.length > 13 || args.length < 3) throw engine.getExceptionUtil().createFunctionException(pc, "S3Write", 3, 13, args.length);

		String charset = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		String mimeType = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		Object objACL = args.length > 5 ? args[5] : null;
		String location = args.length > 6 && args[6] != null ? cast.toString(args[6]) : null;
		String accessKeyId = args.length > 7 && args[7] != null ? cast.toString(args[7]) : null;
		String secretAccessKey = args.length > 8 && args[8] != null ? cast.toString(args[8]) : null;
		String host = args.length > 9 && args[9] != null ? cast.toString(args[9]) : null;
		double timeout = args.length > 10 && args[10] != null ? cast.toDoubleValue(args[10]) : 0;
		S3Conditions conditions = toConditions(pc, args.length > 11 ? args[11] : null, args.length > 12 ? args[12] : null, null, null);

		return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], charset, mimeType, objACL, location, accessKeyId, secretAccessKey, host, timeout, conditions);
	}
}
