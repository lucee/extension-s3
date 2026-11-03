package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.ObjectOptions;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Move extends S3Function {

	private static final long serialVersionUID = 7678256258034826909L;

	public static Object call(PageContext pc, String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName, Object objACL, String location,
			String accessKeyId, String secretAccessKey, String host, double timeout) throws PageException {
		return call(pc, srcBucketName, srcObjectName, trgBucketName, trgObjectName, objACL, location, accessKeyId, secretAccessKey, host, timeout, null);
	}

	public static Object call(PageContext pc, String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName, Object objACL, String location,
			String accessKeyId, String secretAccessKey, String host, double timeout, ObjectOptions opts) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		if (Util.isEmpty(trgObjectName, true)) trgObjectName = srcObjectName;

		// canned name (string) or array of grant structs, same as S3Copy
		Object acl = null;
		try {
			acl = objACL != null ? AccessControlListUtil.toAccessControlList(objACL) : null;
		}
		catch (S3Exception s3e) {
			throw eng.getCastUtil().toPageException(s3e);
		}
		if (eng.getStringUtil().isEmpty(location, true)) location = null;

		try {
			// create S3 Instance
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());
			s3.moveObject(srcBucketName, srcObjectName, trgBucketName, trgObjectName, acl, location, opts);
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
		return null;
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();

		// arguments after the timeout: metadata, headers, storageClass
		ObjectOptions opts = null;
		int len = args.length;
		if (len > 10) {
			if (len > 13) throw engine.getExceptionUtil().createFunctionException(pc, "S3Move", 3, 13, len);
			opts = toObjectOptions(pc, "S3Move", 11, args[10], len > 11 ? args[11] : null, len > 12 ? args[12] : null);
			// ignore the arguments not defined at the end, so the number of arguments matches what the user defined
			len = 10;
			while (len > 3 && args[len - 1] == null)
				len--;
		}

		if (len == 10) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), cast.toString(args[7]), cast.toString(args[8]), cast.toDoubleValue(args[9]), opts);
		if (len == 9) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), cast.toString(args[7]), cast.toString(args[8]), 0, opts);
		if (len == 8) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), cast.toString(args[7]), null, 0, opts);
		if (len == 7) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), null, null, 0, opts);
		if (len == 6)
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]), null, null, null, 0, opts);
		if (len == 5) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], null, null, null, null, 0, opts);
		if (len == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), null, null, null, null, null, 0, opts);
		if (len == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), null, null, null, null, null, null, 0, opts);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3Move", 3, 13, args.length);
	}
}