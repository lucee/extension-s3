package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Copy extends S3Function {

	private static final long serialVersionUID = 7678256258034826909L;

	public static Object call(PageContext pc, String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName, Object objACL, String location,
			String accessKeyId, String secretAccessKey, String host, double timeout) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();

		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}

		if (Util.isEmpty(trgObjectName, true)) trgObjectName = srcObjectName;

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
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			s3.copyObject(srcBucketName, srcObjectName, trgBucketName, trgObjectName, acl, location);
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

		if (args.length == 10) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), cast.toString(args[7]), cast.toString(args[8]), cast.toDoubleValue(args[9]));
		if (args.length == 9) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), cast.toString(args[7]), cast.toString(args[8]), 0);
		if (args.length == 8) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), cast.toString(args[7]), null, 0);
		if (args.length == 7) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]),
				cast.toString(args[6]), null, null, 0);
		if (args.length == 6)
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], cast.toString(args[5]), null, null, null, 0);
		if (args.length == 5) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), args[4], null, null, null, null, 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), null, null, null, null, null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), null, null, null, null, null, null, 0);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3Copy", 3, 10, args.length);
	}
}