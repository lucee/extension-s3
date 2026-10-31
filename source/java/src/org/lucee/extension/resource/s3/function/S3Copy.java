package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Conditions;
import org.lucee.extension.resource.s3.S3Exception;

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
		return call(pc, srcBucketName, srcObjectName, trgBucketName, trgObjectName, objACL, location, accessKeyId, secretAccessKey, host, timeout, null);
	}

	public static Object call(PageContext pc, String srcBucketName, String srcObjectName, String trgBucketName, String trgObjectName, Object objACL, String location,
			String accessKeyId, String secretAccessKey, String host, double timeout, S3Conditions conditions) throws PageException {
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
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());
			s3.copyObject(srcBucketName, srcObjectName, trgBucketName, trgObjectName, acl, location, conditions);
		}
		catch (Exception e) {
			throw toPageException(e);
		}
		return null;
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		if (args.length > 14 || args.length < 3) throw engine.getExceptionUtil().createFunctionException(pc, "S3Copy", 3, 14, args.length);

		String trgObjectName = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		Object objACL = args.length > 4 ? args[4] : null;
		String location = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		String accessKeyId = args.length > 6 && args[6] != null ? cast.toString(args[6]) : null;
		String secretAccessKey = args.length > 7 && args[7] != null ? cast.toString(args[7]) : null;
		String host = args.length > 8 && args[8] != null ? cast.toString(args[8]) : null;
		double timeout = args.length > 9 && args[9] != null ? cast.toDoubleValue(args[9]) : 0;
		S3Conditions conditions = toConditions(pc, args.length > 10 ? args[10] : null, args.length > 11 ? args[11] : null, args.length > 12 ? args[12] : null,
				args.length > 13 ? args[13] : null);

		return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), trgObjectName, objACL, location, accessKeyId, secretAccessKey, host, timeout,
				conditions);
	}
}
