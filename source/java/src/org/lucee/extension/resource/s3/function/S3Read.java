package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Conditions;
import org.lucee.extension.resource.s3.S3Util;

import com.amazonaws.services.s3.model.S3Object;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Read extends S3Function {

	private static final long serialVersionUID = 811465114006696746L;

	public static String call(PageContext pc, String bucketName, String objectName, String charset, String accessKeyId, String secretAccessKey, String host, double timeout)
			throws PageException {
		return call(pc, bucketName, objectName, charset, accessKeyId, secretAccessKey, host, timeout, null);
	}

	public static String call(PageContext pc, String bucketName, String objectName, String charset, String accessKeyId, String secretAccessKey, String host, double timeout,
			S3Conditions conditions) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		S3Object obj = null;
		try {
			// create S3 Instance
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());
			obj = s3.getData(bucketName, objectName, conditions);

			// copy data
			return eng.getIOUtil().toString(obj.getObjectContent(), toCharset(pc, charset));

		}
		catch (Exception e) {
			throw toPageException(e);
		}
		finally {
			S3Util.closeS3Object(obj);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		if (args.length > 11 || args.length < 2) throw engine.getExceptionUtil().createFunctionException(pc, "S3Read", 2, 11, args.length);

		String charset = args.length > 2 && args[2] != null ? cast.toString(args[2]) : null;
		String accessKeyId = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		String secretAccessKey = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String host = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		double timeout = args.length > 6 && args[6] != null ? cast.toDoubleValue(args[6]) : 0;
		S3Conditions conditions = toConditions(pc, args.length > 7 ? args[7] : null, args.length > 8 ? args[8] : null, args.length > 9 ? args[9] : null,
				args.length > 10 ? args[10] : null);

		return call(pc, cast.toString(args[0]), cast.toString(args[1]), charset, accessKeyId, secretAccessKey, host, timeout, conditions);
	}
}
