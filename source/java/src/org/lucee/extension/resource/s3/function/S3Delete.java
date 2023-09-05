package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Delete extends S3Function {

	private static final long serialVersionUID = -8478080686638374027L;

	public static Object call(PageContext pc, String bucketName, String objectName, boolean force, String accessKeyId, String secretAccessKey, String host, double timeout)
			throws PageException {

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			if (Util.isEmpty(objectName, true)) s3.delete(bucketName, force);
			else s3.delete(bucketName, objectName, force);
			return null;
		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		if (args.length == 7) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toString(args[5]), cast.toDoubleValue(args[6]));
		if (args.length == 6) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toString(args[5]), 0);
		if (args.length == 5)
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), cast.toString(args[4]), null, 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), cast.toString(args[3]), null, null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toBooleanValue(args[2]), null, null, null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toString(args[1]), true, null, null, null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), null, true, null, null, null, 0);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3Delete", 1, 7, args.length);
	}
}