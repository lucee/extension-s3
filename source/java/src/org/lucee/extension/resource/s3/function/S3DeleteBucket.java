package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3DeleteBucket extends S3Function {

	private static final long serialVersionUID = 7282589538156249399L;

	public static Object call(PageContext pc, String bucketName, boolean force, String accessKeyId, String secretAccessKey, String host, double timeout) throws PageException {

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			s3.delete(bucketName, force);
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
		if (args.length == 6) return call(pc, cast.toString(args[0]), cast.toBooleanValue(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toString(args[4]),
				cast.toDoubleValue(args[5]));
		if (args.length == 5) return call(pc, cast.toString(args[0]), cast.toBooleanValue(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toString(args[4]), 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toBooleanValue(args[1]), cast.toString(args[2]), cast.toString(args[3]), null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toBooleanValue(args[1]), cast.toString(args[2]), null, null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toBooleanValue(args[1]), null, null, null, 0);
		if (args.length == 1) return call(pc, cast.toString(args[0]), true, null, null, null, 0);
		throw engine.getExceptionUtil().createFunctionException(pc, "S3DeleteBucket", 1, 6, args.length);
	}
}