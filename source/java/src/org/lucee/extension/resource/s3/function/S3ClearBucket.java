package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.dt.TimeSpan;
import lucee.runtime.util.Cast;

public class S3ClearBucket extends S3Function {

	private static final long serialVersionUID = -2904887068889788095L;

	public static Object call(PageContext pc, String bucketName, TimeSpan maxAge, String accessKeyId, String secretAccessKey, String host, double timeout) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			s3.clear(bucketName, maxAge == null ? 0 : maxAge.getMillis());
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
		if (args.length == 6) return call(pc, cast.toString(args[0]), args[1] == null ? null : cast.toTimespan(args[1]), cast.toString(args[2]), cast.toString(args[3]),
				cast.toString(args[4]), cast.toDoubleValue(args[5]));
		if (args.length == 5)
			return call(pc, cast.toString(args[0]), args[1] == null ? null : cast.toTimespan(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toString(args[4]), 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), args[1] == null ? null : cast.toTimespan(args[1]), cast.toString(args[2]), cast.toString(args[3]), null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), args[1] == null ? null : cast.toTimespan(args[1]), cast.toString(args[2]), null, null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), args[1] == null ? null : cast.toTimespan(args[1]), null, null, null, 0);
		if (args.length == 1) return call(pc, cast.toString(args[0]), null, null, null, null, 0);
		throw engine.getExceptionUtil().createFunctionException(pc, "S3ClearBucket", 1, 6, args.length);
	}
}