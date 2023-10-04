package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Query;
import lucee.runtime.util.Cast;

public class S3ListBuckets extends S3Function {

	private static final long serialVersionUID = -1304574485741405364L;

	public static Query call(PageContext pc, String accessKeyId, String secretAccessKey, String host, double timeout) throws PageException {

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			return s3.listBucketsAsQuery();

		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toDoubleValue(args[3]));
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toString(args[1]), null, 0);
		if (args.length == 1) return call(pc, cast.toString(args[0]), null, null, 0);
		if (args.length == 0) return call(pc, null, null, null, 0);
		throw engine.getExceptionUtil().createFunctionException(pc, "S3ListBuckets", 0, 4, args.length);
	}
}