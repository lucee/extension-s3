package org.lucee.extension.resource.s3.function;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3DeleteBucket extends S3Function {

	private static final long serialVersionUID = 7282589538156249399L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		if (args.length == 6) return S3Delete.call(pc, cast.toString(args[0]), null, cast.toBooleanValue(args[1]), cast.toString(args[2]), cast.toString(args[3]),
				cast.toString(args[4]), cast.toDoubleValue(args[5]));
		if (args.length == 5)
			return S3Delete.call(pc, cast.toString(args[0]), null, cast.toBooleanValue(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toString(args[4]), 0);
		if (args.length == 4) return S3Delete.call(pc, cast.toString(args[0]), null, cast.toBooleanValue(args[1]), cast.toString(args[2]), cast.toString(args[3]), null, 0);
		if (args.length == 3) return S3Delete.call(pc, cast.toString(args[0]), null, cast.toBooleanValue(args[1]), cast.toString(args[2]), null, null, 0);
		if (args.length == 2) return S3Delete.call(pc, cast.toString(args[0]), null, cast.toBooleanValue(args[1]), null, null, null, 0);
		if (args.length == 1) return S3Delete.call(pc, cast.toString(args[0]), null, true, null, null, null, 0);
		throw engine.getExceptionUtil().createFunctionException(pc, "S3DeleteBucket", 1, 6, args.length);
	}
}