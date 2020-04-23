package org.lucee.extension.resource.s3.function;

import org.jets3t.service.model.S3Object;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Read extends S3Function {

	private static final long serialVersionUID = 811465114006696746L;

	public static String call(PageContext pc, String bucketName, String objectName, String charset, String accessKeyId, String secretAccessKey, double timeout)
			throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		try {
			// create S3 Instance
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey), toTimeout(timeout));
			S3Object obj = s3.getData(bucketName, objectName);

			// copy data
			return eng.getIOUtil().toString(obj.getDataInputStream(), toCharset(pc, charset));

		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();

		if (args.length == 6)
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toString(args[4]), cast.toDoubleValue(args[5]));
		if (args.length == 5) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toString(args[4]), 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), null, null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toString(args[1]), null, null, null, 0);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3Read", 2, 6, args.length);
	}
}