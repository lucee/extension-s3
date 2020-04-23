package org.lucee.extension.resource.s3.function;

import java.io.ByteArrayOutputStream;

import org.jets3t.service.model.S3Object;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3ReadBinary extends S3Function {

	private static final long serialVersionUID = -4139469412601435295L;

	public static byte[] call(PageContext pc, String bucketName, String objectName, String accessKeyId, String secretAccessKey, double timeout) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		try {
			// create S3 Instance
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey), toTimeout(timeout));
			S3Object obj = s3.getData(bucketName, objectName);

			// copy data
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			eng.getIOUtil().copy(obj.getDataInputStream(), baos, true, true);

			return baos.toByteArray();
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();

		if (args.length == 5) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), cast.toDoubleValue(args[4]));
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toString(args[3]), 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), null, 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toString(args[1]), null, null, 0);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3ReadBinary", 2, 5, args.length);
	}
}
