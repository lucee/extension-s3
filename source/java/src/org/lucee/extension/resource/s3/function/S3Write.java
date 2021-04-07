package org.lucee.extension.resource.s3.function;

import java.io.File;

import org.jets3t.service.acl.AccessControlList;
import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Write extends S3Function {

	private static final long serialVersionUID = -1542466765971539713L;

	public static Object call(PageContext pc, String bucketName, String objectName, Object value, String charset, String mimeType, Object objACL, String location,
			String accessKeyId, String secretAccessKey, double timeout) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();

		AccessControlList acl = null;
		try {
			acl = objACL != null ? AccessControlListUtil.toAccessControlList(objACL) : null;
		}
		catch (S3Exception s3e) {
			throw eng.getCastUtil().toPageException(s3e);
		}
		if (eng.getStringUtil().isEmpty(mimeType, true)) mimeType = null;
		if (eng.getStringUtil().isEmpty(location, true)) location = null;

		try {
			// create S3 Instance
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey), toTimeout(timeout));

			// binary
			if (eng.getDecisionUtil().isBinary(value)) {
				s3.write(bucketName, objectName, eng.getCastUtil().toBinary(value), mimeType, acl, location);
			}
			else if (value instanceof File) {
				File f = (File) value;
				s3.write(bucketName, objectName, f, acl, location);
			}
			else {
				if (eng.getStringUtil().isEmpty(charset, true)) charset = null;
				s3.write(bucketName, objectName, eng.getCastUtil().toString(value), mimeType, charset, acl, location);
			}
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

		if (args.length == 10) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5],
				cast.toString(args[6]), cast.toString(args[7]), cast.toString(args[8]), cast.toDoubleValue(args[9]));
		if (args.length == 9) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5],
				cast.toString(args[6]), cast.toString(args[7]), cast.toString(args[8]), 0);
		if (args.length == 8) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5],
				cast.toString(args[6]), cast.toString(args[7]), null, 0);
		if (args.length == 7) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5],
				cast.toString(args[6]), null, null, 0);
		if (args.length == 6)
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), args[5], null, null, null, 0);
		if (args.length == 5) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), cast.toString(args[4]), null, null, null, null, 0);
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], cast.toString(args[3]), null, null, null, null, null, 0);
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), args[2], null, null, null, null, null, null, 0);

		throw engine.getExceptionUtil().createFunctionException(pc, "S3Write", 3, 10, args.length);
	}
}