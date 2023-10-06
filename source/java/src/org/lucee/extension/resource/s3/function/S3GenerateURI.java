package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3GenerateURI extends S3Function {

	private static final long serialVersionUID = -1361680673704246251L;

	// see https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
	public static String call(PageContext pc, String bucketNameOrPath, String objectName, String type, boolean secure, String accessKeyId, String secretAccessKey, String host,
			double timeout) throws PageException {

		// arn:partition:service:region:account-id:resource-id

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {

			PropsAndEndpoint pae = extractFromPath(eng, bucketNameOrPath, objectName, accessKeyId, secretAccessKey, host);

			// create S3 Instance
			S3 s3 = S3.getInstance(pae.props != null ? pae.props : toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));

			if (Util.isEmpty(type, true) || (type = type.trim()).equalsIgnoreCase("virtualhost")) {
				return s3.generateURI(pae.bucketName, pae.objectName, S3.URI_STYLE_VIRTUAL_HOST, secure);
			}
			else if (type.equalsIgnoreCase("path")) {
				return s3.generateURI(pae.bucketName, pae.objectName, S3.URI_STYLE_PATH, secure);
			}
			else if (type.equalsIgnoreCase("s3")) {
				return s3.generateURI(pae.bucketName, pae.objectName, S3.URI_STYLE_S3, secure);
			}
			else if (type.equalsIgnoreCase("arn")) {
				return s3.generateURI(pae.bucketName, pae.objectName, S3.URI_STYLE_ARN, secure);
			}
			else {
				throw new S3Exception("type [" + type + "] is invalid, valid types are [virtualhost, path, arn or s3]");
			}
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();

		if (args.length == 8) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toBooleanValue(args[3]), cast.toString(args[4]), cast.toString(args[5]),
					cast.toString(args[6]), cast.toDoubleValue(args[7]));
		}
		if (args.length == 7) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toBooleanValue(args[3]), cast.toString(args[4]), cast.toString(args[5]),
					cast.toString(args[6]), 0);
		}
		if (args.length == 6) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toBooleanValue(args[3]), cast.toString(args[4]), cast.toString(args[5]),
					null, 0);
		}
		if (args.length == 5) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toBooleanValue(args[3]), cast.toString(args[4]), null, null, 0);
		}
		if (args.length == 4) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toBooleanValue(args[3]), null, null, null, 0);
		}
		if (args.length == 3) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), true, null, null, null, 0);
		}
		if (args.length == 2) {
			return call(pc, cast.toString(args[0]), cast.toString(args[1]), null, true, null, null, null, 0);
		}
		if (args.length == 1) {
			return call(pc, cast.toString(args[0]), null, null, true, null, null, null, 0);
		}

		throw engine.getExceptionUtil().createFunctionException(pc, "S3GenerateURL", 1, 8, args.length);
	}
}