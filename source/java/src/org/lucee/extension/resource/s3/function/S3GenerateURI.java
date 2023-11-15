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

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		if (args.length < 1 || args.length > 8) throw eng.getExceptionUtil().createFunctionException(pc, "S3GenerateURL", 1, 8, args.length);

		// required
		String bucketNameOrPath = cast.toString(args[0]);

		// optional
		String objectName = args.length > 1 && args[1] != null ? cast.toString(args[1]) : null;
		String type = args.length > 2 && args[2] != null ? cast.toString(args[2]) : null;
		boolean secure = args.length > 3 && !isEmpty(args[3]) ? cast.toBooleanValue(args[3]) : true;

		String accessKeyId = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String secretAccessKey = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		String host = args.length > 6 && args[6] != null ? cast.toString(args[6]) : null;
		double timeout = args.length > 7 && !isEmpty(args[7]) ? cast.toDoubleValue(args[7]) : 0;

		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}

		try {

			PropsAndEndpoint pae = extractFromPath(eng, bucketNameOrPath, objectName, accessKeyId, secretAccessKey, host);

			// create S3 Instance
			S3 s3 = S3.getInstance(pae.props != null ? pae.props : toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());

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

	// String accessKeyId, String secretAccessKey, String host,double timeout) throws PageException {

}