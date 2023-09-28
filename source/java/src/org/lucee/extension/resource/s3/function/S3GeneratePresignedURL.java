package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Properties;
import org.lucee.extension.resource.s3.S3Resource;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.commons.lang.types.RefString;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Struct;
import lucee.runtime.type.dt.DateTime;
import lucee.runtime.util.Cast;

public class S3GeneratePresignedURL extends S3Function {

	private static final long serialVersionUID = 1L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {

		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();

		if (args.length < 1 || args.length < 17) throw eng.getExceptionUtil().createFunctionException(pc, "S3GeneratePresignedURL", 1, 17, args.length);
		String tmp;

		// required
		String bucketNameOrPath = cast.toString(args[0]);

		// optional
		String objectName = args.length > 1 && args[1] != null ? cast.toString(args[1]) : null;
		DateTime expireDate = args.length > 2 && args[2] != null ? cast.toDateTime(args[2], pc.getTimeZone()) : null;
		String httpMethod = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		String sseAlgorithm = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String sseCustomerKey = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		String checksum = args.length > 6 && args[6] != null ? cast.toString(args[6]) : null;
		String contentType = args.length > 7 && args[7] != null ? cast.toString(args[7]) : null;
		String contentDisposition = args.length > 8 && args[8] != null ? cast.toString(args[8]) : null;
		String contentEncoding = args.length > 9 && args[9] != null ? cast.toString(args[9]) : null;
		String versionId = args.length > 10 && args[10] != null ? cast.toString(args[10]) : null;
		Boolean zeroByteContent = args.length > 11 && !isEmpty(args[11]) ? cast.toBoolean(args[11]) : null;
		Struct customResponseHeaders = args.length > 12 && !isEmpty(args[12]) ? cast.toStruct(args[12]) : null;
		String accessKeyId = args.length > 13 && args[13] != null ? cast.toString(args[13]) : null;
		String secretAccessKey = args.length > 14 && args[14] != null ? cast.toString(args[14]) : null;
		String host = args.length > 15 && args[15] != null ? cast.toString(args[15]) : null;
		double timeout = args.length > 16 && !isEmpty(args[16]) ? cast.toDoubleValue(args[16]) : null;

		// for backward compatibility, when host was not existing
		if (eng.getDecisionUtil().isNumber(host)) {
			timeout = eng.getCastUtil().toDoubleValue(host);
			host = null;
		}
		try {
			// create S3 Instance
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));

			// get bucket and object from path
			if (Util.isEmpty(objectName) && ("" + bucketNameOrPath).toLowerCase().startsWith("s3://")) {
				S3Properties props = new S3Properties();
				RefString location = eng.getCreationUtil().createRefString(null);
				String[] bo = S3Resource.toBO(S3ResourceProvider.loadWithNewPattern(props, location, bucketNameOrPath.substring(5), Util.isEmpty(accessKeyId)));
				bucketNameOrPath = bo[0];
				objectName = bo[1];
				if (objectName != null && objectName.endsWith("/")) objectName = objectName.substring(0, objectName.length() - 1);
			}

			return s3.generatePresignedURL(bucketNameOrPath, objectName, expireDate, httpMethod, sseAlgorithm, sseCustomerKey, checksum, contentType, contentDisposition,
					contentEncoding, versionId, zeroByteContent, customResponseHeaders).toExternalForm();

		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}

	private boolean isEmpty(Object object) {
		if (object instanceof CharSequence) Util.isEmpty(object.toString(), true);
		return object == null;
	}
}