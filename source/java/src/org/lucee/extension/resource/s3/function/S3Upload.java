package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.S3;

import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3Upload extends S3Function {

	private static final long serialVersionUID = 4408651588846605652L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		if (args.length > 9 || args.length < 3) throw eng.getExceptionUtil().createFunctionException(pc, "S3Upload", 3, 9, args.length);

		// required
		String bucketName = cast.toString(args[0]);
		String objectName = cast.toString(args[1]);
		Resource source = S3Write.toResource(pc, args[2], true, null);
		if (source == null) throw eng.getExceptionUtil().createFunctionException(pc, "S3Upload", 3, "source", "could not convert the given source to a file", "");
		else if (!source.isFile()) throw eng.getExceptionUtil().createFunctionException(pc, "S3Upload", 3, "source", "the file [" + source + "] does not exist", "");

		// optional
		Object objACL = args.length > 3 && args[3] != null ? args[3] : null;
		String location = args.length > 4 && !isEmpty(args[4]) ? cast.toString(args[4]) : null;
		String accessKeyId = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		String secretAccessKey = args.length > 6 && args[6] != null ? cast.toString(args[6]) : null;
		String host = args.length > 7 && args[7] != null ? cast.toString(args[7]) : null;
		double timeout = args.length > 8 && !isEmpty(args[8]) ? cast.toDoubleValue(args[8]) : 0;

		// validate
		Object acl = null;
		try {
			acl = objACL != null ? AccessControlListUtil.toAccessControlList(objACL) : null;
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			s3.write(bucketName, objectName, source, acl, location);
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
		return null;
	}
}