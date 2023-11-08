package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.AccessControlListUtil;
import org.lucee.extension.resource.s3.S3;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.util.Cast;

public class S3CreateBucket extends S3Function {

	private static final long serialVersionUID = -4199099330518505504L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		if (args.length > 9 || args.length < 3) throw eng.getExceptionUtil().createFunctionException(pc, "S3CreateBucket", 3, 9, args.length);

		// required
		String bucketName = cast.toString(args[0]);

		// optional
		Object objACL = args.length > 1 && args[1] != null ? args[1] : null;
		String location = args.length > 2 && !isEmpty(args[2]) ? cast.toString(args[2]) : null;
		String accessKeyId = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		String secretAccessKey = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String host = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		double timeout = args.length > 6 && !isEmpty(args[6]) ? cast.toDoubleValue(args[6]) : 0;

		// validate
		Object acl = null;
		try {
			acl = objACL != null ? AccessControlListUtil.toAccessControlList(objACL) : null;
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout));
			s3.createDirectory(bucketName, acl, location);
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
		return null;
	}
}