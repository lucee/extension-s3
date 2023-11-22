package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.listener.ComponentListListener;
import org.lucee.extension.resource.s3.listener.ListListener;
import org.lucee.extension.resource.s3.listener.UDFListListener;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.Component;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.UDF;
import lucee.runtime.util.Cast;

public class S3ListBucket extends S3Function {

	private static final long serialVersionUID = 3486553628255584848L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();
		if (args.length < 1 || args.length > 7) throw eng.getExceptionUtil().createFunctionException(pc, "S3ListBucket", 1, 7, args.length);

		// required
		String bucketName = cast.toString(args[0]);

		// optional
		Object listener = args.length > 1 && args[1] != null ? args[1] : null;
		int blockSize = args.length > 2 && args[2] != null ? cast.toIntValue(args[2]) : 1000;
		String accessKeyId = args.length > 3 && args[3] != null ? cast.toString(args[3]) : null;
		String secretAccessKey = args.length > 4 && args[4] != null ? cast.toString(args[4]) : null;
		String host = args.length > 5 && args[5] != null ? cast.toString(args[5]) : null;
		double timeout = args.length > 6 && !isEmpty(args[6]) ? cast.toDoubleValue(args[6]) : 0;

		// validate

		try {
			S3 s3 = S3.getInstance(toS3Properties(pc, accessKeyId, secretAccessKey, host), toTimeout(timeout), pc.getConfig());

			// no listener
			if (listener == null) {
				return s3.listObjectsAsQuery(bucketName, blockSize, null);
			}
			else {
				ListListener list = toListener(eng, pc, listener);
				list.before();
				s3.listObjectsAsQuery(bucketName, blockSize, toListener(eng, pc, listener));
				list.after();
			}
		}
		catch (Exception e) {
			throw CFMLEngineFactory.getInstance().getCastUtil().toPageException(e);
		}
		return null;
	}

	private ListListener toListener(CFMLEngine eng, PageContext pc, Object listener) throws PageException {
		if (listener instanceof UDF) {
			return new UDFListListener(eng, pc, (UDF) listener);
		}
		if (listener instanceof Component) {
			return new ComponentListListener(eng, pc, (Component) listener);
		}
		throw CFMLEngineFactory.getInstance().getExceptionUtil().createFunctionException(pc, "S3ListBucket", 2, "listener",
				"invalid listener type [" + listener.getClass().getName() + "], only functions and components are supported as listeners", "");
	}
}