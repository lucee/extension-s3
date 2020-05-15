package org.lucee.extension.resource.s3.function;

import org.jets3t.service.model.S3Object;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3ResourceProvider;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Query;
import lucee.runtime.util.Cast;
import lucee.runtime.util.Creation;

public class S3ListBucket extends S3Function {

	private static final long serialVersionUID = 3486553628255584848L;

	public static Query call(PageContext pc, String bucketName, String accessKeyId, String secretAccessKey, double timeout) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Creation creator = eng.getCreationUtil();

		final Key objectName = creator.createKey("objectName");
		final Key size = creator.createKey("size");
		final Key lastModified = creator.createKey("lastModified");

		try {
			S3 s3 = S3ResourceProvider.getS3(toS3Properties(pc, accessKeyId, secretAccessKey), toTimeout(timeout));
			S3Object[] objects = s3.listObjects(bucketName);

			Query qry = eng.getCreationUtil().createQuery(new Key[] { objectName, size, lastModified }, objects == null ? 0 : objects.length, "buckets");
			if (objects == null || objects.length == 0) return qry;

			int row = 0;
			for (S3Object obj: objects) {
				row++;
				qry.setAt(objectName, row, obj.getName());
				qry.setAt(lastModified, row, obj.getLastModifiedDate());
				qry.setAt(size, row, obj.getContentLength());
			}
			return qry;
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast cast = engine.getCastUtil();
		if (args.length == 4) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), cast.toDoubleValue(args[3]));
		if (args.length == 3) return call(pc, cast.toString(args[0]), cast.toString(args[1]), cast.toString(args[2]), 0);
		if (args.length == 2) return call(pc, cast.toString(args[0]), cast.toString(args[1]), null, 0);
		if (args.length == 1) return call(pc, cast.toString(args[0]), null, null, 0);
		throw engine.getExceptionUtil().createFunctionException(pc, "S3ListBucket", 1, 4, args.length);
	}
}