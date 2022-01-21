package org.lucee.extension.resource.s3;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.ServiceUtils;

import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;

public class S3ObjectFactory {

	public static S3Object getInstance(Resource res) throws NoSuchAlgorithmException, IOException {
		InputStream is = null;
		try {
			S3Object so = new S3Object(res.getName());
			so.setDataInputStream(is = res.getInputStream());
			// so.setSHA256Hash(ServiceUtils.hashSHA256(res.getInputStream()));
			so.setMd5Hash(ServiceUtils.computeMD5Hash(res.getInputStream()));
			so.setContentLength(res.length());

			String mt = CFMLEngineFactory.getInstance().getResourceUtil().getMimeType(S3.max1000(res), null);
			if (mt != null) so.setContentType(mt);
			return so;
		}
		finally {
			Util.closeEL(is);
		}
	}
}
