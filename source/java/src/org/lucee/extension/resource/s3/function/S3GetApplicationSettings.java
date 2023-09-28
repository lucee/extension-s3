package org.lucee.extension.resource.s3.function;

import org.lucee.extension.resource.s3.S3Properties;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;

public class S3GetApplicationSettings extends S3Function {

	private static final long serialVersionUID = -8254283477292061372L;

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		if (args.length == 0) {
			return S3Properties.getApplicationData(pc);
		}
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		throw engine.getExceptionUtil().createFunctionException(pc, "S3GetApplicationSettings", 0, 0, args.length);
	}
}