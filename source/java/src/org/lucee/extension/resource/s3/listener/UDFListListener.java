package org.lucee.extension.resource.s3.listener;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Query;
import lucee.runtime.type.UDF;

public class UDFListListener implements ListListener {

	private PageContext pc;
	private UDF listener;
	private CFMLEngine eng;

	public UDFListListener(CFMLEngine eng, PageContext pc, UDF listener) {
		this.eng = eng;
		this.pc = pc;
		this.listener = listener;
	}

	@Override
	public boolean invoke(Query data) throws PageException {
		Object res = listener.call(pc, new Object[] { data }, true);
		if (res == null || Util.isEmpty(res.toString())) return true;
		return eng.getCastUtil().toBooleanValue(res);
	}

	@Override
	public void before() {
	}

	@Override
	public void after() {
	}

}
