package org.lucee.extension.resource.s3.listener;

import org.lucee.extension.resource.s3.function.S3Download;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.util.Util;
import lucee.runtime.Component;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Query;

public class ComponentListListener implements ListListener {

	private PageContext pc;
	private Component listener;
	private Component csa;
	private Key INVOKE;
	private Key BEFORE;
	private Key AFTER;
	private CFMLEngine eng;

	public ComponentListListener(CFMLEngine eng, PageContext pc, Component listener) throws PageException {
		INVOKE = eng.getCastUtil().toKey("invoke");
		BEFORE = eng.getCastUtil().toKey("before");
		AFTER = eng.getCastUtil().toKey("after");
		this.eng = eng;
		this.pc = pc;
		this.listener = listener;
		csa = S3Download.toComponentSpecificAccess(Component.ACCESS_PRIVATE, listener);

	}

	@Override
	public void before() throws PageException {
		if (S3Download.toFunction(csa.get(BEFORE, null), null) != null) {
			listener.call(pc, BEFORE, new Object[] {});
		}
	}

	@Override
	public boolean invoke(Query data) throws PageException {
		if (S3Download.toFunction(csa.get(INVOKE, null), null) != null) {
			Object res = listener.call(pc, INVOKE, new Object[] { data });
			if (res == null || Util.isEmpty(res.toString())) return true;
			return eng.getCastUtil().toBooleanValue(res);
		}
		else {
			throw eng.getExceptionUtil().createFunctionException(pc, "S3ListBucket", 2, "component",
					"the listener component does not contain a instance function with name [invoke] that is required", null);
		}
	}

	@Override
	public void after() throws PageException {
		if (S3Download.toFunction(csa.get(AFTER, null), null) != null) {
			listener.call(pc, AFTER, new Object[] {});
		}
	}

}
