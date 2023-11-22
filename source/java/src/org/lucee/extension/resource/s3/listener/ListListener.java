package org.lucee.extension.resource.s3.listener;

import lucee.runtime.exp.PageException;
import lucee.runtime.type.Query;

public interface ListListener {

	public void before() throws PageException;

	public boolean invoke(Query data) throws PageException;

	public void after() throws PageException;

}
