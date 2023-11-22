package org.lucee.extension.resource.s3;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.listener.S3InfoListener;
import org.lucee.extension.resource.s3.util.print;

import lucee.commons.io.res.filter.ResourceFilter;
import lucee.commons.io.res.filter.ResourceNameFilter;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;

public class S3ResourceS3InfoListener implements S3InfoListener {
	public Map<String, S3Resource> records = new LinkedHashMap<>();
	private CFMLEngine engine;
	private S3ResourceProvider provider;
	private S3 s3;
	private S3Properties props;
	private String location;
	private ResourceNameFilter nameFilter;
	private ResourceFilter filter;

	public S3ResourceS3InfoListener(S3ResourceProvider provider, S3 s3, S3Properties props, String location, ResourceNameFilter nameFilter, ResourceFilter filter) {
		print.e("S3ResourceS3InfoListener:init");
		engine = CFMLEngineFactory.getInstance();
		this.provider = provider;
		this.s3 = s3;
		this.props = props;
		this.location = location;
		this.nameFilter = nameFilter;
		this.filter = filter;

	}

	@Override
	public void invoke(List<S3Info> infos) {
		for (S3Info info: infos) {
			S3Resource res = new S3Resource(engine, s3, props, location, provider, info.getBucketName(), S3.improveObjectName(info.getObjectName(), false));
			if (nameFilter != null && !nameFilter.accept(res.getParentResource(), res.getName())) continue;
			if (filter != null && !filter.accept(res)) continue;
			records.put(res.getAbsolutePath(), res);
		}
	}

	public S3Resource[] getRecords() {
		return records.values().toArray(new S3Resource[records.size()]);
	}

}
