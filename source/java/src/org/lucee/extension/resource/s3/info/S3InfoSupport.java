package org.lucee.extension.resource.s3.info;

import java.util.Date;

import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.Owner;

import lucee.commons.io.log.Log;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.type.Struct;

public abstract class S3InfoSupport implements S3Info {

	protected final S3 s3;
	private Regions region;
	protected Log log;

	public S3InfoSupport(S3 s3, Log log) {
		this.s3 = s3;
		this.log = log;
	}

	@Override
	public Struct getMetaData() throws S3Exception {
		Struct data = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		data.setEL("bucketName", getBucketName());
		String on = getObjectName();
		if (!Util.isEmpty(on)) data.put("objectName", on);
		long lm = getLastModified();
		if (lm > 0) data.put("lastModified", new Date());
		Owner ow = getOwner();
		if (ow != null) {
			data.setEL("owner", ow.getDisplayName());
			data.setEL("ownerName", ow.getDisplayName());
			data.setEL("ownerNd", ow.getId());

		}
		Regions r = getRegion();
		if (r != null) {
			data.setEL("region", region.getName());
		}
		data.setEL("size", getSize());
		data.setEL("directory", isDirectory());
		data.setEL("file", isFile());
		data.setEL("exists", exists());
		data.setEL("bucket", isBucket());

		return data;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof S3Info)) return false;
		S3Info other = (S3Info) obj;
		if (getBucketName() == null) {
			if (other.getBucketName() != null) return false;
		}
		else if (!getBucketName().equals(other.getBucketName())) return false;

		if (getObjectName() == null) {
			if (other.getObjectName() != null) return false;
		}
		return getObjectName().equals(other.getObjectName());
	}

	@Override
	public final Regions getRegion() {
		if (s3 == null) return null;
		if (region == null) {
			synchronized (S3.getToken("S3InfoSupport:region:" + getBucketName())) {
				if (region == null) {
					try {
						region = s3.getBucketRegion(getBucketName(), true);
					}
					catch (S3Exception e) {
						if (log != null) log.error("s3", e);
						else e.printStackTrace();
					}
				}
			}
		}
		return region;
	}
}
