package org.lucee.extension.resource.s3.info;

import org.jets3t.service.model.S3Bucket;

public interface S3BucketInfo extends S3Info {
	public S3Bucket getBucket();
}
