package org.lucee.extension.resource.s3.pool;

import com.amazonaws.services.s3.AmazonS3;

public class AmazonS3AndPool {
	public final AmazonS3 amazonS3;
	public final AmazonS3Pool amazonS3Pool;
	private boolean invalidate;

	public AmazonS3AndPool(AmazonS3Pool amazonS3Pool, AmazonS3 amazonS3) {
		this.amazonS3Pool = amazonS3Pool;
		this.amazonS3 = amazonS3;
	}

	public void doInvalidate() {
		this.invalidate = true;
	}

	public boolean isInvalidated() {
		return this.invalidate;
	}
}
