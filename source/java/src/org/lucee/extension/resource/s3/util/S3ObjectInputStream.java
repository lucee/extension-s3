package org.lucee.extension.resource.s3.util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.model.S3Object;

/**
 * Input stream that releases the parent {@link S3Object} (and its HTTP connection) when closed.
 */
public final class S3ObjectInputStream extends FilterInputStream {

	private final S3Object s3Object;
	private boolean closed;

	public S3ObjectInputStream(S3Object s3Object) {
		super(s3Object.getObjectContent());
		this.s3Object = s3Object;
	}

	@Override
	public void close() throws IOException {
		if (closed) return;
		closed = true;
		try {
			super.close();
		}
		finally {
			try {
				s3Object.close();
			}
			catch (IOException e) {
				throw e;
			}
			catch (Exception e) {
				throw new IOException("failed to close S3Object", e);
			}
		}
	}
}
