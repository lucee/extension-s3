package org.lucee.extension.resource.s3;

import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.handlers.RequestHandler2;

import lucee.commons.io.log.Log;

/**
 * Tracks in-flight HTTP requests and logs when the connection pool is under pressure or exhausted.
 */
final class S3HttpPoolMonitor extends RequestHandler2 {

	private static final long WARN_INTERVAL_MS = 60_000L;

	private final int maxConnections;
	private final double warnUtilization;
	private final Log log;
	private final String clientLabel;
	private final AtomicInteger inFlight = new AtomicInteger();
	private volatile long lastWarnAt;

	S3HttpPoolMonitor(S3HttpPoolSettings pool, Log log, String clientLabel) {
		this.maxConnections = pool.getEffectiveMaxConnections();
		this.warnUtilization = pool.getWarnUtilization() == null ? S3HttpPoolSettings.DEFAULT_WARN_UTILIZATION : pool.getWarnUtilization().doubleValue();
		this.log = log;
		this.clientLabel = clientLabel;
	}

	@Override
	public void beforeRequest(Request<?> request) {
		int current = inFlight.incrementAndGet();
		if (log == null || warnUtilization <= 0D) return;

		int warnAt = (int) Math.ceil(maxConnections * warnUtilization);
		if (warnAt < 1) warnAt = 1;
		if (current >= warnAt) {
			long now = System.currentTimeMillis();
			if (now - lastWarnAt >= WARN_INTERVAL_MS) {
				lastWarnAt = now;
				String msg = "S3 HTTP connection pool utilization high: " + current + "/" + maxConnections + " in-flight requests"
						+ " (warn threshold " + (int) (warnUtilization * 100) + "%) for client [" + clientLabel + "]";
				if (current >= maxConnections) {
					log.log(Log.LEVEL_ERROR, "S3", msg + "; pool limit reached, further requests may block until a connection is released");
				}
				else {
					log.log(Log.LEVEL_WARN, "S3", msg + "; consider raising [this.vfs.s3.pool.maxConnections] if this persists");
				}
			}
		}
	}

	@Override
	public void afterResponse(Request<?> request, Response<?> response) {
		inFlight.decrementAndGet();
	}

	@Override
	public void afterError(Request<?> request, Response<?> response, Exception e) {
		inFlight.decrementAndGet();
		if (log == null || e == null) return;
		if (isPoolTimeout(e)) {
			log.log(Log.LEVEL_ERROR, "S3",
					"S3 HTTP connection pool exhausted (timeout waiting for connection from pool) for client [" + clientLabel + "]; "
							+ "in-flight at failure: ~" + inFlight.get() + ", configured maxConnections: " + maxConnections
							+ ". Increase [this.vfs.s3.pool.maxConnections] or reduce concurrent S3 traffic. Cause: " + e.getMessage());
		}
	}

	static boolean isPoolTimeout(Throwable e) {
		while (e != null) {
			if (e.getClass().getName().indexOf("ConnectionPoolTimeoutException") != -1) return true;
			String msg = e.getMessage();
			if (msg != null && msg.indexOf("Timeout waiting for connection from pool") != -1) return true;
			e = e.getCause();
		}
		return false;
	}
}
