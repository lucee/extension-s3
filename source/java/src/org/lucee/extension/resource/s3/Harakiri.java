package org.lucee.extension.resource.s3;

import java.util.Map;

import org.lucee.extension.resource.s3.info.S3Info;

import lucee.commons.io.log.Log;

public class Harakiri {
	private S3Cache cache;
	private HarakiriThread thread;
	private Log log;

	public Harakiri(S3Cache cache, Log log) {
		this.cache = cache;
		this.log = log;
	}

	public void touch() {
		if (thread == null || !thread.isAlive()) {
			synchronized (this) {
				if (thread == null || !thread.isAlive()) {
					thread = new HarakiriThread(cache, log);
					thread.start();
				}
			}
		}
	}

	private static class HarakiriThread extends Thread {
		private static final long IDLE_TIMEOUT = 10000;
		private static final long INTERVALL = 1000;
		private long lastMod;
		private S3Cache cache;
		private Log log;

		public HarakiriThread(S3Cache cache, Log log) {
			this.cache = cache;
			this.log = log;
		}

		@Override
		public void run() {
			while (true) {
				if (log != null)
					log.debug("S3", "S3 cache observer: checking for elements to flush in the cache, there are " + cache.exists.size() + " elements currently in the cache");
				if (!cache.exists.isEmpty()) {
					long now = System.currentTimeMillis();
					try {
						for (Map.Entry<String, S3Info> e: cache.exists.entrySet()) {
							if (e.getValue().validUntil() < now) {
								if (log != null) log.debug("S3", "S3 cache observer: remove object " + e.getKey() + " from cache");
								cache.exists.remove(e.getKey());

							}
						}
					}
					catch (Exception e) {
						if (log != null) log.error("S3", e);
					}
					lastMod = now;
				}
				// nothing to do ATM
				else {
					long now = System.currentTimeMillis();
					if (lastMod + IDLE_TIMEOUT < now) {
						if (log != null) log.debug("S3", "S3 cache observer: nothing to do, idle timeout reached, stoping observer ");
						break;
					}
					else if (log != null) log.debug("S3", "S3 cache observer: nothing to do, remaining idle for another " + ((lastMod + IDLE_TIMEOUT) - now) + "ms");
				}
				if (log != null) log.debug("S3", "S3 cache observer: sleep for " + INTERVALL + "ms");
				try {
					sleep(INTERVALL);
				}
				catch (InterruptedException e) {
					if (log != null) log.error("S3", e);
				}
			}
		}
	}
}
