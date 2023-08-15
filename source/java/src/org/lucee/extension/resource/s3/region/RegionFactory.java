package org.lucee.extension.resource.s3.region;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.resource.s3.S3Exception;

import com.amazonaws.regions.Regions;

import lucee.loader.util.Util;

public class RegionFactory {

	public static final Region US_EAST_1 = new Region("us-east-1");
	public static final Region US_EAST_2 = new Region("us-east-2");
	public static final Region ERROR = new Region("<error>");

	private static Map<String, Region> regions = new ConcurrentHashMap<>();

	static {
		Region rr;
		for (Regions r: Regions.values()) {
			rr = new Region(r.getName().toLowerCase());
			regions.put(r.getName().toLowerCase(), rr);
			regions.put(r.toString().toLowerCase(), rr);
			regions.put(r.getDescription().toLowerCase(), rr);
			regions.put(r.name().toLowerCase(), rr);
		}

		regions.put("us", US_EAST_1);
	}

	public static Region getInstance(String region) throws S3Exception {
		if (Util.isEmpty(region, true)) throw new S3Exception("no region defined");
		region = region.toLowerCase().trim();
		Region r = regions.get(region);
		if (r == null) {
			regions.put(region, r = new Region(region));
		}
		return r;
	}

	public static class Region {

		private String name;

		private Region(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return name;
		}

	}
}
