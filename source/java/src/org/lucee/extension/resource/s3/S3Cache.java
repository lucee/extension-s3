package org.lucee.extension.resource.s3;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.resource.s3.S3.S3BucketExists;
import org.lucee.extension.resource.s3.S3.ValidUntilElement;
import org.lucee.extension.resource.s3.S3.ValidUntilMap;
import org.lucee.extension.resource.s3.info.S3BucketWrapper;
import org.lucee.extension.resource.s3.info.S3Info;
import org.lucee.extension.resource.s3.region.RegionFactory;
import org.lucee.extension.resource.s3.region.RegionFactory.Region;

import com.amazonaws.services.s3.model.AccessControlList;

import lucee.commons.io.log.Log;

class S3Cache {

	final Harakiri harakiri;
	ValidUntilMap<S3BucketWrapper> buckets;
	Map<String, S3BucketExists> existBuckets;
	final Map<String, ValidUntilMap<S3Info>> objects = new ConcurrentHashMap<String, ValidUntilMap<S3Info>>();
	final Map<String, ValidUntilElement<AccessControlList>> accessControlLists = new ConcurrentHashMap<String, ValidUntilElement<AccessControlList>>();
	final Map<String, Region> regions = new ConcurrentHashMap<String, Region>();
	final Map<String, Region> bucketRegions = new ConcurrentHashMap<String, Region>();
	final Map<String, S3Info> exists = new ConcurrentHashMap<String, S3Info>();

	public S3Cache(Log log) {
		regions.put("US", RegionFactory.US_EAST_1);
		harakiri = new Harakiri(this, log);
	}
}