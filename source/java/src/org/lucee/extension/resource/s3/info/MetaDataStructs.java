package org.lucee.extension.resource.s3.info;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;

import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.type.Struct;

/**
 * Builds the "metadata" and "headers" sub structs of the result of s3GetMetaData(), they keep the names
 * exactly as they are stored on the server (the flat top level keys are changed for historical reasons).
 */
public final class MetaDataStructs {

	private MetaDataStructs() {
	}

	/**
	 * the user metadata (x-amz-meta-*) with the original names, without the x-amz-meta- prefix
	 */
	public static Struct toMetadata(ObjectMetadata omd) {
		Struct sct = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		Map<String, String> umd = omd.getUserMetadata();
		if (umd != null) {
			Iterator<Entry<String, String>> it = umd.entrySet().iterator();
			Entry<String, String> e;
			while (it.hasNext()) {
				e = it.next();
				sct.setEL(e.getKey(), e.getValue());
			}
		}
		return sct;
	}

	/**
	 * all headers of the response with their original names, including the user metadata with the
	 * x-amz-meta- prefix
	 */
	public static Struct toHeaders(ObjectMetadata omd) {
		Struct sct = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		Map<String, Object> rmd = omd.getRawMetadata();
		if (rmd != null) {
			Iterator<Entry<String, Object>> it = rmd.entrySet().iterator();
			Entry<String, Object> e;
			while (it.hasNext()) {
				e = it.next();
				sct.setEL(e.getKey(), e.getValue());
			}
		}
		Map<String, String> umd = omd.getUserMetadata();
		if (umd != null) {
			Iterator<Entry<String, String>> it = umd.entrySet().iterator();
			Entry<String, String> e;
			while (it.hasNext()) {
				e = it.next();
				sct.setEL(Headers.S3_USER_METADATA_PREFIX + e.getKey(), e.getValue());
			}
		}
		return sct;
	}
}
