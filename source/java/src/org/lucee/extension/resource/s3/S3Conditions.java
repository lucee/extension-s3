package org.lucee.extension.resource.s3;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;

import lucee.loader.util.Util;

/**
 * Conditions of a conditional S3 request (see
 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html). Reads and
 * copies support all four conditions, writes only support ifMatch and ifNoneMatch.
 */
public final class S3Conditions {

	private static final String HEADER_IF_MATCH = "If-Match";
	private static final String HEADER_IF_NONE_MATCH = "If-None-Match";

	private final List<String> ifMatch;
	private final List<String> ifNoneMatch;
	private final Date ifModifiedSince;
	private final Date ifUnmodifiedSince;

	private S3Conditions(List<String> ifMatch, List<String> ifNoneMatch, Date ifModifiedSince, Date ifUnmodifiedSince) {
		this.ifMatch = ifMatch;
		this.ifNoneMatch = ifNoneMatch;
		this.ifModifiedSince = ifModifiedSince;
		this.ifUnmodifiedSince = ifUnmodifiedSince;
	}

	/**
	 * @return the conditions or null if no condition is defined at all
	 */
	public static S3Conditions of(String ifMatch, String ifNoneMatch, Date ifModifiedSince, Date ifUnmodifiedSince) {
		List<String> match = toETags(ifMatch);
		List<String> noneMatch = toETags(ifNoneMatch);
		if (match == null && noneMatch == null && ifModifiedSince == null && ifUnmodifiedSince == null) return null;
		return new S3Conditions(match, noneMatch, ifModifiedSince, ifUnmodifiedSince);
	}

	/**
	 * the ETag can be defined with or without the quotes, "*" matches any existing object, more than
	 * one ETag can be defined as a comma separated list
	 */
	private static List<String> toETags(String str) {
		if (Util.isEmpty(str, true)) return null;
		List<String> list = new ArrayList<String>();
		for (String tag: str.split(",")) {
			tag = tag.trim();
			if (tag.isEmpty()) continue;
			if (!tag.equals("*") && !tag.startsWith("\"") && !tag.startsWith("W/")) tag = "\"" + tag + "\"";
			list.add(tag);
		}
		return list.isEmpty() ? null : list;
	}

	public boolean hasWriteIncompatible() {
		return ifModifiedSince != null || ifUnmodifiedSince != null;
	}

	/**
	 * writes (PutObject) can only be conditional on the ETag
	 * 
	 * @throws S3Exception
	 */
	public void validateForWrite() throws S3Exception {
		if (hasWriteIncompatible()) throw new S3Exception("ifModifiedSince and ifUnmodifiedSince are not supported when writing an object, only ifMatch and ifNoneMatch are");
	}

	public void apply(GetObjectRequest req) {
		if (ifMatch != null) for (String tag: ifMatch)
			req.withMatchingETagConstraint(tag);
		if (ifNoneMatch != null) for (String tag: ifNoneMatch)
			req.withNonmatchingETagConstraint(tag);
		if (ifModifiedSince != null) req.setModifiedSinceConstraint(ifModifiedSince);
		if (ifUnmodifiedSince != null) req.setUnmodifiedSinceConstraint(ifUnmodifiedSince);
	}

	/**
	 * the conditions of a copy are conditions for the source object
	 */
	public void apply(CopyObjectRequest req) {
		if (ifMatch != null) for (String tag: ifMatch)
			req.withMatchingETagConstraint(tag);
		if (ifNoneMatch != null) for (String tag: ifNoneMatch)
			req.withNonmatchingETagConstraint(tag);
		if (ifModifiedSince != null) req.setModifiedSinceConstraint(ifModifiedSince);
		if (ifUnmodifiedSince != null) req.setUnmodifiedSinceConstraint(ifUnmodifiedSince);
	}

	/**
	 * sets the conditions as http headers (PutObject, CompleteMultipartUpload)
	 */
	public void applyAsHeaders(AmazonWebServiceRequest req) {
		if (ifMatch != null) req.putCustomRequestHeader(HEADER_IF_MATCH, join(ifMatch));
		if (ifNoneMatch != null) req.putCustomRequestHeader(HEADER_IF_NONE_MATCH, join(ifNoneMatch));
	}

	private static String join(List<String> list) {
		StringBuilder sb = new StringBuilder();
		for (String s: list) {
			if (sb.length() > 0) sb.append(", ");
			sb.append(s);
		}
		return sb.toString();
	}

	public S3PreconditionFailedException toException(String action, String target) {
		return new S3PreconditionFailedException("precondition failed, could not " + action + " [" + target + "], the condition(s) [" + toString() + "] are not met");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (ifMatch != null) sb.append("ifMatch:").append(join(ifMatch));
		if (ifNoneMatch != null) sb.append(sb.length() > 0 ? ", " : "").append("ifNoneMatch:").append(join(ifNoneMatch));
		if (ifModifiedSince != null) sb.append(sb.length() > 0 ? ", " : "").append("ifModifiedSince:").append(ifModifiedSince);
		if (ifUnmodifiedSince != null) sb.append(sb.length() > 0 ? ", " : "").append("ifUnmodifiedSince:").append(ifUnmodifiedSince);
		return sb.toString();
	}
}
