/**
 * Copyright (c) 2015, Lucee Assosication Switzerland
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either 
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public 
 * License along with this library.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package org.lucee.extension.resource.s3;

import java.io.IOException;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;

public class S3Exception extends IOException {

	private static final long serialVersionUID = 454222134889105256L;
	private String ec;
	private long proposedSize;
	private int statusCode;
	private String requestId;
	private String extendedRequestId;
	private String bucketName;
	private String objectName;

	public S3Exception(String message) {
		super(message);
	}

	public S3Exception(String message, Throwable t) {
		super(message, t);
		if (t instanceof AmazonServiceException) setServiceDetails((AmazonServiceException) t);
	}

	/**
	 * keeps the structured details of an error response of the service (http status, error code,
	 * request ids and, if the server reports them, bucket and key)
	 */
	public void setServiceDetails(AmazonServiceException se) {
		if (ec == null) ec = se.getErrorCode();
		statusCode = se.getStatusCode();
		requestId = se.getRequestId();
		if (se instanceof AmazonS3Exception) {
			AmazonS3Exception ase = (AmazonS3Exception) se;
			extendedRequestId = ase.getExtendedRequestId();
			Map<String, String> details = ase.getAdditionalDetails();
			if (details != null) {
				String v = details.get("BucketName");
				if (v == null) v = details.get("Bucket");
				if (v != null && v.trim().length() > 0) bucketName = v;
				v = details.get("Key");
				if (v != null && v.length() > 0) objectName = v;
			}
		}
	}

	public void setErrorCode(String ec) {
		this.ec = ec;
	}

	/**
	 * the error code as it is shown to CFML (catch.errorCode), see getPublicErrorCode()
	 */
	public String getErrorCode() {
		return getPublicErrorCode();
	}

	public void setProposedSize(long proposedSize) {
		this.proposedSize = proposedSize;
	}

	public long getProposedSize() {
		return proposedSize;
	}

	/**
	 * the http status of the response, 0 if the error did not come from a response of the service
	 */
	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String getRequestId() {
		return requestId;
	}

	public String getExtendedRequestId() {
		return extendedRequestId;
	}

	public String getBucketName() {
		return bucketName;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setLocation(String bucketName, String objectName) {
		this.bucketName = bucketName;
		this.objectName = objectName;
	}

	/**
	 * The error code as it is exposed to CFML. It is the code of the service (NoSuchKey, AccessDenied,
	 * SlowDown, ...). A response without body (HEAD) has no such code, the SDK then uses the status line
	 * ("404 Not Found"), which is mapped to a name (NotFound, Forbidden, ...).
	 */
	public String getPublicErrorCode() {
		String code = ec;
		if (code != null) code = code.trim();
		if (code == null || code.length() == 0) {
			if (statusCode == 0) return null;
			return fromStatus(statusCode, null);
		}
		// "404 Not Found"
		int i = 0;
		while (i < code.length() && Character.isDigit(code.charAt(i)))
			i++;
		if (i == 3 && (i == code.length() || code.charAt(i) == ' ')) {
			return fromStatus(Integer.parseInt(code.substring(0, 3)), code.substring(i).trim());
		}
		return code;
	}

	private static String fromStatus(int status, String reason) {
		if (status == 404) return "NotFound";
		if (status == 403) return "Forbidden";
		if (status == 400) return "BadRequest";
		if (reason != null && reason.length() > 0) return reason.replace(" ", "");
		return String.valueOf(status);
	}
}
