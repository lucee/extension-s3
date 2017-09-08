package org.lucee.extension.resource.s3;

import org.jets3t.service.acl.AccessControlList;

public class S3Properties {
	
	private String host=S3.DEFAULT_HOST;
	private String secretAccessKey;
	private String accessKeyId;
	private boolean hasCustomCredentials;
	private AccessControlList acl;

	public void setHost(String host) {this.host=host;}
	public String getHost() {return host;}
	
	public void setSecretAccessKey(String secretAccessKey) {this.secretAccessKey=secretAccessKey;}
	public String getSecretAccessKey() {return secretAccessKey;}

	public void setAccessKeyId(String accessKeyId) {this.accessKeyId=accessKeyId;}
	public String getAccessKeyId() {return accessKeyId;}

	public void setCustomCredentials(boolean hasCustomCredentials) {this.hasCustomCredentials=hasCustomCredentials;}
	public boolean getCustomCredentials() {return hasCustomCredentials;}
	
	public String toString(){
		return new StringBuilder()
		.append("host:").append(host).append(";")
		.append("accessKeyId:").append(accessKeyId).append(";")
		.append("secretAccessKey:").append(secretAccessKey).append(";")
		.append("custom:").append(hasCustomCredentials).append(";")
		.toString();
	}
	public void setACL(AccessControlList acl) {
		this.acl=acl;
	}
	public AccessControlList getACL() {
		return this.acl;
	}
}
