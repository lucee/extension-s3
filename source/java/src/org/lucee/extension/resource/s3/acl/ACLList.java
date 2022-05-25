package org.lucee.extension.resource.s3.acl;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class ACLList {
	public static final ACLList CannedPublicRead = new ACLList(CannedAccessControlList.PublicRead);
	public static final ACLList CannedPrivate = new ACLList(CannedAccessControlList.Private);
	public static final ACLList CannedPublicReadWrite = new ACLList(CannedAccessControlList.PublicReadWrite);
	public static final ACLList CannedAuthenticatedRead = new ACLList(CannedAccessControlList.AuthenticatedRead);;

	public final CannedAccessControlList cacl;
	public final AccessControlList acl;

	public ACLList(CannedAccessControlList cacl) {
		this.cacl = cacl;
		this.acl = null;
	}

	public ACLList(AccessControlList acl) {
		this.acl = acl;
		this.cacl = null;
	}

	public Object getAccessControlList() {
		return cacl != null ? cacl : acl;
	}

	public boolean isCanned() {
		return cacl != null;
	}

	public void setACL(PutObjectRequest por) {
		if (isCanned()) por.setCannedAcl(cacl);
		else if (acl != null) por.setAccessControlList(acl);
	}

	public void setACL(CreateBucketRequest por) {
		if (isCanned()) por.setCannedAcl(cacl);
		else if (acl != null) por.setAccessControlList(acl);
	}

	public void setACL(CopyObjectRequest por) {
		if (isCanned()) por.setCannedAccessControlList(cacl);
		else if (acl != null) por.setAccessControlList(acl);
	}

}
