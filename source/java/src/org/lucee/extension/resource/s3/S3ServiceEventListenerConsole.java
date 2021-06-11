package org.lucee.extension.resource.s3;

import org.jets3t.service.multi.event.CopyObjectsEvent;
import org.jets3t.service.multi.event.CreateBucketsEvent;
import org.jets3t.service.multi.event.CreateObjectsEvent;
import org.jets3t.service.multi.event.DeleteObjectsEvent;
import org.jets3t.service.multi.event.DownloadObjectsEvent;
import org.jets3t.service.multi.event.GetObjectHeadsEvent;
import org.jets3t.service.multi.event.GetObjectsEvent;
import org.jets3t.service.multi.event.ListObjectsEvent;
import org.jets3t.service.multi.event.LookupACLEvent;
import org.jets3t.service.multi.event.UpdateACLEvent;
import org.jets3t.service.multi.s3.MultipartCompletesEvent;
import org.jets3t.service.multi.s3.MultipartStartsEvent;
import org.jets3t.service.multi.s3.MultipartUploadsEvent;
import org.jets3t.service.multi.s3.S3ServiceEventListener;

public class S3ServiceEventListenerConsole implements S3ServiceEventListener {

	@Override
	public void event(ListObjectsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.list");
	}

	@Override
	public void event(CreateObjectsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.createObject");
	}

	@Override
	public void event(CopyObjectsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.copyObject");
	}

	@Override
	public void event(CreateBucketsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.createBucket");
	}

	@Override
	public void event(DeleteObjectsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.deleteObject");
	}

	@Override
	public void event(GetObjectsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.getObject");
	}

	@Override
	public void event(GetObjectHeadsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.getObjectHead");
	}

	@Override
	public void event(LookupACLEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.lookupACL");
	}

	@Override
	public void event(UpdateACLEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.updateACL");
	}

	@Override
	public void event(DownloadObjectsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.downloadObject");
	}

	@Override
	public void event(MultipartUploadsEvent event) {
		System.err.println("S3ServiceEventListenerConsole.multipartUpload");
	}

	@Override
	public void event(MultipartStartsEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.multipartStarts");
	}

	@Override
	public void event(MultipartCompletesEvent arg0) {
		System.err.println("S3ServiceEventListenerConsole.MultipartComplete");
	}

}
