package org.lucee.extension.resource.s3.listener;

import java.util.List;

import org.lucee.extension.resource.s3.info.S3Info;

public interface S3InfoListener {

	void invoke(List<S3Info> improveResult);

}
