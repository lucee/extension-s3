package org.lucee.extension.resource.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.lucee.extension.resource.s3.info.S3Info;

import lucee.loader.util.Util;

public class TestCase {

	public static void main(String[] args) throws IOException, ServiceException {
		S3Properties prop = new S3Properties();
		prop.setAccessKeyId("...");
		prop.setSecretAccessKey("...");

		String bucketName = "testInJava";
		String dir1 = "test/";
		String dir2 = "test/sub1/";
		String dir3 = "test/sub1/sub2/";
		String file1 = "test/sub1/sub2/c.txt";
		String file2 = "test/sub1/d.txt";
		// 52568
		long start = System.currentTimeMillis();

		S3 s3 = new S3(prop, 100000);
		try {
			if (s3.exists(bucketName)) s3.delete(bucketName, true);

			List<S3Info> buckets = s3.list(false, false);
			// dump("Buckets",buckets);
			int initalBucketCount = buckets.size();

			// create/delete bucket
			assertFalse(s3.exists(bucketName));
			s3.createDirectory(bucketName, null, null);
			assertTrue(s3.exists(bucketName));
			s3.delete(bucketName, true);
			assertFalse(s3.exists(bucketName));

			// create/delete folder
			assertFalse(s3.exists(bucketName));

			assertFalse(s3.exists(bucketName, dir1));
			assertFalse(s3.isDirectory(bucketName, dir1));
			assertFalse(s3.isFile(bucketName, dir1));

			assertFalse(s3.exists(bucketName, dir2));
			assertFalse(s3.isDirectory(bucketName, dir2));
			assertFalse(s3.isFile(bucketName, dir2));

			assertFalse(s3.exists(bucketName, dir3));
			assertFalse(s3.isDirectory(bucketName, dir3));
			assertFalse(s3.isFile(bucketName, dir3));

			s3.createDirectory(bucketName, dir3, null, null);

			assertTrue(s3.exists(bucketName));

			assertTrue(s3.exists(bucketName, dir1));
			assertTrue(s3.isDirectory(bucketName, dir1));
			assertFalse(s3.isFile(bucketName, dir1));

			assertTrue(s3.exists(bucketName, dir2));
			assertTrue(s3.isDirectory(bucketName, dir2));
			assertFalse(s3.isFile(bucketName, dir2));

			assertTrue(s3.exists(bucketName, dir3));
			assertTrue(s3.isDirectory(bucketName, dir3));
			assertFalse(s3.isFile(bucketName, dir3));

			buckets = s3.list(false, false);
			contains(buckets, bucketName, null);
			assertEquals(initalBucketCount + 1, buckets.size());

			// must fail
			boolean failed = false;
			try {
				s3.delete(bucketName, false);
			}
			catch (S3Exception e) {
				failed = true;
			}
			assertTrue(failed);
			s3.delete(bucketName, true);
			assertFalse(s3.exists(bucketName));
			assertFalse(s3.exists(bucketName, dir1));
			assertFalse(s3.exists(bucketName, dir2));
			assertFalse(s3.exists(bucketName, dir3));

			// second round
			s3.createDirectory(bucketName, dir3, null, null);
			List<S3Info> recPseu = s3.list(bucketName, null, true, true, false);
			assertEquals(4, recPseu.size());
			contains(recPseu, bucketName, dir1);
			contains(recPseu, bucketName, dir2);
			contains(recPseu, bucketName, dir3);

			List<S3Info> rec = s3.list(bucketName, null, true, false, false);
			assertEquals(2, rec.size());
			contains(rec, bucketName, dir3);

			List<S3Info> pseu = s3.list(bucketName, null, false, true, false);
			assertEquals(2, pseu.size());
			contains(pseu, bucketName, dir1);

			assertTrue(s3.exists(bucketName));
			assertTrue(s3.exists(bucketName, dir1));
			assertTrue(s3.exists(bucketName, dir2));
			assertTrue(s3.exists(bucketName, dir3));

			s3.delete(bucketName, dir3, false);

			assertTrue(s3.exists(bucketName));
			assertTrue(s3.exists(bucketName, dir1));
			assertTrue(s3.exists(bucketName, dir2));
			assertFalse(s3.exists(bucketName, dir3));

			s3.delete(bucketName, dir2, false);

			assertTrue(s3.exists(bucketName));
			assertTrue(s3.exists(bucketName, dir1));
			assertFalse(s3.exists(bucketName, dir2));
			assertFalse(s3.exists(bucketName, dir3));

			s3.delete(bucketName, dir1, false);

			assertTrue(s3.exists(bucketName));
			assertFalse(s3.exists(bucketName, dir1));
			assertFalse(s3.exists(bucketName, dir2));
			assertFalse(s3.exists(bucketName, dir3));

			s3.delete(bucketName, false);

			assertFalse(s3.exists(bucketName));
			assertFalse(s3.exists(bucketName, dir1));
			assertFalse(s3.exists(bucketName, dir2));
			assertFalse(s3.exists(bucketName, dir3));

			// third round
			s3.createDirectory(bucketName, dir3, null, null);

			assertTrue(s3.exists(bucketName));
			assertTrue(s3.exists(bucketName, dir1));
			assertTrue(s3.exists(bucketName, dir2));
			assertTrue(s3.exists(bucketName, dir3));

			s3.delete(bucketName, dir1, true);

			assertTrue(s3.exists(bucketName));
			assertFalse(s3.exists(bucketName, dir1));
			assertFalse(s3.exists(bucketName, dir2));
			assertFalse(s3.exists(bucketName, dir3));

			s3.delete(bucketName, false);

			// create File
			s3.createFile(bucketName, file1, null, null);
			assertTrue(s3.exists(bucketName, dir1));
			assertTrue(s3.exists(bucketName, dir2));
			assertTrue(s3.exists(bucketName, dir3));
			assertTrue(s3.exists(bucketName, file1));
			assertFalse(s3.isDirectory(bucketName, file1));
			assertTrue(s3.isFile(bucketName, file1));

			s3.delete(bucketName, file1, false);
			assertTrue(s3.exists(bucketName, dir1));
			assertTrue(s3.exists(bucketName, dir2));
			assertTrue(s3.exists(bucketName, dir3));
			assertFalse(s3.exists(bucketName, file1));
			assertFalse(s3.isDirectory(bucketName, file1));
			assertFalse(s3.isFile(bucketName, file1));

			s3.write(bucketName, file1, "Susi", "test/plain", "UTF-8", null, null);
			S3Object obj = s3.getData(bucketName, file1);
			InputStream is = obj.getDataInputStream();
			try {
				assertEquals("Susi", Util.toString(is));
			}
			finally {
				is.close();
			}
			s3.delete(bucketName, true);

			// Copy
			s3.write(bucketName, file1, "Susi", "test/plain", "UTF-8", null, null);
			s3.copy(bucketName, file1, bucketName, file2);
			List<S3Info> list = s3.list(bucketName, dir1, true, true, true);
			assertEquals(4, list.size());
			contains(list, bucketName, dir2);
			contains(list, bucketName, dir3);
			contains(list, bucketName, file1);
			contains(list, bucketName, file2);
			s3.delete(bucketName, true);

			// Move
			s3.write(bucketName, file1, "Susi", "test/plain", "UTF-8", null, null);
			s3.move(bucketName, file1, bucketName, file2);
			list = s3.list(bucketName, dir1, true, true, true);

			assertEquals(3, list.size());
			contains(list, bucketName, dir2);
			contains(list, bucketName, dir3);
			contains(list, bucketName, file2);

			// S3
			bucketName = "testcaseS3";

			if (s3.isDirectory(bucketName, null)) s3.delete(bucketName, true);

			assertFalse(s3.exists(bucketName));
			assertFalse(s3.exists(bucketName, null));
			assertFalse(s3.isDirectory(bucketName, null));
			assertFalse(s3.isFile(bucketName, null));
			s3.createDirectory(bucketName, null, null, null);
			assertTrue(s3.exists(bucketName));
			assertTrue(s3.exists(bucketName, null));
			assertFalse(s3.isFile(bucketName, null));
			assertTrue(s3.isDirectory(bucketName, null));

			// we accept this because S3 accept this, so if ACF does not, that is a bug/limitation in ACF.
			String sub = "a";
			if (!s3.isFile(bucketName, sub)) ;
			s3.write(bucketName, sub, "", null, null, null, null);
			assertTrue(s3.exists(bucketName, sub));
			assertTrue(s3.isDirectory(bucketName, sub));
			assertFalse(s3.isFile(bucketName, sub));

			// because previous file is empty it is accepted as directory
			String subsub = sub + "/foo.txt";
			if (!s3.isFile(bucketName, subsub)) s3.write(bucketName, subsub, "hello text", null, null, null, null);

			assertFalse(s3.isDirectory(bucketName, subsub));
			assertTrue(s3.isFile(bucketName, subsub));
			assertTrue(s3.exists(bucketName, sub));
			assertTrue(s3.isDirectory(bucketName, sub));
			assertFalse(s3.isFile(bucketName, sub));

			list = s3.list(bucketName, sub, true, true, true);
			assertEquals(1, list.size());

			s3.delete(bucketName, true);
		}
		finally {
			// print.e("exe-time:"+(System.currentTimeMillis()-start));

		}
	}

	private static void contains(List<S3Info> buckets, String bucketName, String objectName) {
		Iterator<S3Info> it = buckets.iterator();
		while (it.hasNext()) {
			S3Info i = it.next();
			if ((bucketName == null || bucketName.equals(i.getBucketName())) && (objectName == null || objectName.equals(i.getObjectName()))) return;

		}
		throw new RuntimeException("does not contain: " + bucketName);
	}

	private static void assertFalse(boolean b) {
		if (b) throw new RuntimeException("assertFalse:true");
	}

	private static void assertTrue(boolean b) {
		if (!b) throw new RuntimeException("assertTrue:false");
	}

	private static void assertEquals(Object l, Object r) {
		if (!l.equals(r)) throw new RuntimeException("assertEquals(" + l + "," + r + ")");
	}

}
