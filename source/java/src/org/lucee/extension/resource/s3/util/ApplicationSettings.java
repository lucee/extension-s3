package org.lucee.extension.resource.s3.util;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.jets3t.service.acl.AccessControlList;
import org.lucee.extension.resource.s3.S3;
import org.lucee.extension.resource.s3.S3Properties;

import lucee.commons.io.res.Resource;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.PageContext;
import lucee.runtime.config.Config;
import lucee.runtime.exp.PageException;
import lucee.runtime.ext.function.BIF;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import lucee.runtime.type.dt.TimeSpan;
import lucee.runtime.util.Creation;

public class ApplicationSettings {

	private static final Key _S3;
	private static final Key _VFS;
	private static final Key HOST;
	private static final Key SERVER;
	public static final Key ACCESS_KEY_ID;
	public static final Key AWS_ACCESS_KEY_ID;
	public static final Key AWS_SECRET_KEY;
	public static final Key SECRET_KEY;
	public static final Key DEFAULT_LOCATION;
	public static final Key ACL;
	public static final Key CACHE;

	private static boolean init = false;
	private static BIF bif;

	static {
		Creation cre = CFMLEngineFactory.getInstance().getCreationUtil();
		_S3 = cre.createKey("s3");
		_VFS = cre.createKey("vfs");
		HOST = cre.createKey("host");
		SERVER = cre.createKey("server");
		AWS_ACCESS_KEY_ID = cre.createKey("awsAccessKeyId");
		ACCESS_KEY_ID = cre.createKey("accessKeyId");
		AWS_SECRET_KEY = cre.createKey("awsSecretKey");
		SECRET_KEY = cre.createKey("secretKey");
		DEFAULT_LOCATION = cre.createKey("defaultLocation");
		ACL = cre.createKey("acl");
		CACHE = cre.createKey("cache");
	}
	private static Map<String, S3PropertiesCollection> propsColl = new WeakHashMap<String, S3PropertiesCollection>();

	public ApplicationSettings() {

	}

	public static S3PropertiesCollection readS3PropertiesCollection(PageContext pc) throws PageException {
		String key = pc.getId() + ":" + pc.getStartTime();

		S3PropertiesCollection existing = propsColl.get(key);
		if (existing != null) {
			return existing;
		}

		S3PropertiesCollection coll = null;
		synchronized (S3.getToken(key)) {

			existing = propsColl.get(key);
			if (existing != null) {
				return existing;
			}

			CFMLEngine eng = CFMLEngineFactory.getInstance();
			if (bif == null || !bif.getClass().getClassLoader().equals(pc.getClass().getClassLoader())) {
				try {
					bif = eng.getClassUtil().loadBIF(pc, "lucee.runtime.functions.system.GetApplicationSettings");
				}
				catch (Exception e) {
					throw eng.getCastUtil().toPageException(e);
				}
			}
			final Struct sct = (Struct) bif.invoke(pc, new Object[] { Boolean.TRUE });
			Struct data;
			{
				// read default s3 properties
				data = eng.getCastUtil().toStruct(sct.get(_S3, null), null);
				if (data == null || data.isEmpty()) {
					Struct tmp = eng.getCastUtil().toStruct(sct.get(_VFS, null), null);
					data = (tmp == null || tmp.isEmpty()) ? null : eng.getCastUtil().toStruct(tmp.get(_S3, null), null);
				}

				if (data != null && !data.isEmpty()) {
					S3Properties s3prop = toS3Properties(eng, data, null);

					if (s3prop != null) {
						coll = new S3PropertiesCollection();
						coll.setDefault(s3prop);
						propsColl.put(key, coll);
					}
				}
			}

			// read all mapped properties
			{
				Object o = sct.get(_VFS, null);
				if (o instanceof Struct) o = ((Struct) o).get(_S3, null);
				if (o instanceof Struct && !(data = (Struct) o).isEmpty()) {
					// multiple configs
					Struct sub;
					Iterator<Entry<Key, Object>> it = data.entryIterator();
					Entry<Key, Object> e;
					while (it.hasNext()) {
						e = it.next();
						if (!(e.getValue() instanceof Struct)) continue;

						sub = (Struct) e.getValue();
						S3Properties s3prop = toS3Properties(eng, sub, null);
						if (s3prop != null) {
							if (coll == null) {
								coll = new S3PropertiesCollection();
								propsColl.put(key, coll);
							}
							coll.setMapping(e.getKey(), s3prop);
						}
					}
				}
			}
		}
		return coll;
	}

	private static S3Properties toS3Properties(CFMLEngine eng, Struct sct, S3Properties defaultValue) throws PageException {
		S3Properties props = null;

		// accesskey_id
		String str = eng.getCastUtil().toString(sct.get(AWS_ACCESS_KEY_ID, null), null);
		if (eng.getStringUtil().isEmpty(str)) str = eng.getCastUtil().toString(sct.get(ACCESS_KEY_ID, null), null);
		if (!Util.isEmpty(str)) {
			if (props == null) props = new S3Properties();
			props.setAccessKeyId(str);
		}

		// secret key
		str = eng.getCastUtil().toString(sct.get(AWS_SECRET_KEY, null), null);
		if (eng.getStringUtil().isEmpty(str)) str = eng.getCastUtil().toString(sct.get(SECRET_KEY, null), null);
		if (!Util.isEmpty(str)) {
			if (props == null) props = new S3Properties();
			props.setSecretAccessKey(str);
		}

		// host
		str = eng.getCastUtil().toString(sct.get(HOST, null), null);
		if (eng.getStringUtil().isEmpty(str)) str = eng.getCastUtil().toString(sct.get(SERVER, null), null);
		if (!Util.isEmpty(str)) {
			if (props == null) props = new S3Properties();
			props.setHost(str);
		}

		// location
		str = eng.getCastUtil().toString(sct.get(DEFAULT_LOCATION, null), null);
		if (!Util.isEmpty(str)) {
			if (props == null) props = new S3Properties();
			props.setLocation(str);
		}

		// ACL
		AccessControlList acl = S3.toACL(eng.getCastUtil().toString(sct.get(ACL, null), null), null);
		if (acl != null) {
			if (props == null) props = new S3Properties();
			props.setACL(acl);
		}

		// Cache
		TimeSpan cache = eng.getCastUtil().toTimespan(sct.get(CACHE, null), null);
		if (cache != null) {
			if (props == null) props = new S3Properties();
			props.setCache(cache);
		}

		return props;
	}

	public static File getDefaultFontDirectory(Config config) {
		Resource fonts = config.getConfigDir().getRealResource("fonts");
		fonts.mkdirs();
		if (fonts.isDirectory() && fonts instanceof File) return (File) fonts;
		return null;
	}

	public static class S3PropertiesCollection {

		private S3Properties defaultProp;
		private Map<Key, S3Properties> coll = new ConcurrentHashMap<>();

		public void setDefault(S3Properties defaultProp) {
			this.defaultProp = defaultProp;
		}

		public S3Properties getDefault() {
			return defaultProp;
		}

		public void setMapping(Key key, S3Properties s3Properties) {
			coll.put(key, s3Properties);
		}

		public S3Properties getMapping(Key key) {
			return coll.get(key);
		}

		public Set<Entry<Key, S3Properties>> getMappings() {
			return coll.entrySet();
		}

		public S3Properties getMapping(String key) {
			return getMapping(CFMLEngineFactory.getInstance().getCreationUtil().createKey((key + "").trim()));
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			if (defaultProp != null) {
				sb.append("default:").append(defaultProp.getAccessKeyId()).append(';');
			}

			Iterator<Entry<Key, S3Properties>> it = coll.entrySet().iterator();
			Entry<Key, S3Properties> e;
			while (it.hasNext()) {
				e = it.next();
				sb.append(e.getKey()).append(":").append(e.getValue().getAccessKeyId()).append(';');
			}

			return sb.toString();
		}
	}

}
