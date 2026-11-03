package org.lucee.extension.resource.s3;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import lucee.loader.util.Util;

/**
 * Per region bucket name suffix, used for disaster recovery setups where a bucket is replicated
 * into a bucket with a different name in another region (for example "mybucket" in us-east-1 and
 * "mybucket-dr" in eu-west-1).
 *
 * The code and the paths (s3://mybucket/file.txt) always use the logical bucket name, the suffix
 * of the effective region is appended just before the request is sent to S3 (see
 * {@link #toRemote(String, String)}). Without a configured suffix for the region of a call the
 * bucket name is not touched.
 *
 * Instances are immutable.
 */
public final class S3BucketSuffix {

	public static final S3BucketSuffix EMPTY = new S3BucketSuffix(Collections.<String, String>emptyMap(), Collections.<String>emptySet(), null);

	private static final Pattern VALID_SUFFIX = Pattern.compile("[a-z0-9.\\-]+");

	/** region (lower case) to suffix, never contains empty suffixes */
	private final Map<String, String> suffixes;
	/** logical bucket names (or "prefix*" patterns) that never get a suffix */
	private final Set<String> exclude;
	/** the region used for calls that do not name a region themselves, can be null */
	private final String region;

	private S3BucketSuffix(Map<String, String> suffixes, Set<String> exclude, String region) {
		this.suffixes = suffixes;
		this.exclude = exclude;
		this.region = region;
	}

	/**
	 * @param suffixes region to suffix, null or empty for no suffixes
	 * @param exclude names of logical buckets that never get a suffix (a trailing * matches a prefix),
	 *            can be null
	 * @param region the region used when the call itself does not name a region, can be null
	 */
	public static S3BucketSuffix create(Map<String, String> suffixes, Set<String> exclude, String region) throws S3Exception {
		if (suffixes == null || suffixes.isEmpty()) return EMPTY;
		Map<String, String> sfx = new LinkedHashMap<String, String>();
		for (Entry<String, String> e: suffixes.entrySet()) {
			String r = normalizeRegion(e.getKey());
			if (r == null) throw new S3Exception("bucket suffix configuration has an entry with an empty region");
			String s = e.getValue() == null ? "" : e.getValue().trim();
			validateSuffix(r, s);
			if (s.isEmpty()) continue; // an empty suffix means "no suffix for this region"
			sfx.put(r, s);
		}
		if (sfx.isEmpty()) return EMPTY;
		Set<String> exc = new LinkedHashSet<String>();
		if (exclude != null) {
			for (String ex: exclude) {
				if (!Util.isEmpty(ex, true)) exc.add(ex.trim());
			}
		}
		return new S3BucketSuffix(Collections.unmodifiableMap(sfx), Collections.unmodifiableSet(exc), normalizeRegion(region));
	}

	/**
	 * the same configuration, but with another default region
	 */
	public S3BucketSuffix withRegion(String region) {
		if (isEmpty()) return this;
		return new S3BucketSuffix(suffixes, exclude, normalizeRegion(region));
	}

	/** true when no region has a suffix, in this case nothing is touched */
	public boolean isEmpty() {
		return suffixes.isEmpty();
	}

	public String getRegion() {
		return region;
	}

	public Map<String, String> getSuffixes() {
		return suffixes;
	}

	public Set<String> getExclude() {
		return exclude;
	}

	/**
	 * the suffix for the given region
	 *
	 * @return the suffix or null if there is none
	 */
	public String getSuffix(String region) {
		if (suffixes.isEmpty()) return null;
		String r = normalizeRegion(region);
		if (r == null) return null;
		return suffixes.get(r);
	}

	public boolean isExcluded(String logicalBucket) {
		if (logicalBucket == null) return false;
		for (String ex: exclude) {
			if (ex.endsWith("*")) {
				if (logicalBucket.startsWith(ex.substring(0, ex.length() - 1))) return true;
			}
			else if (ex.equals(logicalBucket)) return true;
		}
		return false;
	}

	/**
	 * the bucket name used in the request to S3
	 *
	 * @param logicalBucket the bucket name used in the code or in the path
	 * @param explicitRegion the region the call explicitly asked for, if null or empty the region of
	 *            this configuration is used
	 * @return the logical bucket plus the suffix of the effective region, or the logical bucket as is,
	 *         when the region has no suffix or the bucket is excluded
	 */
	public String toRemote(String logicalBucket, String explicitRegion) {
		if (suffixes.isEmpty() || Util.isEmpty(logicalBucket)) return logicalBucket;
		String suffix = getSuffix(Util.isEmpty(explicitRegion, true) ? region : explicitRegion);
		if (suffix == null) return logicalBucket;
		if (isExcluded(logicalBucket)) return logicalBucket;
		return logicalBucket + suffix;
	}

	public String toRemote(String logicalBucket) {
		return toRemote(logicalBucket, null);
	}

	/**
	 * the reverse of toRemote, used for bucket names that come from S3 (for example the bucket
	 * listing)
	 *
	 * @return the logical bucket name or null if the bucket is not visible for this configuration,
	 *         because it is not a bucket in the suffixed region set
	 */
	public String toLogical(String remoteBucket) {
		if (suffixes.isEmpty() || Util.isEmpty(remoteBucket)) return remoteBucket;
		String suffix = getSuffix(region);
		if (suffix == null) return remoteBucket;
		if (isExcluded(remoteBucket)) return remoteBucket;
		if (remoteBucket.length() > suffix.length() && remoteBucket.endsWith(suffix)) {
			return remoteBucket.substring(0, remoteBucket.length() - suffix.length());
		}
		return null;
	}

	/**
	 * a string that differs for every configuration that has a different effect, empty when there is
	 * nothing configured
	 */
	public String toCacheKey() {
		if (suffixes.isEmpty()) return "";
		return new StringBuilder().append("suffixes:").append(suffixes).append(";exclude:").append(exclude).append(";suffixRegion:").append(region).toString();
	}

	@Override
	public String toString() {
		if (suffixes.isEmpty()) return "bucketSuffixes:none";
		return "bucketSuffixes:" + suffixes + (exclude.isEmpty() ? "" : ";exclude:" + exclude) + (region == null ? "" : ";region:" + region);
	}

	private static String normalizeRegion(String region) {
		if (Util.isEmpty(region, true)) return null;
		return region.trim().toLowerCase(Locale.ENGLISH);
	}

	private static void validateSuffix(String region, String suffix) throws S3Exception {
		if (suffix.isEmpty()) return;
		if (!VALID_SUFFIX.matcher(suffix).matches() || suffix.length() > 60) {
			throw new S3Exception("invalid bucket suffix [" + suffix + "] for region [" + region
					+ "], a suffix can only contain lower case letters, digits, dots and hyphens (like \"-dr\") and has a maximum length of 60 characters");
		}
	}

	/**
	 * parses a map region to suffix from a json string like {"eu-west-1":"-dr"}. This is the format
	 * used in the environment variable or system property.
	 *
	 * @param json
	 * @return the map, empty if the string is empty
	 * @throws S3Exception if the string is not a flat json object of strings
	 */
	public static Map<String, String> parseSuffixes(String json) throws S3Exception {
		Map<String, String> result = new LinkedHashMap<String, String>();
		if (Util.isEmpty(json, true)) return result;
		Json p = new Json(json.trim());
		p.skipWs();
		p.expect('{');
		p.skipWs();
		if (p.peek() == '}') {
			p.pos++;
		}
		else {
			while (true) {
				p.skipWs();
				String key = p.readString();
				p.skipWs();
				p.expect(':');
				p.skipWs();
				String value = p.readString();
				if (result.containsKey(key)) throw p.error("duplicate region [" + key + "]");
				result.put(key, value);
				p.skipWs();
				char c = p.next();
				if (c == ',') continue;
				if (c == '}') break;
				throw p.error("expected , or }");
			}
		}
		p.skipWs();
		if (p.pos != p.str.length()) throw p.error("unexpected content after the closing }");
		return result;
	}

	/**
	 * parses the names of the buckets that never get a suffix from a comma separated list
	 */
	public static Set<String> parseExclude(String list) {
		Set<String> result = new LinkedHashSet<String>();
		if (Util.isEmpty(list, true)) return result;
		for (String s: list.split(",")) {
			if (!Util.isEmpty(s, true)) result.add(s.trim());
		}
		return result;
	}

	/**
	 * the suffixes from the environment variable LUCEE_S3_BUCKETSUFFIXES or the system property
	 * lucee.s3.bucketsuffixes
	 *
	 * @return null if not set
	 */
	public static Map<String, String> suffixesFromEnv() throws S3Exception {
		String str = S3Util.getSystemPropOrEnvVar("lucee.s3.bucketsuffixes", null);
		if (str == null || Util.isEmpty(str, true)) return null;
		try {
			return parseSuffixes(str);
		}
		catch (S3Exception e) {
			throw new S3Exception("invalid value for the environment variable LUCEE_S3_BUCKETSUFFIXES (system property lucee.s3.bucketsuffixes): " + e.getMessage());
		}
	}

	/**
	 * the buckets that never get a suffix from the environment variable LUCEE_S3_BUCKETSUFFIXEXCLUDE
	 * or the system property lucee.s3.bucketsuffixexclude
	 *
	 * @return null if not set
	 */
	public static Set<String> excludeFromEnv() {
		String str = S3Util.getSystemPropOrEnvVar("lucee.s3.bucketsuffixexclude", null);
		if (str == null || Util.isEmpty(str, true)) return null;
		return parseExclude(str);
	}

	/**
	 * a minimal strict parser for a flat json object with string values
	 */
	private static final class Json {
		private final String str;
		private int pos = 0;

		private Json(String str) {
			this.str = str;
		}

		private S3Exception error(String msg) {
			return new S3Exception("invalid bucket suffixes definition, " + msg + " at position " + pos + ", expected a json object like {\"eu-west-1\":\"-dr\"}");
		}

		private void skipWs() {
			while (pos < str.length() && Character.isWhitespace(str.charAt(pos)))
				pos++;
		}

		private char peek() throws S3Exception {
			if (pos >= str.length()) throw error("unexpected end");
			return str.charAt(pos);
		}

		private char next() throws S3Exception {
			char c = peek();
			pos++;
			return c;
		}

		private void expect(char expected) throws S3Exception {
			char c = peek();
			if (c != expected) throw error("expected " + expected + " but found " + c);
			pos++;
		}

		private String readString() throws S3Exception {
			expect('"');
			StringBuilder sb = new StringBuilder();
			while (true) {
				char c = next();
				if (c == '"') break;
				if (c == '\\') {
					char e = next();
					switch (e) {
					case '"':
					case '\\':
					case '/':
						sb.append(e);
						break;
					case 'u':
						if (pos + 4 > str.length()) throw error("incomplete unicode escape");
						try {
							sb.append((char) Integer.parseInt(str.substring(pos, pos + 4), 16));
						}
						catch (NumberFormatException nfe) {
							throw error("invalid unicode escape");
						}
						pos += 4;
						break;
					default:
						throw error("unsupported escape \\" + e);
					}
				}
				else sb.append(c);
			}
			return sb.toString();
		}
	}
}
