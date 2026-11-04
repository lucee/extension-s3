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
 * Per region bucket name suffix and prefix, used for disaster recovery setups where a bucket is
 * replicated into a bucket with a different name in another region (for example "mybucket" in
 * us-east-1 and "mybucket-dr" in eu-west-1), or where the buckets of a region carry a prefix (for
 * example "susi" is "livestock-susi" in us-west-2).
 *
 * The code and the paths (s3://mybucket/file.txt) always use the logical bucket name, the prefix and
 * the suffix of the effective region are added just before the request is sent to S3 (see
 * {@link #toRemote(String, String)}), the remote name is prefix + logical name + suffix. Without a
 * configured prefix or suffix for the region of a call the bucket name is not touched.
 *
 * The prefixes and the suffixes have an own list of buckets that are excluded (the opt-out for
 * buckets that are not replicated / not prefixed).
 *
 * Instances are immutable.
 */
public final class S3BucketSuffix {

	public static final S3BucketSuffix EMPTY = new S3BucketSuffix(Collections.<String, String>emptyMap(), Collections.<String>emptySet(), Collections.<String, String>emptyMap(),
			Collections.<String>emptySet(), null);

	private static final Pattern VALID_SUFFIX = Pattern.compile("[a-z0-9.\\-]+");
	// a bucket name starts with a letter or a digit
	private static final Pattern VALID_PREFIX = Pattern.compile("[a-z0-9][a-z0-9.\\-]*");

	private static final String EXAMPLE_SUFFIXES = "{\"eu-west-1\":\"-dr\"}";
	private static final String EXAMPLE_PREFIXES = "{\"us-west-2\":\"livestock-\"}";

	/** region (lower case) to suffix, never contains empty suffixes */
	private final Map<String, String> suffixes;
	/** logical bucket names (or "prefix*" patterns) that never get a suffix */
	private final Set<String> exclude;
	/** region (lower case) to prefix, never contains empty prefixes */
	private final Map<String, String> prefixes;
	/** logical bucket names (or "prefix*" patterns) that never get a prefix */
	private final Set<String> prefixExclude;
	/** the region used for calls that do not name a region themselves, can be null */
	private final String region;

	private S3BucketSuffix(Map<String, String> suffixes, Set<String> exclude, Map<String, String> prefixes, Set<String> prefixExclude, String region) {
		this.suffixes = suffixes;
		this.exclude = exclude;
		this.prefixes = prefixes;
		this.prefixExclude = prefixExclude;
		this.region = region;
	}

	/**
	 * @param suffixes region to suffix, null or empty for no suffixes
	 * @param exclude names of logical buckets that never get a suffix (a trailing * matches a prefix),
	 *            can be null
	 * @param region the region used when the call itself does not name a region, can be null
	 */
	public static S3BucketSuffix create(Map<String, String> suffixes, Set<String> exclude, String region) throws S3Exception {
		return create(null, null, suffixes, exclude, region);
	}

	/**
	 * @param prefixes region to prefix, null or empty for no prefixes
	 * @param prefixExclude names of logical buckets that never get a prefix (a trailing * matches a
	 *            prefix), can be null
	 * @param suffixes region to suffix, null or empty for no suffixes
	 * @param exclude names of logical buckets that never get a suffix (a trailing * matches a prefix),
	 *            can be null
	 * @param region the region used when the call itself does not name a region, can be null
	 */
	public static S3BucketSuffix create(Map<String, String> prefixes, Set<String> prefixExclude, Map<String, String> suffixes, Set<String> exclude, String region) throws S3Exception {
		Map<String, String> pfx = toRegionMap(prefixes, true);
		Map<String, String> sfx = toRegionMap(suffixes, false);
		if (pfx.isEmpty() && sfx.isEmpty()) return EMPTY;
		return new S3BucketSuffix(Collections.unmodifiableMap(sfx), toNameSet(exclude), Collections.unmodifiableMap(pfx), toNameSet(prefixExclude), normalizeRegion(region));
	}

	private static Map<String, String> toRegionMap(Map<String, String> in, boolean prefix) throws S3Exception {
		Map<String, String> result = new LinkedHashMap<String, String>();
		if (in == null) return result;
		String kind = prefix ? "prefix" : "suffix";
		for (Entry<String, String> e: in.entrySet()) {
			String r = normalizeRegion(e.getKey());
			if (r == null) throw new S3Exception("bucket " + kind + " configuration has an entry with an empty region");
			String v = e.getValue() == null ? "" : e.getValue().trim();
			if (prefix) validatePrefix(r, v);
			else validateSuffix(r, v);
			if (v.isEmpty()) continue; // an empty value means "none for this region"
			result.put(r, v);
		}
		return result;
	}

	private static Set<String> toNameSet(Set<String> names) {
		Set<String> result = new LinkedHashSet<String>();
		if (names != null) {
			for (String n: names) {
				if (!Util.isEmpty(n, true)) result.add(n.trim());
			}
		}
		return Collections.unmodifiableSet(result);
	}

	/**
	 * the same configuration, but with another default region
	 */
	public S3BucketSuffix withRegion(String region) {
		if (isEmpty()) return this;
		return new S3BucketSuffix(suffixes, exclude, prefixes, prefixExclude, normalizeRegion(region));
	}

	/** true when no region has a prefix or a suffix, in this case nothing is touched */
	public boolean isEmpty() {
		return suffixes.isEmpty() && prefixes.isEmpty();
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

	public Map<String, String> getPrefixes() {
		return prefixes;
	}

	public Set<String> getPrefixExclude() {
		return prefixExclude;
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

	/**
	 * the prefix for the given region
	 *
	 * @return the prefix or null if there is none
	 */
	public String getPrefix(String region) {
		if (prefixes.isEmpty()) return null;
		String r = normalizeRegion(region);
		if (r == null) return null;
		return prefixes.get(r);
	}

	/** true if the logical bucket never gets a suffix */
	public boolean isExcluded(String logicalBucket) {
		return matches(exclude, logicalBucket);
	}

	/** true if the logical bucket never gets a prefix */
	public boolean isExcludedFromPrefix(String logicalBucket) {
		return matches(prefixExclude, logicalBucket);
	}

	private static boolean matches(Set<String> names, String logicalBucket) {
		if (logicalBucket == null) return false;
		for (String ex: names) {
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
	 * @return prefix + the logical bucket + suffix of the effective region, or the logical bucket as is,
	 *         when the region has neither a prefix nor a suffix or the bucket is excluded
	 */
	public String toRemote(String logicalBucket, String explicitRegion) {
		if (isEmpty() || Util.isEmpty(logicalBucket)) return logicalBucket;
		String r = Util.isEmpty(explicitRegion, true) ? region : explicitRegion;
		String prefix = effectivePrefix(r, logicalBucket);
		String suffix = effectiveSuffix(r, logicalBucket);
		if (prefix == null && suffix == null) return logicalBucket;
		return (prefix == null ? "" : prefix) + logicalBucket + (suffix == null ? "" : suffix);
	}

	public String toRemote(String logicalBucket) {
		return toRemote(logicalBucket, null);
	}

	/** the prefix that applies to the bucket, null if there is none */
	private String effectivePrefix(String region, String logicalBucket) {
		String prefix = getPrefix(region);
		if (prefix == null || isExcludedFromPrefix(logicalBucket)) return null;
		return prefix;
	}

	/** the suffix that applies to the bucket, null if there is none */
	private String effectiveSuffix(String region, String logicalBucket) {
		String suffix = getSuffix(region);
		if (suffix == null || isExcluded(logicalBucket)) return null;
		return suffix;
	}

	/**
	 * the reverse of toRemote, used for bucket names that come from S3 (for example the bucket
	 * listing)
	 *
	 * @return the logical bucket name or null if the bucket is not visible for this configuration,
	 *         because it is not a bucket in the prefixed / suffixed region set
	 */
	public String toLogical(String remoteBucket) {
		if (isEmpty() || Util.isEmpty(remoteBucket)) return remoteBucket;
		String prefix = getPrefix(region);
		String suffix = getSuffix(region);
		if (prefix == null && suffix == null) return remoteBucket;

		// the excluded buckets are defined with the logical name, so every possible logical name (the
		// name as is, without the prefix and/or the suffix) is only accepted when it leads back to
		// the bucket name on S3
		String[] candidates = new String[4];
		candidates[0] = remoteBucket;
		if (prefix != null && suffix != null) candidates[1] = strip(remoteBucket, prefix, suffix);
		if (prefix != null) candidates[2] = strip(remoteBucket, prefix, null);
		if (suffix != null) candidates[3] = strip(remoteBucket, null, suffix);
		for (String candidate: candidates) {
			if (candidate != null && remoteBucket.equals(toRemote(candidate, null))) return candidate;
		}
		return null;
	}

	/**
	 * @return the name without the prefix and the suffix or null if the name does not have them or
	 *         nothing is left
	 */
	private static String strip(String name, String prefix, String suffix) {
		if (prefix != null) {
			if (!name.startsWith(prefix)) return null;
			name = name.substring(prefix.length());
		}
		if (suffix != null) {
			if (!name.endsWith(suffix)) return null;
			name = name.substring(0, name.length() - suffix.length());
		}
		return name.isEmpty() ? null : name;
	}

	/**
	 * a string that differs for every configuration that has a different effect, empty when there is
	 * nothing configured
	 */
	public String toCacheKey() {
		if (isEmpty()) return "";
		StringBuilder sb = new StringBuilder();
		if (!suffixes.isEmpty()) sb.append("suffixes:").append(suffixes).append(";exclude:").append(exclude).append(";");
		// the prefix part is only there when a prefix is defined, so the key of a configuration with
		// suffixes only is the same as before the prefixes existed
		if (!prefixes.isEmpty()) sb.append("prefixes:").append(prefixes).append(";prefixExclude:").append(prefixExclude).append(";");
		sb.append("suffixRegion:").append(region);
		return sb.toString();
	}

	@Override
	public String toString() {
		if (isEmpty()) return "bucketSuffixes:none";
		StringBuilder sb = new StringBuilder();
		if (!prefixes.isEmpty()) sb.append("bucketPrefixes:").append(prefixes).append(prefixExclude.isEmpty() ? "" : ";excludePrefix:" + prefixExclude).append(";");
		if (!suffixes.isEmpty()) sb.append("bucketSuffixes:").append(suffixes).append(exclude.isEmpty() ? "" : ";exclude:" + exclude);
		return sb.toString() + (region == null ? "" : ";region:" + region);
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

	private static void validatePrefix(String region, String prefix) throws S3Exception {
		if (prefix.isEmpty()) return;
		if (!VALID_PREFIX.matcher(prefix).matches() || prefix.length() > 60) {
			throw new S3Exception("invalid bucket prefix [" + prefix + "] for region [" + region
					+ "], a prefix can only contain lower case letters, digits, dots and hyphens, has to start with a letter or a digit (like \"livestock-\") and has a maximum length of 60 characters");
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
		return parseRegionMap(json, "suffixes", EXAMPLE_SUFFIXES);
	}

	/**
	 * parses a map region to prefix from a json string like {"us-west-2":"livestock-"}. This is the
	 * format used in the environment variable or system property.
	 *
	 * @param json
	 * @return the map, empty if the string is empty
	 * @throws S3Exception if the string is not a flat json object of strings
	 */
	public static Map<String, String> parsePrefixes(String json) throws S3Exception {
		return parseRegionMap(json, "prefixes", EXAMPLE_PREFIXES);
	}

	private static Map<String, String> parseRegionMap(String json, String kind, String example) throws S3Exception {
		Map<String, String> result = new LinkedHashMap<String, String>();
		if (Util.isEmpty(json, true)) return result;
		Json p = new Json(json.trim(), kind, example);
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
	 * the prefixes from the environment variable LUCEE_S3_BUCKETPREFIXES or the system property
	 * lucee.s3.bucketprefixes
	 *
	 * @return null if not set
	 */
	public static Map<String, String> prefixesFromEnv() throws S3Exception {
		String str = S3Util.getSystemPropOrEnvVar("lucee.s3.bucketprefixes", null);
		if (str == null || Util.isEmpty(str, true)) return null;
		try {
			return parsePrefixes(str);
		}
		catch (S3Exception e) {
			throw new S3Exception("invalid value for the environment variable LUCEE_S3_BUCKETPREFIXES (system property lucee.s3.bucketprefixes): " + e.getMessage());
		}
	}

	/**
	 * the buckets that never get a prefix from the environment variable LUCEE_S3_BUCKETPREFIXEXCLUDE
	 * or the system property lucee.s3.bucketprefixexclude
	 *
	 * @return null if not set
	 */
	public static Set<String> prefixExcludeFromEnv() {
		String str = S3Util.getSystemPropOrEnvVar("lucee.s3.bucketprefixexclude", null);
		if (str == null || Util.isEmpty(str, true)) return null;
		return parseExclude(str);
	}

	/**
	 * a minimal strict parser for a flat json object with string values
	 */
	private static final class Json {
		private final String str;
		private final String kind;
		private final String example;
		private int pos = 0;

		private Json(String str, String kind, String example) {
			this.str = str;
			this.kind = kind;
			this.example = example;
		}

		private S3Exception error(String msg) {
			return new S3Exception("invalid bucket " + kind + " definition, " + msg + " at position " + pos + ", expected a json object like " + example);
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
