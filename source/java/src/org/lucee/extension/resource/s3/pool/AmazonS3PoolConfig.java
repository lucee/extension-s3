package org.lucee.extension.resource.s3.pool;

import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.amazonaws.services.s3.AmazonS3;

public class AmazonS3PoolConfig extends GenericObjectPoolConfig<AmazonS3> {

	private AmazonS3PoolConfig() {

	}

	public static AmazonS3PoolConfig getStandardInstance() {
		AmazonS3PoolConfig config = new AmazonS3PoolConfig();

		// TODO log pool config

		config.setBlockWhenExhausted(BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED);
		config.setFairness(BaseObjectPoolConfig.DEFAULT_FAIRNESS);
		config.setLifo(BaseObjectPoolConfig.DEFAULT_LIFO);
		config.setMaxIdle(4);
		config.setMaxTotal(24);
		config.setMaxWaitMillis(GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS);
		config.setMinEvictableIdleTimeMillis(GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
		config.setTimeBetweenEvictionRunsMillis(GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
		config.setMinIdle(1);
		config.setNumTestsPerEvictionRun(GenericObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
		config.setSoftMinEvictableIdleTimeMillis(GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);

		config.setTestOnCreate(false);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(false);
		config.setTestWhileIdle(true);
		// config.setTestOnBorrow(caster.toBooleanValue(arguments.get("testOnBorrow", null), true));
		// config.setTestOnCreate(caster.toBooleanValue(arguments.get("testOnCreate", null),
		// GenericObjectPoolConfig.DEFAULT_TEST_ON_CREATE));
		// config.setTestOnReturn(caster.toBooleanValue(arguments.get("testOnReturn", null),
		// GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN));
		// config.setTestWhileIdle(caster.toBooleanValue(arguments.get("testWhileIdle", null),
		// GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE));
		return config;
	}

}