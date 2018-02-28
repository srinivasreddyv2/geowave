/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.test.basic;

import java.io.File;
import java.net.URL;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasLocalIT 
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasLocalIT.class);
	protected static final String GDELT_INPUT_FILE = "/Users/svarala/Downloads/gdelt";

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.CASSANDRA,
		GeoWaveStoreType.DYNAMODB,
		GeoWaveStoreType.HBASE
	})
	protected static DataStorePluginOptions dataStore = new DataStorePluginOptions();
//	static {
//
//		dataStore.setFactoryFamily(new MemoryStoreFactoryFamily());
//		dataStore.setFactoryOptions(new MemoryStoreFactoryFamily().getDataStoreFactory().createOptionsInstance());
//	}

	private static long startMillis;
	private static final int NUM_THREADS = 1;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveBasLocalIT  *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveBasLocalIT  *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void tessingleThreadedIngest() {
		testIngestAndQuery(NUM_THREADS);
	}

	public void testIngestAndQuery(
			final int nthreads ) {
		long mark = System.currentTimeMillis();

		LOGGER.debug("Testing DataStore Type: " + dataStore.getType());
 try {
		// Ensure empty datastore
		TestUtils.deleteAll(dataStore);

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL,
				GDELT_INPUT_FILE,
				"gdelt",
				nthreads);

		long dur = (System.currentTimeMillis() - mark);
		LOGGER.debug("Ingest (points) duration = " + dur + " ms with " + nthreads + " thread(s).");

		CloseableIterator<Object> obj = dataStore.createDataStore().query(new QueryOptions(), new EverythingQuery());
		int i =0;
		while(obj.hasNext()){
			obj.next();
			i++;
		}
		
		System.err.println("there are '" +i+ "' records");
		
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error: '"
					+ e.getLocalizedMessage() + "'");
		}

	

		TestUtils.deleteAll(dataStore);
	}

	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
