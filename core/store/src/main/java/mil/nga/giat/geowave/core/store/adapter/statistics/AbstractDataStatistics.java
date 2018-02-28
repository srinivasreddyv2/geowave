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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

abstract public class AbstractDataStatistics<T> implements
		DataStatistics<T>
{
	protected static final ByteArrayId STATS_SEPARATOR = new ByteArrayId(
			"_");
	protected static final String STATS_ID_SEPARATOR = "#";

	/**
	 * ID of source data adapter
	 */
	protected short internalDataAdapterId;
	protected byte[] visibility;
	/**
	 * ID of statistic to be tracked
	 */
	protected ByteArrayId statisticsId;

	public void setStatisticsId(
			ByteArrayId statisticsId ) {
		this.statisticsId = statisticsId;
	}

	protected AbstractDataStatistics() {}

	public AbstractDataStatistics(
			final ByteArrayId statisticsId ) {
		this.statisticsId = statisticsId;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	public short getInternalDataAdapterId() {
		return internalDataAdapterId;
	}

	public void setInternalDataAdapterId(
			short internalDataAdapterId ) {
		this.internalDataAdapterId = internalDataAdapterId;
	}

	@Override
	public void setVisibility(
			final byte[] visibility ) {
		this.visibility = visibility;
	}

	@Override
	public ByteArrayId getStatisticsId() {
		return statisticsId;
	}

	protected static ByteArrayId composeId(
			final String statsType,
			final String name ) {
		return new ByteArrayId(
				statsType + STATS_ID_SEPARATOR + name);
	}

	protected static String decomposeNameFromId(
			final ByteArrayId id ) {
		final String idString = id.getString();
		final int pos = idString.lastIndexOf('#');
		return idString.substring(pos + 1);
	}

	@SuppressWarnings("unchecked")
	public DataStatistics<T> duplicate() {
		DataStatistics<T> newStats;
		try {
			newStats = this.getClass().newInstance();
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(
					"Cannot duplicate statistics class " + this.getClass(),
					e);
		}

		newStats.fromBinary(toBinary());
		return newStats;
	}

	public JSONObject toJSONObject(
			InternalAdapterStore store )
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				"AbstractDataStatistics");
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"statisticsID",
				statisticsId.getString());
		return jo;
	}

	@Override
	public String toString() {
		return "AbstractDataStatistics [internalDataAdapterId=" + internalDataAdapterId + ", statisticsId="
				+ statisticsId.getString() + "]";
	}
}
