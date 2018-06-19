package mil.nga.giat.geowave.core.store.operations;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

abstract public class BaseReaderParams<IdType, AdapterType extends DataAdapter<?>>
{

	private final PrimaryIndex index;
	private final PersistentAdapterStore adapterStore;
	private final List<IdType> adapterIds;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final Pair<AdapterType, Aggregation<?, ?, ?>> aggregation;
	private final Pair<List<String>, AdapterType> fieldSubsets;
	private final boolean isMixedVisibility;
	private final Integer limit;
	private final String[] additionalAuthorizations;

	public BaseReaderParams(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final List<IdType> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<AdapterType, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, AdapterType> fieldSubsets,
			final boolean isMixedVisibility,
			final Integer limit,
			final String... additionalAuthorizations ) {
		this.index = index;
		this.adapterStore = adapterStore;
		this.adapterIds = adapterIds;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		this.aggregation = aggregation;
		this.fieldSubsets = fieldSubsets;
		this.isMixedVisibility = isMixedVisibility;
		this.limit = limit;
		this.additionalAuthorizations = additionalAuthorizations;
	}

	public PrimaryIndex getIndex() {
		return index;
	}

	public PersistentAdapterStore getAdapterStore() {
		return adapterStore;
	}

	public List<IdType> getAdapterIds() {
		return adapterIds;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<AdapterType, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	public Pair<List<String>, AdapterType> getFieldSubsets() {
		return fieldSubsets;
	}

	public boolean isMixedVisibility() {
		return isMixedVisibility;
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public Integer getLimit() {
		return limit;
	}

	public String[] getAdditionalAuthorizations() {
		return additionalAuthorizations;
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return null;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return null;
	}

	public DistributableQueryFilter getFilter() {
		return null;
	}

	public boolean isServersideAggregation() {
		return false;
	}
}
