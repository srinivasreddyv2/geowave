package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class BaseQueryOptions
{

	private static ScanCallback<Object, GeoWaveRow> DEFAULT_CALLBACK = new ScanCallback<Object, GeoWaveRow>() {
		@Override
		public void entryScanned(
				final Object entry,
				final GeoWaveRow row ) {}
	};

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {
		"SE_TRANSIENT_FIELD_NOT_RESTORED"
	})
	private List<InternalDataAdapter<?>> adapters = null;
	private List<Short> adapterIds = null;
	private ByteArrayId indexId = null;
	private transient PrimaryIndex index = null;
	private Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregationAdapterPair;
	private Integer limit = -1;
	private double[] maxResolutionSubsamplingPerDimension = null;
	private transient ScanCallback<?, ?> scanCallback = DEFAULT_CALLBACK;
	private String[] authorizations = new String[0];
	private Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair;

	public BaseQueryOptions(
			QueryOptions options,
			InternalAdapterStore internalAdapterStore ) {
		super();
		this.indexId = options.getIndexId();
		this.index = options.getIndex();
		this.limit = options.getLimit();
		this.maxResolutionSubsamplingPerDimension = options.getMaxResolutionSubsamplingPerDimension();
		this.authorizations = options.getAuthorizations();

		if (options.getAggregation() != null) {
			DataAdapter<?> adapter = options.getAggregation().getLeft();
			short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
			this.aggregationAdapterPair = new ImmutablePair<InternalDataAdapter<?>, Aggregation<?, ?, ?>>(
					new InternalDataAdapterWrapper(
							(WritableDataAdapter) adapter,
							internalAdapterId),
					options.getAggregation().getRight());
		}

		if (options.getFieldIdsAdapterPair() != null) {
			DataAdapter<?> adapter = options.getFieldIdsAdapterPair().getRight();
			short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
			this.fieldIdsAdapterPair = new ImmutablePair<List<String>, InternalDataAdapter<?>>(
					options.getFieldIdsAdapterPair().getLeft(),
					new InternalDataAdapterWrapper(
							(WritableDataAdapter) adapter,
							internalAdapterId));
		}

		if (options.getAdapterIds() != null) {
			this.adapterIds = Lists.transform(
					options.getAdapterIds(),
					new Function<ByteArrayId, Short>() {
						@Override
						public Short apply(
								ByteArrayId input ) {
							return internalAdapterStore.getInternalAdapterId(input);
						}
					});
		}
		if (options.getAdapters() != null) {
			this.adapters = Lists.transform(
					options.getAdapters(),
					new Function<DataAdapter<?>, InternalDataAdapter<?>>() {
						@Override
						public InternalDataAdapter<?> apply(
								DataAdapter<?> adapter ) {
							short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
							return new InternalDataAdapterWrapper(
									(WritableDataAdapter) adapter,
									internalAdapterId);
						}
					});
		}
	}

	/**
	 * Return the set of adapter/index associations. If the adapters are not
	 * provided, then look up all of them. If the index is not provided, then
	 * look up all of them.
	 *
	 * DataStores are responsible for selecting a single adapter/index per
	 * query. For deletions, the Data Stores are interested in all the
	 * associations.
	 *
	 * @param adapterStore
	 * @param
	 * @param indexStore
	 * @return
	 * @throws IOException
	 */

	public List<Pair<PrimaryIndex, List<InternalDataAdapter<?>>>> getIndicesForAdapters(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		return BaseDataStoreUtils.combineByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	public CloseableIterator<InternalDataAdapter<?>> getAdapters(
			final PersistentAdapterStore adapterStore ) {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<InternalDataAdapter<?>>();
				for (final short id : adapterIds) {
					final InternalDataAdapter adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
			return new CloseableIterator.Wrapper(
					adapters.iterator());
		}
		return adapterStore.getAdapters();
	}

	public InternalDataAdapter<?>[] getAdaptersArray(
			final PersistentAdapterStore adapterStore )
			throws IOException {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<InternalDataAdapter<?>>();
				for (final short id : adapterIds) {
					final InternalDataAdapter<?> adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
			return adapters.toArray(new InternalDataAdapter[adapters.size()]);
		}
		final List<InternalDataAdapter> list = new ArrayList<InternalDataAdapter>();
		if (adapterStore != null && adapterStore.getAdapters() != null) {
			try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext()) {
					list.add(it.next());
				}
			}
		}
		return list.toArray(new InternalDataAdapter[list.size()]);
	}

	public void setInternalAdapterId(
			final short internalAdapterId ) {
		adapterIds = Arrays.asList(internalAdapterId);
	}

	public List<Short> getAdapterIds() {
		return adapterIds;
	}

	/*
	 * public List<Short> getAdapterIds( final PersistentAdapterStore
	 * adapterStore ) throws IOException { final List<Short> ids = new
	 * ArrayList<Short>(); if ((adapterIds == null) || adapterIds.isEmpty()) {
	 * try (CloseableIterator<InternalDataAdapter<?>> it =
	 * getAdapters(adapterStore)) { while (it.hasNext()) {
	 * ids.add((it.next()).getInternalAdapterId()); } } } else {
	 * ids.addAll(adapterIds); } return ids; }
	 */

	private List<Pair<PrimaryIndex, InternalDataAdapter<?>>> compileIndicesForAdapters(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		if ((adapterIds != null) && !adapterIds.isEmpty()) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<InternalDataAdapter<?>>();
				for (final Short id : adapterIds) {
					final InternalDataAdapter<?> adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
		}
		else {
			adapters = new ArrayList<InternalDataAdapter<?>>();
			try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext()) {
					adapters.add(it.next());
				}
			}
		}
		final List<Pair<PrimaryIndex, InternalDataAdapter<?>>> result = new ArrayList<Pair<PrimaryIndex, InternalDataAdapter<?>>>();
		for (final InternalDataAdapter<?> adapter : adapters) {
			final AdapterToIndexMapping indices = adapterIndexMappingStore.getIndicesForAdapter(adapter
					.getInternalAdapterId());
			if (index != null) {
				result.add(Pair.of(
						index,
						adapter));
			}
			else if ((indexId != null) && indices.contains(indexId)) {
				if (index == null) {
					index = (PrimaryIndex) indexStore.getIndex(indexId);
					result.add(Pair.of(
							index,
							adapter));
				}
			}
			else if (indices.isNotEmpty()) {
				for (final ByteArrayId id : indices.getIndexIds()) {
					final PrimaryIndex pIndex = (PrimaryIndex) indexStore.getIndex(id);
					// this could happen if persistent was turned off
					if (pIndex != null) {
						result.add(Pair.of(
								pIndex,
								adapter));
					}
				}
			}
		}
		return result;
	}

	public ScanCallback<?, ?> getScanCallback() {
		return scanCallback == null ? DEFAULT_CALLBACK : scanCallback;
	}

	/**
	 * @param scanCallback
	 *            a function called for each item discovered per the query
	 *            constraints
	 */
	public void setScanCallback(
			final ScanCallback<?, ?> scanCallback ) {
		this.scanCallback = scanCallback;
	}

	/**
	 *
	 * @return Limit the number of data items to return
	 */
	public Integer getLimit() {
		return limit;
	}

	/**
	 * a value <= 0 or null indicates no limits
	 *
	 * @param limit
	 */
	public void setLimit(
			Integer limit ) {
		if ((limit == null) || (limit == 0)) {
			limit = -1;
		}
		this.limit = limit;
	}

	/**
	 *
	 * @return authorizations to apply to the query in addition to the
	 *         authorizations assigned to the data store as a whole.
	 */
	public String[] getAuthorizations() {
		return authorizations == null ? new String[0] : authorizations;
	}

	public void setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
	}

	public void setMaxResolutionSubsamplingPerDimension(
			final double[] maxResolutionSubsamplingPerDimension ) {
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregationAdapterPair;
	}

	public void setAggregation(
			final Aggregation<?, ?, ?> aggregation,
			final InternalDataAdapter<?> adapter ) {
		aggregationAdapterPair = new ImmutablePair<InternalDataAdapter<?>, Aggregation<?, ?, ?>>(
				adapter,
				aggregation);
	}

	/**
	 * Return a set list adapter/index associations. If the adapters are not
	 * provided, then look up all of them. If the index is not provided, then
	 * look up all of them. The full set of adapter/index associations is
	 * reduced so that a single index is queried per adapter and the number
	 * indices queried is minimized.
	 *
	 * DataStores are responsible for selecting a single adapter/index per
	 * query. For deletions, the Data Stores are interested in all the
	 * associations.
	 *
	 * @param adapterStore
	 * @param adapterIndexMappingStore
	 * @param indexStore
	 * @return
	 * @throws IOException
	 */
	public List<Pair<PrimaryIndex, List<InternalDataAdapter<?>>>> getAdaptersWithMinimalSetOfIndices(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		// TODO this probably doesn't have to use PrimaryIndex and should be
		// sufficient to use index IDs
		return BaseDataStoreUtils.reduceIndicesAndGroupByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	/**
	 *
	 * @return a paring of fieldIds and their associated data adapter >>>>>>>
	 *         wip: bitmask approach
	 */
	public Pair<List<String>, InternalDataAdapter<?>> getFieldIdsAdapterPair() {
		return fieldIdsAdapterPair;
	}

}
