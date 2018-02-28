package mil.nga.giat.geowave.core.store.metadata;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.MetadataType;

/**
 * This class will persist Data Adapters within an Accumulo table for GeoWave
 * metadata. The adapters will be persisted in an "ADAPTER" column family.
 *
 * There is an LRU cache associated with it so staying in sync with external
 * updates is not practical - it assumes the objects are not updated often or at
 * all. The objects are stored in their own table.
 */
public class AdapterStoreImpl extends
		AbstractGeoWavePersistence<InternalDataAdapter<?>> implements
		PersistentAdapterStore
{

	public AdapterStoreImpl(
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		super(
				operations,
				options,
				MetadataType.ADAPTER);
	}

	@Override
	public void addAdapter(
			final InternalDataAdapter<?> adapter ) {
		addObject(adapter);
	}

	@Override
	public InternalDataAdapter<?> getAdapter(
			final Short internalAdapterId ) {
		return getObject(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)),
				null);
	}

	@Override
	protected InternalDataAdapter<?> fromValue(
			GeoWaveMetadata entry ) {
		WritableDataAdapter<?> adapter = (WritableDataAdapter<?>) PersistenceUtils.fromBinary(entry.getValue());
		return new InternalDataAdapterWrapper<>(
				adapter,
				ByteArrayUtils.byteArrayToShort(entry.getPrimaryId()));
	}

	@Override
	protected byte[] getValue(
			InternalDataAdapter<?> object ) {
		return PersistenceUtils.toBinary(object.getAdapter());
	}

	@Override
	public boolean adapterExists(
			final Short internalAdapterId ) {
		return objectExists(
				new ByteArrayId(
						ByteArrayUtils.shortToByteArray(internalAdapterId)),
				null);
	}

	@Override
	protected ByteArrayId getPrimaryId(
			final InternalDataAdapter<?> persistedObject ) {
		return new ByteArrayId(
				ByteArrayUtils.shortToByteArray(persistedObject.getInternalAdapterId()));
	}

	@Override
	public CloseableIterator<InternalDataAdapter<?>> getAdapters() {
		return getObjects();
	}

	@Override
	public void removeAdapter(
			ByteArrayId adapterId ) {
		remove(adapterId);
	}
}
