package mil.nga.giat.geowave.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;

public class JobContextInternalAdapterStore implements
		InternalAdapterStore
{
	private static final Class<?> CLASS = JobContextInternalAdapterStore.class;
	private final JobContext context;
	private final InternalAdapterStore persistentInternalAdapterStore;

	public JobContextInternalAdapterStore(
			final JobContext context,
			final InternalAdapterStore persistentInternalAdapterStore ) {
		this.context = context;
		this.persistentInternalAdapterStore = persistentInternalAdapterStore;
	}

	@Override
	public ByteArrayId getAdapterId(
			final short internalAdapterId ) {
		return persistentInternalAdapterStore.getAdapterId(internalAdapterId);
	}

	@Override
	public Short getInternalAdapterId(
			final ByteArrayId adapterId ) {
		// TODO figure out where to add internal adapter IDs to the job context
		// and read it from the job context instead
		return persistentInternalAdapterStore.getInternalAdapterId(adapterId);
	}

	protected Short getInternalAdapterIdFromJobContext(
			final ByteArrayId adapterId ) {
		// TODO figure out where to add internal adapter IDs to the job context
		// and read it from the job context instead
		return GeoWaveConfiguratorBase.getInternalAdapterId(
				CLASS,
				context,
				adapterId);
	}

	@Override
	public short addAdapterId(
			final ByteArrayId adapterId ) {
		return persistentInternalAdapterStore.addAdapterId(adapterId);
	}

	@Override
	public boolean remove(
			final ByteArrayId adapterId ) {
		return persistentInternalAdapterStore.remove(adapterId);
	}

	public static void addInternalDataAdapter(
			final Configuration configuration,
			final ByteArrayId adapterId,
			final short internalAdapterId ) {
		// TODO figure out where to add this
		GeoWaveConfiguratorBase.addInternalAdapterId(
				CLASS,
				configuration,
				adapterId,
				internalAdapterId);
	}

	@Override
	public boolean remove(
			final short internalAdapterId ) {
		return persistentInternalAdapterStore.remove(internalAdapterId);
	}

	@Override
	public void removeAll() {
		persistentInternalAdapterStore.removeAll();
	}

}
