package mil.nga.giat.geowave.mapreduce;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

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
			short internalAdapterId ) {
		return persistentInternalAdapterStore.getAdapterId(internalAdapterId);
	}

	@Override
	public Short getInternalAdapterId(
			ByteArrayId adapterId ) {
		// TODO figure out where to add internal adapter IDs to the job context
		// and read it from the job context instead
		return persistentInternalAdapterStore.getInternalAdapterId(adapterId);
	}

	protected Short getInternalAdapterIdFromJobContext(
			ByteArrayId adapterId ) {
		// TODO figure out where to add internal adapter IDs to the job context
		// and read it from the job context instead
		return GeoWaveConfiguratorBase.getInternalAdapterId(
				CLASS,
				context,
				adapterId);
	}

	@Override
	public short addAdapterId(
			ByteArrayId adapterId ) {
		return persistentInternalAdapterStore.addAdapterId(adapterId);
	}

	@Override
	public boolean remove(
			ByteArrayId adapterId ) {
		return persistentInternalAdapterStore.remove(adapterId);
	}

	public static void addInternalDataAdapter(
			final Configuration configuration,
			final ByteArrayId adapterId,
			short internalAdapterId ) {
		// TODO figure out where to add this
		GeoWaveConfiguratorBase.addInternalAdapterId(
				CLASS,
				configuration,
				adapterId,
				internalAdapterId);
	}

}
