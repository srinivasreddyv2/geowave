package mil.nga.giat.geowave.core.store.index.numeric;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class NumberRangeFilter implements
		DistributableQueryFilter
{
	protected ByteArrayId fieldId;
	protected Number lowerValue;
	protected Number upperValue;
	protected boolean inclusiveLow;
	protected boolean inclusiveHigh;

	protected NumberRangeFilter() {
		super();
	}

	public NumberRangeFilter(
			ByteArrayId fieldId,
			Number lowerValue,
			Number upperValue,
			boolean inclusiveLow,
			boolean inclusiveHigh ) {
		super();
		this.fieldId = fieldId;
		this.lowerValue = lowerValue;
		this.upperValue = upperValue;
		this.inclusiveHigh = inclusiveHigh;
		this.inclusiveLow = inclusiveLow;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArrayId value = (ByteArrayId) persistenceEncoding.getCommonData().getValue(
				fieldId);
		if (value != null) {
			final double val = Lexicoders.DOUBLE.fromByteArray(value.getBytes());
			if (inclusiveLow && inclusiveHigh)
				return val >= lowerValue.doubleValue() && val <= upperValue.doubleValue();
			else if (inclusiveLow)
				return val >= lowerValue.doubleValue() && val < upperValue.doubleValue();
			else if (inclusiveHigh)
				return val > lowerValue.doubleValue() && val <= upperValue.doubleValue();
			else
				return val > lowerValue.doubleValue() && val < upperValue.doubleValue();
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldId.getBytes().length + 16);
		bb.putInt(fieldId.getBytes().length);
		bb.put(fieldId.getBytes());
		bb.putDouble(lowerValue.doubleValue());
		bb.putDouble(upperValue.doubleValue());
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldIdBytes = new byte[bb.getInt()];
		bb.get(fieldIdBytes);
		fieldId = new ByteArrayId(
				fieldIdBytes);
		lowerValue = new Double(
				bb.getDouble());
		upperValue = new Double(
				bb.getDouble());

	}

}