package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;

public class shortUtils
{

	public static byte[] toBytes(
			short shortValue ) {
		byte[] bytes = new byte[2];
		ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
		buffer.putShort(shortValue);
		return buffer.array();
	}

	public static short fromBytes(
			byte[] byteArray ) {
		ByteBuffer buffer = ByteBuffer.wrap(byteArray);
		return buffer.getShort();
	}

	public static void main(
			String[] argv ) {
		short a = 123;
		byte[] byteArray = toBytes(a);
		System.out.println(byteArray);
		System.out.println(fromBytes(byteArray));
	}
}
