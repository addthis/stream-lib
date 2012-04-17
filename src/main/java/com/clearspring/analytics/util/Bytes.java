package com.clearspring.analytics.util;

import java.nio.ByteBuffer;

public class Bytes
{
	public static void addByteArray(byte[] bArray, int index, long[] bits)
	{
		for (long bit : bits)
		{
			index = addByteArray(bArray, index, bit);
		}
	}

	public static void addByteArray(byte[] bArray, int index, int[] bits)
	{
		for (int bit : bits)
		{
			index = addByteArray(bArray, index, bit);
		}
	}

	public static int addByteArray(byte[] bArray, int index, int value)
	{
		byte[] bytes = int2bytearray(value);
		for (byte aByte : bytes)
		{
			bArray[index++] = aByte;
		}
		return index;
	}

	public static int addByteArray(byte[] bArray, int index, long value)
	{
		byte[] bytes = long2bytearray(value);
		for (byte aByte : bytes)
		{
			bArray[index++] = aByte;
		}
		return index;
	}

	public static void addByteArray(byte[] bArray, int startIndex, double value)
	{
		byte[] bytes = double2bytearray(value);
		for (byte aByte : bytes)
		{
			bArray[startIndex++] = aByte;
		}
	}


	public static byte[] long2bytearray(long l)
	{
		byte b[] = new byte[8];

		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.putLong(l);
		return b;
	}

	public static byte[] int2bytearray(int i)
	{
		byte b[] = new byte[4];

		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.putInt(i);
		return b;
	}

	public static byte[] double2bytearray(double d)
	{
		byte b[] = new byte[8];

		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.putDouble(d);
		return b;
	}

}
