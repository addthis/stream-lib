package com.clearspring.analytics.util;


import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;

public class UnsignedIntComparator implements Comparator<byte[]>
{
	@Override
	public int compare(byte[] left, byte[] right)
	{
		int l = Varint.readUnsignedVarInt(left);
		int r = Varint.readUnsignedVarInt(right);
		return l - r;
	}
}