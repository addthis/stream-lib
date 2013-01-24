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
		// we know that these are unsigned var ints
		try
		{
			int minLength = Math.min(left.length, right.length);
			for (int i = 0; i < minLength; i++)
			{
				DataInput lis = new DataInputStream(new ByteArrayInputStream(left));
				DataInput ris = new DataInputStream(new ByteArrayInputStream(right));
				int l = Varint.readUnsignedVarInt(lis);
				int r = Varint.readUnsignedVarInt(ris);
				int result = l - r;
				if (result != 0)
				{
					return result;
				}
			}
			return left.length - right.length;
		}
		catch (IOException e)
		{
			//TODO
		}
		return 0;
	}
}