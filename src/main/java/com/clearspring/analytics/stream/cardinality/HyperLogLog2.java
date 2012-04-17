package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.Bytes;
import com.clearspring.analytics.util.IBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

public class HyperLogLog2 implements ICardinality
{
	private final int m;
	private final double alphaM;
	private final int[] M;
	private final int kComp;

	private static final double pow_2_32 = Math.pow(-2, 32);

	public HyperLogLog2(double rsd)
	{
		int bits = (int)(1.04 / rsd);
		int k = (int)Math.ceil(log2(Math.pow(bits,2)));
		kComp = 32 - k;
		m = (int)Math.pow(2, k);

		alphaM = m == 16 ? 0.673
				: m == 32 ? 0.697
				: m == 64 ? 0.709
				: 0.7213 / (1 + 1.079 / m);

		M = new int[m];
	}

	public HyperLogLog2(int[] mergedArray)
	{
		M = mergedArray;
		m = M.length;
		int k = (int)Math.ceil(log2(Math.pow(m,2)));
		kComp = 32 - k;

		alphaM = m == 16 ? 0.673
				: m == 32 ? 0.697
				: m == 64 ? 0.709
				: 0.7213 / (1 + 1.079 / m);

	}


	private double log2(double x)
	{
		return Math.log( x ) / Math.log( 2 );
	}

	private int rank(int hash, int max)
	{
		int r = 1;
		while ((hash & 1) == 0 && r <= max) { ++r; hash >>>= 1; }
		return r;
	}

	@Override
	public boolean offer(Object o)
	{
		int x = MurmurHash.hash(o.toString().getBytes());
		int j = x >>> kComp;
		int orig = M[j];
		M[j] = Math.max(M[j], rank(x, kComp));
		return orig == M[j];
	}

	@Override
	public long cardinality()
	{
		double c = 0;
		for (int j = 0; j < m; ++j)
		{
			c += 1 / Math.pow(2, M[j]);
		}
		double estimate = alphaM * Math.pow(m, 2) / c;

		if (estimate <= 5.0/2.0 * m)
		{
			double V = 0;
			for (int i = 0; i < m; ++i) if (M[i] == 0) ++V;
			if (V > 0) estimate = m * Math.log(m / V);
		}
		else if (estimate > 1.0/30.0 * pow_2_32)
		{
			estimate = -pow_2_32 * Math.log(1 - estimate / pow_2_32);
		}

		return Math.round(estimate);
	}

	@Override
	public int sizeof()
	{
		return m;
	}

	@Override
	public byte[] getBytes() throws IOException
	{
		int bytes = M.length*4;
		byte[] bArray = new byte[bytes+4];

		Bytes.addByteArray(bArray, 0, bytes);
		Bytes.addByteArray(bArray, 4, M);

		return bArray;
	}

	@Override
	public ICardinality merge(ICardinality... estimators)
	{
		return HyperLogLog2.mergeEstimators(prepMerge(estimators));
	}

	protected HyperLogLog2[] prepMerge(ICardinality... estimators)
	{
		int numEstimators = (estimators == null) ? 0 : estimators.length;
		HyperLogLog2[] lls = new HyperLogLog2[numEstimators+1];
		if(numEstimators > 0)
		{
			for(int i=0; i<numEstimators; i++)
			{
				if(estimators[i] instanceof HyperLogLog2)
				{
					lls[i] = (HyperLogLog2)estimators[i];
				}
				else
				{
					throw new RuntimeException("Unable to merge HyperLogLog2 with "+estimators[i].getClass().getName());
				}
			}
		}
		lls[numEstimators] = this;
		return lls;
	}

	/**
	 * @param estimators
	 * @return null if no estimators are provided
	 */
	protected static int[] mergeArrays(HyperLogLog2... estimators)
	{
		int[] mergedArray = null;
		int numEstimators = (estimators == null) ? 0 : estimators.length;
		if(numEstimators > 0)
		{
			int size = estimators[0].sizeof();
			mergedArray = new int[size];

			for(int e=0; e<numEstimators; e++)
			{
				if(estimators[e].sizeof() != size)
				{
					throw new RuntimeException("Cannot merge estimators of different sizes");
				}

				for(int b=0; b<size; b++)
				{
					int v = mergedArray[b];
					int ev = estimators[e].M[b];
					mergedArray[b] = ev > v ? ev : v;
				}
			}
		}
		return mergedArray;
	}

	/**
	 * Merges estimators to produce an estimator for their combined streams
	 * @param estimators
	 * @return merged estimator or null if no estimators were provided
	 */
	public static HyperLogLog2 mergeEstimators(HyperLogLog2... estimators)
	{
		HyperLogLog2 merged = null;

		int[] mergedArray = mergeArrays(estimators);
		if(mergedArray != null) merged = new HyperLogLog2(mergedArray);

		return merged;
	}

	public static int[] getBits(byte[] mBytes) throws IOException
	{
		int bitSize = mBytes.length / 4;
		int[] bits = new int[bitSize];
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(mBytes));
		for (int i = 0; i < bitSize; i++)
		{
			bits[i] = dis.readInt();
		}
		return bits;
	}

	public static class Builder implements IBuilder<ICardinality>, Serializable
	{
		private static final long serialVersionUID = 2205437102378081334L;


		@Override
		public HyperLogLog2 build()
		{
			return new HyperLogLog2(0.05);
		}

		@Override
		public int sizeof()
		{
			return 0;
		}

		public static HyperLogLog2 build(byte[] bytes) throws IOException
		{
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputStream oi = new DataInputStream(bais);
			int size = oi.readInt();
			byte[] arrayBytes = new byte[size];
			oi.readFully(arrayBytes);
			return new HyperLogLog2(getBits(arrayBytes));
		}
	}
}
