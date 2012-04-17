package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.Bytes;
import com.clearspring.analytics.util.IBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Java implementation of HyperLogLog (HLL) algorithm from this paper:
 *
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HLL is an improved version of LogLog that is capable of estimating
 * the cardinality of a set with accuracy = 1.04/sqrt(m) where
 * m = 2^b.  So we can control accuracy vs space usage by increasing
 * or decreasing b.
 *
 * The main benefit of using HLL over LL is that it only requires 64%
 * of the space that LL does to get the same accuracy.
 *
 * This implementation implements a single counter.  If a large (millions)
 * number of counters are required you may want to refer to:
 *
 * http://dsiutils.dsi.unimi.it/
 *
 * It has a more complex implmentation of HLL that supports multiple counters
 * in a single object, drastically reducing the java overhead from creating
 * a large number of objects.
 *
 * This implementation leveraged a javascript implementation that Yammer has
 * been working on:
 *
 * https://github.com/yammer/probablyjs
 */
public class HyperLogLog implements ICardinality
{
	private final static int POW_2_32 = (int)Math.pow(2, 32);
	private final static int NEGATIVE_POW_2_32 = (int)Math.pow(-2, 32);

	private final RegisterSet registerSet;
	private final int log2m;
	private final int m;
	private final double alphaMM;


	/**
	 * Create a new HyperLogLog instance using the specified standard deviation.
	 *
	 * @param rsd - the relative standard deviation for the counter.
	 *				smaller values create counters that require more space.
	 */
	public HyperLogLog(double rsd)
	{
		this.log2m = (int) (Math.log(( 1.106 / rsd ) * ( 1.106 / rsd ))/Math.log(2));
		this.m = (int) Math.pow(2, this.log2m);
		this.registerSet = new RegisterSet(m);

		// See the paper.
		switch ( log2m ) {
			case 4:
				alphaMM = 0.673 * m * m; break;
			case 5:
				alphaMM = 0.697 * m * m; break;
			case 6:
				alphaMM = 0.709 * m * m; break;
			default:
				alphaMM = ( 0.7213 / ( 1 + 1.079 / m ) ) * m * m;
		}
	}

	/**
	 * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
	 * the counter.  The larger the log2m the better the accuracy.
	 *
	 * accuracy = 1.04/sqrt(2^log2m)
	 *
	 * @param log2m - the number of bits to use as the basis for the HLL instance
	 */
	public HyperLogLog(int log2m)
	{
		this.log2m = log2m;
		this.m = (int) Math.pow(2, this.log2m);
		this.registerSet = new RegisterSet(m);

		// See the paper.
		switch ( log2m ) {
			case 4:
				alphaMM = 0.673 * m * m; break;
			case 5:
				alphaMM = 0.697 * m * m; break;
			case 6:
				alphaMM = 0.709 * m * m; break;
			default:
				alphaMM = ( 0.7213 / ( 1 + 1.079 / m ) ) * m * m;
		}
	}

	/**
	 * Creates a new HyperLogLog instance using the given registers.  Used for unmarshalling a serialized
	 * instance and for merging multiple counters together.
	 *
	 * @param registerSet - the initial values for the register set
	 */
	public HyperLogLog(int log2m, RegisterSet registerSet)
	{
		this.registerSet = registerSet;
		this.log2m = log2m;
		this.m = (int) Math.pow(2, this.log2m);

		// See the paper.
		switch ( log2m ) {
			case 4:
				alphaMM = 0.673 * m * m; break;
			case 5:
				alphaMM = 0.697 * m * m; break;
			case 6:
				alphaMM = 0.709 * m * m; break;
			default:
				alphaMM = ( 0.7213 / ( 1 + 1.079 / m ) ) * m * m;
		}
	}


	@Override
	public boolean offer(Object o)
	{
		final int x = MurmurHash.hash(o.toString().getBytes());
		// j becomes the binary address determined by the first b log2m of x
		// j will be between 0 and 2^log2m
		final int j =  x >>> (Integer.SIZE - log2m);
		final int r = Integer.numberOfLeadingZeros((x << this.log2m) | (1 << (this.log2m - 1)) + 1) + 1;
		if (registerSet.get(j) < r)
		{
			registerSet.set(j, r);
			return true;
		}
		else
		{
			return false;
		}
	}


	@Override
	public long cardinality()
	{
		double registerSum = 0;
		int count = registerSet.count;
		for (int j = 0; j < registerSet.count; j++)
		{
			registerSum += Math.pow(2, (-1 * registerSet.get(j)));
		}

		double estimate = alphaMM  * (1 / registerSum);

		if(estimate <= (5.0/2.0) * count) {
			// Small Range Estimate
			double zeros = 0.0;
			for(int z = 0; z < count; z++) {
				if(registerSet.get(z) == 0) { zeros++; }
			}
			return Math.round(count * Math.log(count / zeros));
		} else if(estimate <= (1.0/30.0) * POW_2_32) {
			// Intermedia Range Estimate
			return Math.round(estimate);
		} else if(estimate > (1.0/30.0) * POW_2_32) {
			// Large Range Estimate
			return Math.round( (NEGATIVE_POW_2_32 * Math.log(1 - (estimate / POW_2_32))) );
		}
		return 0;
	}

	@Override
	public int sizeof()
	{
		return registerSet.size * 4;
	}

	@Override
	public byte[] getBytes() throws IOException
	{
		int bytes = registerSet.size*4;
		byte[] bArray = new byte[bytes+8];

		Bytes.addByteArray(bArray, 0, log2m);
		Bytes.addByteArray(bArray, 4, bytes);
		Bytes.addByteArray(bArray, 8, registerSet.bits());

		return bArray;
	}

	@Override
	public ICardinality merge(ICardinality... estimators)
	{
		return HyperLogLog.mergeEstimators(prepMerge(estimators));
	}

	protected HyperLogLog[] prepMerge(ICardinality... estimators)
	{
		int numEstimators = (estimators == null) ? 0 : estimators.length;
		HyperLogLog[] lls = new HyperLogLog[numEstimators+1];
		if(numEstimators > 0)
		{
			for(int i=0; i<numEstimators; i++)
			{
				if(estimators[i] instanceof HyperLogLog)
				{
					lls[i] = (HyperLogLog)estimators[i];
				}
				else
				{
					throw new RuntimeException("Unable to merge HyperLogLog with "+estimators[i].getClass().getName());
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
	protected static RegisterSet mergeRegisters(HyperLogLog... estimators)
	{
		RegisterSet mergedSet = null;
		int numEstimators = (estimators == null) ? 0 : estimators.length;
		if(numEstimators > 0)
		{
			int size = estimators[0].sizeof();
			mergedSet = new RegisterSet((int) Math.pow(2, estimators[0].log2m));

			for(int e=0; e<numEstimators; e++)
			{
				if(estimators[e].sizeof() != size)
				{
					throw new RuntimeException("Cannot merge estimators of different sizes");
				}
				HyperLogLog estimator = estimators[e];
				for(int b=0; b<mergedSet.count; b++)
				{
					if (estimator.registerSet.get(b) > mergedSet.get(b))
					{
						mergedSet.set(b, estimator.registerSet.get(b));
					}
				}
			}
		}
		return mergedSet;
	}

	/**
	 * Merges estimators to produce an estimator for their combined streams
	 * @param estimators
	 * @return merged estimator or null if no estimators were provided
	 */
	public static HyperLogLog mergeEstimators(HyperLogLog... estimators)
	{
		HyperLogLog merged = null;

		RegisterSet mergedSet = mergeRegisters(estimators);
		if(mergedSet != null) merged = new HyperLogLog(estimators[0].log2m, mergedSet);

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
		private int k;

		public Builder(int k)
		{
			this.k = k;
		}

		@Override
		public HyperLogLog build()
		{
			return new HyperLogLog(k);
		}

		@Override
		public int sizeof()
		{
			return RegisterSet.getBits(1 << k) * 4;
		}

		public static HyperLogLog build(byte[] bytes) throws IOException
		{
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputStream oi = new DataInputStream(bais);
			int log2m = oi.readInt();
			int size = oi.readInt();
			byte[] longArrayBytes = new byte[size];
			oi.readFully(longArrayBytes);
			return new HyperLogLog(log2m, new RegisterSet((int) Math.pow(2, log2m), getBits(longArrayBytes)));
		}
	}
}
