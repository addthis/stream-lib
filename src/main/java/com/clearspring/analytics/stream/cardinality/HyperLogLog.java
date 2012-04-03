package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.IBuilder;
import it.unimi.dsi.Util;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigList;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class HyperLogLog implements ICardinality
{
	/**
	 * The logarithm of the maximum size in registers of a bit vector.
	 */
	public final static int CHUNK_SHIFT = 30;
	/**
	 * The maximum size in registers of a bit vector.
	 */
	public final static long CHUNK_SIZE = 1L << CHUNK_SHIFT;
	/**
	 * The mask used to obtain an register offset in a chunk.
	 */
	public final static long CHUNK_MASK = CHUNK_SIZE - 1;
	/**
	 * An  bit vector containing all registers.
	 */
	protected final LongArrayBitVector bitVector;
	/**
	 * bit views of {@link #bitVector}.
	 */
	protected final LongBigList register;

	/**
	 * The logarithm of the number of registers per counter.
	 */
	protected final int log2m;

	/**
	 * The number of registers per counter.
	 */
	protected final int m;

	/**
	 * The number of registers minus one.
	 */
	protected final int mMinus1;

	/**
	 * The size in bits of each register.
	 */
	protected final int registerSize = 5;

	/**
	 * A seed for hashing.
	 */
	protected long seed;

	/**
	 * The correct value for &alpha;, multiplied by {@link #m}<sup>2</sup> (see the paper).
	 */
	private double alphaMM;

	/**
	 * The mask OR'd with the output of the hash function so that {@link it.unimi.dsi.bits.Fast#leastSignificantBit(long)} does not return too large a value.
	 */
	private long sentinelMask;

	private double rsd;

	/**
	 * Returns the logarithm of the number of registers per counter that are necessary to attain a
	 * given relative standard deviation.
	 *
	 * @param rsd the relative standard deviation to be attained.
	 * @return the logarithm of the number of registers that are necessary to attain relative standard deviation <code>rsd</code>.
	 */
	public static int log2NumberOfRegisters(final double rsd)
	{
		// 1.106 is valid for 16 registers or more.
		return (int) Math.ceil(Fast.log2((1.106 / rsd) * (1.106 / rsd)));
	}


	public static long[] getBits(byte[] M) throws IOException
	{
		int bitSize = M.length / 8;
		long[] bits = new long[bitSize];
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(M));
		for (int i = 0; i < bitSize; i++)
		{
			bits[i] = dis.readLong();
		}
		return bits;
	}

	/**
	 * Creates a new array of counters.
	 *
	 * @param rsd the relative standard deviation.
	 */
	public HyperLogLog(final double rsd)
	{
		this(rsd, Util.randomSeed());
	}

	/**
	 * Creates a new array of counters.
	 *
	 * @param rsd  the relative standard deviation.
	 * @param seed the seed used to compute the hash function.
	 */
	public HyperLogLog(final double rsd, final long seed)
	{
		this.rsd = rsd;
		this.log2m = log2NumberOfRegisters(rsd);
		this.m = 1 << log2m;
		this.mMinus1 = m - 1;
		sentinelMask = 1L << (1 << registerSize) - 2;
		this.bitVector = LongArrayBitVector.ofLength(registerSize * Math.min(CHUNK_SIZE, m - ((long) 0 << CHUNK_SHIFT)));
		this.register = bitVector.asLongBigList(registerSize);
		this.seed = seed;
		// See the paper.
		switch (log2m)
		{
			case 4:
				alphaMM = 0.673 * m * m;
				break;
			case 5:
				alphaMM = 0.697 * m * m;
				break;
			case 6:
				alphaMM = 0.709 * m * m;
				break;
			default:
				alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
		}
	}

	public HyperLogLog(final long[] bits)
	{
		this(bits, 0.05);
	}

	public HyperLogLog(final long[] bits, final double rsd)
	{
		this.rsd = rsd;
		this.log2m = log2NumberOfRegisters(rsd);
		this.m = 1 << log2m;
		this.mMinus1 = m - 1;
		this.bitVector = LongArrayBitVector.wrap(bits);
		this.register = bitVector.asLongBigList(registerSize);
		sentinelMask = 1L << (1 << registerSize) - 2;
		// See the paper.
		switch (log2m)
		{
			case 4:
				alphaMM = 0.673 * m * m;
				break;
			case 5:
				alphaMM = 0.697 * m * m;
				break;
			case 6:
				alphaMM = 0.709 * m * m;
				break;
			default:
				alphaMM = (0.7213 / (1 + 1.079 / m)) * m * m;
		}
	}

	private final static long jenkins(final long x, final long seed)
	{
		long a, b, c;

		/* Set up the internal state */
		a = seed + x;
		b = seed;
		c = 0x9e3779b97f4a7c13L; /* the golden ratio; an arbitrary value */

		a -= b;
		a -= c;
		a ^= (c >>> 43);
		b -= c;
		b -= a;
		b ^= (a << 9);
		c -= a;
		c -= b;
		c ^= (b >>> 8);
		a -= b;
		a -= c;
		a ^= (c >>> 38);
		b -= c;
		b -= a;
		b ^= (a << 23);
		c -= a;
		c -= b;
		c ^= (b >>> 5);
		a -= b;
		a -= c;
		a ^= (c >>> 35);
		b -= c;
		b -= a;
		b ^= (a << 49);
		c -= a;
		c -= b;
		c ^= (b >>> 11);
		a -= b;
		a -= c;
		a ^= (c >>> 12);
		b -= c;
		b -= a;
		b ^= (a << 18);
		c -= a;
		c -= b;
		c ^= (b >>> 22);

		return c;
	}

	@Override
	public boolean offer(Object o)
	{
		long x = jenkins(MurmurHash.hash(o.toString().getBytes()), seed);
		final int j = (int) (x & mMinus1);
		final int r = Fast.leastSignificantBit(x >>> log2m | sentinelMask);
		final long offset = j & CHUNK_MASK;
		register.set(offset, Math.max(r + 1, register.getLong(offset)));
		return true;
	}

	@Override
	public long cardinality()
	{
		int offset = 0;
		int remaining = (int) (Long.SIZE - offset % Long.SIZE);
		int word = (int) (offset / Long.SIZE);
		long[] bits = bitVector.bits();
		long curr = bits[word] >>> offset % Long.SIZE;

		final int registerSize = this.registerSize;
		final int mask = (1 << registerSize) - 1;

		double s = 0;
		int zeroes = 0;
		long r;

		for (int j = m; j-- != 0; )
		{
			if (remaining >= registerSize)
			{
				r = curr & mask;
				curr >>>= registerSize;
				remaining -= registerSize;
			}
			else
			{
				r = (curr | bits[++word] << remaining) & mask;
				curr = bits[word] >>> registerSize - remaining;
				remaining += Long.SIZE - registerSize;
			}

			// if ( ASSERTS ) assert r == registers[ chunk( k ) ].getLong( offset( k ) + j ) : "[" + j + "] " + r + "!=" + registers[ chunk( k ) ].getLong( offset( k ) + j );

			if (r == 0)
			{
				zeroes++;
			}
			s += 1. / (1L << r);
		}

		s = alphaMM / s;
		if (zeroes != 0 && s < 5. * m / 2)
		{
			return (long) (m * Math.log((double) m / zeroes));
		}
		else
		{
			return (long) s;
		}
	}

	@Override
	public int sizeof()
	{
		return m;
	}

	@Override
	public byte[] getBytes() throws IOException
	{
		long[] bits = bitVector.bits();
		byte[] bArray = new byte[bits.length * 8];
		int i = 0;
		for (long bit : bits)
		{
			byte[] bytes = long2bytearray(bit);
			for (byte aByte : bytes)
			{
				bArray[i++] = aByte;
			}

		}
		return bArray;
	}

	private static byte[] long2bytearray(long l)
	{
		byte b[] = new byte[8];

		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.putLong(l);
		return b;
	}

	@Override
	public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException
	{
		if (estimators == null || estimators.length == 0)
		{
			return this;
		}

		ICardinality[] estimatorArray = new HyperLogLog[estimators.length+1];
		for (int i = 0; i < estimators.length; i++)
		{
			estimatorArray[i] = estimators[i];
		}
		estimatorArray[estimatorArray.length-1] = this;
		LongArrayBitVector mergedBytes = mergeBytes(estimatorArray);
		return new HyperLogLog(mergedBytes.bits(), this.rsd);
	}

	public static LongArrayBitVector mergeBytes(ICardinality... estimators)
	{
		LongArrayBitVector mergedBytes = null;
		int numEsitimators = (estimators == null) ? 0 : estimators.length;
		if (numEsitimators > 0)
		{
			HyperLogLog estimator = (HyperLogLog) estimators[0];
			mergedBytes = LongArrayBitVector.ofLength(estimator.bitVector.length());

			for (int e = 0; e < numEsitimators; e++)
			{
				estimator = (HyperLogLog) estimators[e];
				mergedBytes.or(estimator.bitVector);
			}
		}
		return mergedBytes;
	}

	public static class Builder implements IBuilder<ICardinality>, Serializable
	{
		private static final long serialVersionUID = 2205437102378081334L;

		protected final double rsd;

		public Builder()
		{
			this(0.05);
		}

		public Builder(double rsd)
		{
			this.rsd = rsd;
		}

		@Override
		public HyperLogLog build()
		{
			return new HyperLogLog(rsd);
		}

		@Override
		public int sizeof()
		{
			return 1 << log2NumberOfRegisters(rsd);
		}
	}
}
