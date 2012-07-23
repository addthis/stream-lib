/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.IBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Java implementation of HyperLogLog (HLL) algorithm from this paper:
 * <p/>
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * <p/>
 * HLL is an improved version of LogLog that is capable of estimating
 * the cardinality of a set with accuracy = 1.04/sqrt(m) where
 * m = 2^b.  So we can control accuracy vs space usage by increasing
 * or decreasing b.
 * <p/>
 * The main benefit of using HLL over LL is that it only requires 64%
 * of the space that LL does to get the same accuracy.
 * <p/>
 * This implementation implements a single counter.  If a large (millions)
 * number of counters are required you may want to refer to:
 * <p/>
 * http://dsiutils.dsi.unimi.it/
 * <p/>
 * It has a more complex implmentation of HLL that supports multiple counters
 * in a single object, drastically reducing the java overhead from creating
 * a large number of objects.
 * <p/>
 * This implementation leveraged a javascript implementation that Yammer has
 * been working on:
 * <p/>
 * https://github.com/yammer/probablyjs
 */
public class HyperLogLog implements ICardinality
{
    private final static double POW_2_32 = Math.pow(2, 32);
    private final static double NEGATIVE_POW_2_32 = -4294967296.0;

    private final RegisterSet registerSet;
    private final int log2m;
    private final double alphaMM;


    /**
     * Create a new HyperLogLog instance using the specified standard deviation.
     *
     * @param rsd - the relative standard deviation for the counter.
     *            smaller values create counters that require more space.
     */
    public HyperLogLog(double rsd)
    {
        this(log2m(rsd));
    }

    private static int log2m(double rsd)
    {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    /**
     * Create a new HyperLogLog instance.  The log2m parameter defines the accuracy of
     * the counter.  The larger the log2m the better the accuracy.
     * <p/>
     * accuracy = 1.04/sqrt(2^log2m)
     *
     * @param log2m - the number of bits to use as the basis for the HLL instance
     */
    public HyperLogLog(int log2m)
    {
        this(log2m, new RegisterSet((int) Math.pow(2, log2m)));
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
        int m = (int) Math.pow(2, this.log2m);

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


    @Override
    public boolean offer(Object o)
    {
        final int x = MurmurHash.hash(o.toString().getBytes());
        // j becomes the binary address determined by the first b log2m of x
        // j will be between 0 and 2^log2m
        final int j = x >>> (Integer.SIZE - log2m);
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
        return cardinality(true);
    }

    public long cardinality(boolean enableLongRangeCorrection)
    {
        double registerSum = 0;
        int count = registerSet.count;
        for (int j = 0; j < registerSet.count; j++)
        {
            registerSum += Math.pow(2, (-1 * registerSet.get(j)));
        }

        double estimate = alphaMM * (1 / registerSum);

        if (estimate <= (5.0 / 2.0) * count)
        {
            // Small Range Estimate
            double zeros = 0.0;
            for (int z = 0; z < count; z++)
            {
                if (registerSet.get(z) == 0)
                {
                    zeros++;
                }
            }
            return Math.round(count * Math.log(count / zeros));
        }
        else if (estimate <= (1.0 / 30.0) * POW_2_32)
        {
            // Intermedia Range Estimate
            return Math.round(estimate);
        }
        else if (estimate > (1.0 / 30.0) * POW_2_32)
        {
            // Large Range Estimate
            if (enableLongRangeCorrection)
            {
                return Math.round((NEGATIVE_POW_2_32 * Math.log(1.0 - (estimate / POW_2_32))));
            }
            else
            {
                return Math.round(estimate);
            }
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(log2m);
        dos.writeInt(registerSet.size * 4);
        for(int x : registerSet.bits())
        {
            dos.writeInt(x);
        }

        return baos.toByteArray();
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException
    {
        if(estimators == null || estimators.length == 0)
        {
            return this;
        }
        RegisterSet mergedSet;
        int size = this.sizeof();
        mergedSet = new RegisterSet((int) Math.pow(2, this.log2m), this.registerSet.bits());
        for (ICardinality estimator : estimators)
        {
            if(!(estimator instanceof HyperLogLog))
            {
                throw new HyperLogLogMergeException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != size)
            {
                throw new HyperLogLogMergeException("Cannot merge estimators of different sizes");
            }
            HyperLogLog hll = (HyperLogLog) estimator;
            for (int b = 0; b < mergedSet.count; b++)
            {
                mergedSet.set(b, Math.max(mergedSet.get(b), hll.registerSet.get(b)));
            }
        }
        return new HyperLogLog(this.log2m, mergedSet);
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
        private double rsd;

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
            int log2m = log2m(rsd);
            int k = (int)Math.pow(2, log2m);
            return RegisterSet.getBits(k) * 4;
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

    @SuppressWarnings("serial")
    protected static class HyperLogLogMergeException extends CardinalityMergeException
    {
        public HyperLogLogMergeException(String message)
        {
            super(message);
        }
    }
}
