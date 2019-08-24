/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
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

package com.clearspring.experimental.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

import java.io.IOException;

/**
 * Java implementation of HyperBitBit (HBB) algorithm as seen on the presentation
 * by Robert Sedgewick:
 * <p/>
 * https://www.cs.princeton.edu/~rs/talks/AC11-Cardinality.pdf
 * <p/>
 * HBB aims to beat HyperLogLog.
 * From the talk, on practical data:
 * - HyperBitBit, for N < 2^64,
 * -  Uses 128 + 6 bits. (in this implementation case 128 + 8)
 * - Estimates cardinality within 10% of the actual.
 * <p/>
 * The algorithm still need some improvements.
 * - If you insert twice the same element the structure can change (not as in HLL)
 * - For small cardinalities it does not work AT ALL.
 * - The constatn 5.4 used in the cardinality estimation formula should be refined
 * with real world applications feedback
 * <p/>
 * Even so, HyperBitBit has the necessary characteristics to become
 * a better algorithm than HyperLogLog:
 * - Makes one pass through the stream.
 * - Uses a few dozen machine instructions per value
 * - Uses a few hundred bits
 * - Achieves 10% relative accuracy or better
 * <p/>
 * Any feedback to improve the algorithm in its weak points will be welcome.
 * <p/>
 */

public class HyperBitBit implements ICardinality {

    int lgN;
    long sketch;
    long sketch2;

    /**
     * Create a new HyperBitBit instance.
     *
     * Remember that it does not work well for small cardinalities!
     */
    public HyperBitBit() {
        lgN = 5;
        sketch = 0;
        sketch2 = 0;
    }

    @Override
    public boolean offer(Object o) {
        final long x = MurmurHash.hash64(o);
        return offerHashed(x);
    }

    @Override
    public boolean offerHashed(long hashedLong) {
        long k = (hashedLong << 58) >> 58;
        // Calculate the position of the leftmost 1-bit.
        int r = Long.numberOfLeadingZeros(hashedLong >> 6) - 6;

        boolean modified = false;

        if (r > lgN) {
            modified = true;
            sketch = sketch | 1L << k;
        }
        if (r > lgN+1) {
            modified = true;
            sketch2 = sketch2 | 1L << k;
        }
        if (Long.bitCount(sketch) > 31) {
            modified = true;
            sketch = sketch2;
            sketch2 = 0;
            ++lgN;
        }

        return modified;
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cardinality() {
        double exponent = lgN + 5.4 + Long.bitCount(sketch)/32.0;
        return (long) Math.pow(2, exponent);
    }

    @Override
    public int sizeof() {
        return 0;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return new byte[0];
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        throw new HyperBitBitMergeException("Cannot merge estimators of HyperBitBit class");
    }

    @SuppressWarnings("serial")
    static class HyperBitBitMergeException extends CardinalityMergeException {
        public HyperBitBitMergeException(String message) {
            super(message);
        }
    }
}
