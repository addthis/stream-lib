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

package com.clearspring.analytics.stream.cardinality;

import com.clearspring.analytics.hash.MurmurHash;

import java.io.IOException;

public class HyperBitBit implements ICardinality {

    int lgN;
    long sketch;
    long sketch2;

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
