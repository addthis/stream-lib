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

import java.util.Arrays;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.IBuilder;

public class LogLog implements ICardinality {

    /**
     * Gamma function computed using Mathematica
     * AccountingForm[
     * N[With[{m = 2^Range[0, 31]},
     * m (Gamma[-1/m]*(1 - 2^(1/m))/Log[2])^-m], 14]]
     */
    protected static final double[] mAlpha = {
            0,
            0.44567926005415,
            1.2480639342271,
            2.8391255240079,
            6.0165231584809,
            12.369319965552,
            25.073991603111,
            50.482891762408,
            101.30047482584,
            202.93553338100,
            406.20559696699,
            812.74569744189,
            1625.8258850594,
            3251.9862536323,
            6504.3069874480,
            13008.948453415,
            26018.231384516,
            52036.797246302,
            104073.92896967,
            208148.19241629,
            416296.71930949,
            832593.77309585,
            1665187.8806686,
            3330376.0958140,
            6660752.5261049,
            13321505.386687,
            26643011.107850,
            53286022.550177,
            106572045.43483,
            213144091.20414,
            426288182.74275,
            852576365.81999
    };

    protected final int k;
    protected int m;
    protected double Ca;
    protected byte[] M;
    protected int Rsum = 0;

    public LogLog(int k) {
        if (k >= (mAlpha.length - 1)) {
            throw new IllegalArgumentException(String.format("Max k (%d) exceeded: k=%d", mAlpha.length - 1, k));
        }

        this.k = k;
        this.m = 1 << k;
        this.Ca = mAlpha[k];
        this.M = new byte[m];
    }

    public LogLog(byte[] M) {
        this.M = M;
        this.m = M.length;
        this.k = Integer.numberOfTrailingZeros(m);
        assert (m == (1 << k)) : "Invalid array size: M.length must be a power of 2";
        this.Ca = mAlpha[k];
        for (byte b : M) {
            Rsum += b;
        }
    }

    @Override
    public byte[] getBytes() {
        return M;
    }

    public int sizeof() {
        return m;
    }

    @Override
    public long cardinality() {
        /*
        for(int j=0; j<m; j++)
            System.out.print(M[j]+"|");
        System.out.println();
        */

        double Ravg = Rsum / (double) m;
        return (long) (Ca * Math.pow(2, Ravg));
    }

    @Override
    public boolean offerHashed(long hashedLong) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        boolean modified = false;
        int j = hashedInt >>> (Integer.SIZE - k);
        byte r = (byte) (Integer.numberOfLeadingZeros((hashedInt << k) | (1 << (k - 1))) + 1);
        if (M[j] < r) {
            Rsum += r - M[j];
            M[j] = r;
            modified = true;
        }

        return modified;
    }

    @Override
    public boolean offer(Object o) {
        int x = MurmurHash.hash(o);
        return offerHashed(x);
    }

    /**
     * Computes the position of the first set bit of the last Integer.SIZE-k bits
     *
     * @return Integer.SIZE-k if the last k bits are all zero
     */
    protected static int rho(int x, int k) {
        return Integer.numberOfLeadingZeros((x << k) | (1 << (k - 1))) + 1;
    }

    /**
     * @return this if estimators is null or no arguments are passed
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be instances of LogLog of the same size)
     */
    @Override
    public ICardinality merge(ICardinality... estimators) throws LogLogMergeException {
        if (estimators == null) {
            return new LogLog(M);
        }

        byte[] mergedBytes = Arrays.copyOf(this.M, this.M.length);
        for (ICardinality estimator : estimators) {
            if (!(this.getClass().isInstance(estimator))) {
                throw new LogLogMergeException("Cannot merge estimators of different class");
            }
            if (estimator.sizeof() != this.sizeof()) {
                throw new LogLogMergeException("Cannot merge estimators of different sizes");
            }
            LogLog ll = (LogLog) estimator;
            for (int i = 0; i < mergedBytes.length; ++i) {
                mergedBytes[i] = (byte) Math.max(mergedBytes[i], ll.M[i]);
            }
        }

        return new LogLog(mergedBytes);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws LogLogMergeException if estimators are not mergeable (all estimators must be the same size)
     */
    public static LogLog mergeEstimators(LogLog... estimators) throws LogLogMergeException {
        if (estimators == null || estimators.length == 0) {
            return null;
        }
        return (LogLog) estimators[0].merge(Arrays.copyOfRange(estimators, 1, estimators.length));
    }


    @SuppressWarnings("serial")
    protected static class LogLogMergeException extends CardinalityMergeException {

        public LogLogMergeException(String message) {
            super(message);
        }
    }

    public static class Builder implements IBuilder<ICardinality> {

        protected final int k;

        public Builder() {
            this(16);
        }

        public Builder(int k) {
            this.k = k;
        }

        @Override
        public LogLog build() {
            return new LogLog(k);
        }

        @Override
        public int sizeof() {
            return 1 << k;
        }
    }
}
