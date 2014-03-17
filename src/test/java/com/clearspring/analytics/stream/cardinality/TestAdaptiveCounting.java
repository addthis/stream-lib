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

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class TestAdaptiveCounting {

    @Test
    public void testRho() {
        assertEquals(17, LogLog.rho(0, 16));
        assertEquals(16, LogLog.rho(1, 16));
        assertEquals(15, LogLog.rho(2, 16));
        assertEquals(1, LogLog.rho(0x00008000, 16));

        assertEquals(23, LogLog.rho(0, 10));
        assertEquals(22, LogLog.rho(1, 10));
        assertEquals(21, LogLog.rho(2, 10));
        assertEquals(1, LogLog.rho(0x00200000, 10));
    }

    @Test
    public void testRhoL() {
        assertEquals(49, AdaptiveCounting.rho(0L, 16));
        assertEquals(48, AdaptiveCounting.rho(1L, 16));
        assertEquals(47, AdaptiveCounting.rho(2L, 16));
        assertEquals(1, AdaptiveCounting.rho(0x80008000L, 32));

        assertEquals(55, AdaptiveCounting.rho(0L, 10));
        assertEquals(54, AdaptiveCounting.rho(1L, 10));
        assertEquals(53, AdaptiveCounting.rho(2L, 10));
        assertEquals(1, AdaptiveCounting.rho(0x0020000000000000L, 10));

        assertEquals(3, AdaptiveCounting.rho(0xDEA07EEFFEEDCAFEL, 15));
    }

    @Test
    public void testJ() {
        long x = 0xDEADBEEFFEEDCAFEL;
        int k = 12;
        int j = (int) (x >>> (Long.SIZE - k));
        assertEquals(0xDEA, j);
    }

    @Test
    public void testMerge() throws CardinalityMergeException {
        int numToMerge = 10;
        int cardinality = 10000;

        AdaptiveCounting[] lcs = new AdaptiveCounting[numToMerge];
        AdaptiveCounting baseline = new AdaptiveCounting(16);
        for (int i = 0; i < numToMerge; i++) {
            lcs[i] = new AdaptiveCounting(16);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                lcs[i].offer(val);
                baseline.offer(val);
            }
        }

        int expectedCardinality = numToMerge * cardinality;
        long mergedEstimate = AdaptiveCounting.mergeEstimators(lcs).cardinality();
        double error = Math.abs(mergedEstimate - expectedCardinality) / (double) expectedCardinality;
        assertEquals(0.01, error, 0.01);

        AdaptiveCounting lc = lcs[0];
        lcs = Arrays.asList(lcs).subList(1, lcs.length).toArray(new AdaptiveCounting[0]);
        mergedEstimate = lc.merge(lcs).cardinality();
        error = Math.abs(mergedEstimate - expectedCardinality) / (double) expectedCardinality;
        assertEquals(0.01, error, 0.01);

        assertEquals(baseline.cardinality(), mergedEstimate);
    }

    @Ignore
    @Test
    public void testLongCardinality() {
        ICardinality ac = new AdaptiveCounting(16);
        for (long i = 0; i < 5000000000L; i++) {
            ac.offer(Long.valueOf(i));
            if (i % 10000000 == 0) {
                System.out.println("actual: " + i + ", estimated: " + ac.cardinality());
            }
        }

        System.out.println(ac.cardinality());
        assertEquals(5000000000L, ac.cardinality(), 100000000);

    }

    @Test
    public void testSerialization() {
        AdaptiveCounting ac = new AdaptiveCounting(10);
        testSerialization(ac);
    }

    private void testSerialization(AdaptiveCounting ac) {
        AdaptiveCounting clone = new AdaptiveCounting(ac.getBytes());
        assertAdaptiveCountingEquals(ac, clone);

        assertEquals(0, ac.cardinality());

        for (int i = 0; i < 100; i++) {
            ac.offer(i);
        }

        clone = new AdaptiveCounting(ac.getBytes());
        assertAdaptiveCountingEquals(ac, clone);

        for (int i = 0; i < 1000000; i++) {
            ac.offer(i);
        }

        clone = new AdaptiveCounting(ac.getBytes());
        assertAdaptiveCountingEquals(ac, clone);
    }

    private void assertAdaptiveCountingEquals(AdaptiveCounting expected, AdaptiveCounting actual) {
        assertArrayEquals(expected.M, actual.M);
        assertEquals(expected.k, actual.k);
        assertEquals(expected.m, actual.m);
        assertEquals(expected.Ca, actual.Ca, 0.00000001);
        assertEquals(expected.Rsum, actual.Rsum);

        assertEquals(expected.b_e, actual.b_e);
        assertEquals(expected.B_s, actual.B_s, 0.00000001);

        assertEquals(expected.sizeof(), actual.sizeof());
        assertEquals(expected.cardinality(), actual.cardinality());
    }
}
