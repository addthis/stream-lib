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

import java.io.IOException;

import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestLogLog {

    @Test
    public void testSerialization() throws IOException {
        LogLog hll = new LogLog(8);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        LogLog hll2 = new LogLog(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testHighCardinality() {
        long start = System.currentTimeMillis();
        LogLog loglog = new LogLog(10);
        int size = 10000000;
        for (int i = 0; i < size; i++) {
            loglog.offer(TestICardinality.streamElement(i));
        }
        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = loglog.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        assertTrue(err < .11);
    }

    @Test
    public void testHighCardinalityHighOrder() {
        long start = System.currentTimeMillis();
        LogLog loglog = new LogLog(25);
        int size = 100000000;
        for (int i = 0; i < size; i++) {
            loglog.offer(TestICardinality.streamElement(i));
        }
        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = loglog.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(size);
        System.out.println(estimate);
        System.out.println(err);
        assertTrue(err < .06);
    }

    @Test
    public void testMerge() throws CardinalityMergeException {
        int numToMerge = 5;
        int bits = 16;
        int cardinality = 1000000;

        LogLog[] loglogs = new LogLog[numToMerge];
        LogLog baseline = new LogLog(bits);
        for (int i = 0; i < numToMerge; i++) {
            loglogs[i] = new LogLog(bits);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                loglogs[i].offer(val);
                baseline.offer(val);
            }
        }


        LogLog hll = loglogs[0];
        loglogs = Arrays.asList(loglogs).subList(1, loglogs.length).toArray(new LogLog[0]);
        long mergedEstimate = hll.merge(loglogs).cardinality();
        long baselineEstimate = baseline.cardinality();

        System.out.println("Baseline estimate: " + baselineEstimate);

        assertEquals(mergedEstimate, baselineEstimate);
    }

    @Test
    @Ignore
    public void testPrecise() throws CardinalityMergeException {
        int cardinality = 1000000000;
        int b = 12;
        LogLog baseline = new LogLog(b);
        LogLog guava128 = new LogLog(b);
        HashFunction hf128 = Hashing.murmur3_128();
        for (int j = 0; j < cardinality; j++) {
            Double val = Math.random();
            String valString = val.toString();
            baseline.offer(valString);
            guava128.offerHashed(hf128.hashString(valString, Charsets.UTF_8).asLong());
            if (j > 0 && j % 1000000 == 0) {
                System.out.println("current count: " + j);
            }
        }


        long baselineEstimate = baseline.cardinality();
        long g128Estimate = guava128.cardinality();
        double se = cardinality * (1.04 / Math.sqrt(Math.pow(2, b)));
        double baselineError = (baselineEstimate - cardinality) / (double) cardinality;
        double g128Error = (g128Estimate - cardinality) / (double) cardinality;
        System.out.format("b: %f g128 %f", baselineError, g128Error);
        assertTrue("baseline estimate bigger than expected", baselineEstimate >= cardinality - (2 * se));
        assertTrue("baseline estimate smaller than expected", baselineEstimate <= cardinality + (2 * se));
        assertTrue("g128 estimate bigger than expected", g128Estimate >= cardinality - (2 * se));
        assertTrue("g128 estimate smaller than expected", g128Estimate <= cardinality + (2 * se));
    }
}
