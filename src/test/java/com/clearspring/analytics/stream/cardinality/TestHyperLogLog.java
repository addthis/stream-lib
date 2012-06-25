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

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHyperLogLog
{
    @Test
    public void testComputeCount()
    {
        HyperLogLog hyperLogLog = new HyperLogLog(16);
        hyperLogLog.offer(0);
        hyperLogLog.offer(1);
        hyperLogLog.offer(2);
        hyperLogLog.offer(3);
        hyperLogLog.offer(16);
        hyperLogLog.offer(17);
        hyperLogLog.offer(18);
        hyperLogLog.offer(19);
        hyperLogLog.offer(19);
        assertEquals(8, hyperLogLog.cardinality());
    }

    @Test
    public void testSerialization() throws IOException
    {
        HyperLogLog hll = new HyperLogLog(8);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        HyperLogLog hll2 = HyperLogLog.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testHighCardinality()
    {
        long start = System.currentTimeMillis();
        HyperLogLog hyperLogLog = new HyperLogLog(10);
        int size = 10000000;
        for (int i = 0; i < size; i++)
        {
            hyperLogLog.offer(TestICardinality.streamElement(i));
        }
        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = hyperLogLog.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        assertTrue(err < .1);
    }

    @Test
    public void testHighCardinality_withDefinedRSD()
    {
        long start = System.currentTimeMillis();
        HyperLogLog hyperLogLog = new HyperLogLog(0.01);
        int size = 10000000;
        for (int i = 0; i < size; i++)
        {
            hyperLogLog.offer(TestICardinality.streamElement(i));
        }
        System.out.println("time: " + (System.currentTimeMillis() - start));
        long estimate = hyperLogLog.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        assertTrue(err < .1);
    }

    @Test
    public void testMerge() throws CardinalityMergeException
    {
        int numToMerge = 5;
        int bits = 16;
        int cardinality = 1000000;

        HyperLogLog[] hyperLogLogs = new HyperLogLog[numToMerge];
        HyperLogLog baseline = new HyperLogLog(bits);
        for (int i = 0; i < numToMerge; i++)
        {
            hyperLogLogs[i] = new HyperLogLog(bits);
            for (int j = 0; j < cardinality; j++)
            {
                double val = Math.random();
                hyperLogLogs[i].offer(val);
                baseline.offer(val);
            }
        }


        long expectedCardinality = numToMerge * cardinality;
        HyperLogLog hll = hyperLogLogs[0];
        hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLog[0]);
        long mergedEstimate = hll.merge(hyperLogLogs).cardinality();
        double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
    }

    @Test
    public void testPrecise_disableLongRangeCorrection() throws CardinalityMergeException
    {
        int cardinality = 150000000;

        HyperLogLog baseline = new HyperLogLog(20);
        for (int j = 0; j < cardinality; j++)
        {
            double val = Math.random();
            baseline.offer(val);
        }


        long mergedEstimate = baseline.cardinality(false);
        double se = cardinality * (1.04 / Math.sqrt(Math.pow(2, 20)));

        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (cardinality - (3 * se)) + " and " + (cardinality + (3 * se)));

        assertTrue(mergedEstimate >= cardinality - (3 * se));
        assertTrue(mergedEstimate <= cardinality + (3 * se));
    }
}
