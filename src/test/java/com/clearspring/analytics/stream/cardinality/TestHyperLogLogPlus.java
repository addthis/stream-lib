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

public class TestHyperLogLogPlus
{
    @Test
    public void testComputeCount()
    {
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
        hyperLogLogPlus.offer(0);
        hyperLogLogPlus.offer(1);
        hyperLogLogPlus.offer(2);
        hyperLogLogPlus.offer(3);
        hyperLogLogPlus.offer(16);
        hyperLogLogPlus.offer(17);
        hyperLogLogPlus.offer(18);
        hyperLogLogPlus.offer(19);
        hyperLogLogPlus.offer(19);
        assertEquals(8, hyperLogLogPlus.cardinality());
    }

    @Test
    public void testSerialization_Normal() throws IOException
    {
        HyperLogLogPlus hll = new HyperLogLogPlus(5, 25);
		for (int i = 0; i < 100000; i++)
		{
			hll.offer("" + i);
		}
		System.out.println(hll.cardinality());
		HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

	@Test
	public void testSerialization_Sparse() throws IOException
	{
		HyperLogLogPlus hll = new HyperLogLogPlus(14, 25);
		hll.offer("a");
		hll.offer("b");
		hll.offer("c");
		hll.offer("d");
		hll.offer("e");

		HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(hll.getBytes());
		assertEquals(hll.cardinality(), hll2.cardinality());
	}

    @Test
    public void testHighCardinality()
    {
        long start = System.currentTimeMillis();
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(18, 25);
		int size = 10000000;
        for (int i = 0; i < size; i++)
        {
            hyperLogLogPlus.offer(TestICardinality.streamElement(i));
		}
        System.out.println("expected: " + size + ", estimate: " + hyperLogLogPlus.cardinality() + ", time: " + (System.currentTimeMillis() - start));
		long estimate = hyperLogLogPlus.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println(err);
        assertTrue(err < .1);
    }

    @Test
    public void testMerge_Sparse() throws CardinalityMergeException
    {
        int numToMerge = 4;
        int bits = 18;
        int cardinality = 1000000;

        HyperLogLogPlus[] hyperLogLogs = new HyperLogLogPlus[numToMerge];
		HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
        for (int i = 0; i < numToMerge; i++)
        {
            hyperLogLogs[i] = new HyperLogLogPlus(bits, 25);
            for (int j = 0; j < cardinality; j++)
            {
                double val = Math.random();
                hyperLogLogs[i].offer(val);
                baseline.offer(val);
            }
        }


        long expectedCardinality = numToMerge * cardinality;
		HyperLogLogPlus hll = hyperLogLogs[0];
        hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLogPlus[0]);
        long mergedEstimate = hll.merge(hyperLogLogs).cardinality();
        double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
    }

	@Test
	public void testMerge_Normal() throws CardinalityMergeException
	{
		int numToMerge = 4;
		int bits = 18;
		int cardinality = 5000;

		HyperLogLogPlus[] hyperLogLogs = new HyperLogLogPlus[numToMerge];
		HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
		for (int i = 0; i < numToMerge; i++)
		{
			hyperLogLogs[i] = new HyperLogLogPlus(bits, 25);
			for (int j = 0; j < cardinality; j++)
			{
				double val = Math.random();
				hyperLogLogs[i].offer(val);
				baseline.offer(val);
			}
		}


		long expectedCardinality = numToMerge * cardinality;
		HyperLogLogPlus hll = hyperLogLogs[0];
		hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLogPlus[0]);
		long mergedEstimate = hll.merge(hyperLogLogs).cardinality();
		double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

		System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

		assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
		assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
	}
}
