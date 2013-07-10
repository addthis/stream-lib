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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestHyperLogLogPlus
{

    public static void main(final String[] args) throws Throwable
    {
        long startTime = System.currentTimeMillis();

        int numSets = 10;
        int setSize = 1 * 1000 * 1000;
        int repeats = 5;

        HyperLogLogPlus[] counters = new HyperLogLogPlus[numSets];
        for (int i = 0; i < numSets; i++)
        {
            counters[i] = new HyperLogLogPlus(15, 15);
        }
        for (int i = 0; i < numSets; i++)
        {
            for (int j = 0; j < setSize; j++)
            {
                String val = UUID.randomUUID().toString();
                for (int z = 0; z < repeats; z++)
                {
                    counters[i].offer(val);
                }
            }
        }

        ICardinality merged = counters[0];
        long sum = merged.cardinality();
        for (int i = 1; i < numSets; i++)
        {
            sum += counters[i].cardinality();
            merged = merged.merge(counters[i]);
        }

        long trueSize = numSets * setSize;
        System.out.println("True Cardinality: " + trueSize);
        System.out.println("Summed Cardinality: " + sum);
        System.out.println("Merged Cardinality: " + merged.cardinality());
        System.out.println("Merged Error: " + (merged.cardinality() - trueSize) / (float) trueSize);
        System.out.println("Duration: " + ((System.currentTimeMillis() - startTime) / 1000) + "s");
    }

    @Test
    public void testComputeCount()
    {
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
        int count = 70000;
        for (int i = 0; i < count; i++)
        {
            hyperLogLogPlus.offer("i" + i);
        }
        long estimate = hyperLogLogPlus.cardinality();
        double se = count * (1.04 / Math.sqrt(Math.pow(2, 14)));
        long expectedCardinality = count;

        System.out.println("Expect estimate: " + estimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(estimate >= expectedCardinality - (3 * se));
        assertTrue(estimate <= expectedCardinality + (3 * se));
    }

	@Test
	public void testSmallCardinalityRepeatedInsert() {
		HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
		int count = 15000;
		int maxAttempts = 200;
		Random r = new Random();
		for (int i = 0; i < count; i++)
		{
			int n = r.nextInt(maxAttempts) + 1;
			for (int j = 0; j < n ; j++)
			{
				hyperLogLogPlus.offer("i" + i);
			}
		}
		long estimate = hyperLogLogPlus.cardinality();
		double se = count * (1.04 / Math.sqrt(Math.pow(2, 14)));
		long expectedCardinality = count;

		System.out.println("Expect estimate: " + estimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

		assertTrue(estimate >= expectedCardinality - (3 * se));
		assertTrue(estimate <= expectedCardinality + (3 * se));
	}

//    @Test
//    public void testDelta()
//    {
//        HyperLogLogPlus hll = new HyperLogLogPlus(14, 25);
//        ArrayList<byte[]> l = new ArrayList<byte[]>();
//        for (int i = 0; i < 1000000; i++)
//        {
//            hll.deltaAdd(l,i);
//            int out = hll.deltaRead(l,i);
//            assert i == out;
//            int out2 = hll.deltaRead(l,i);
//            assert i == out2;
//        }
//    }

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
        System.out.println("Percentage error  " + err);
        assertTrue(err < .1);
    }

    @Test
    public void testSortEncodedSet()
    {
        List<Integer> testSet = new ArrayList<Integer>();
        testSet.add(655403);
        testSet.add(655416);
        testSet.add(655425);
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
        hyperLogLogPlus.sortEncodedSet(testSet);
        assertEquals(new Integer(655403), testSet.get(0));
        assertEquals(new Integer(655425), testSet.get(1));
        assertEquals(new Integer(655416), testSet.get(2));

    }

    @Test
    public void testMergeSelf() throws CardinalityMergeException, IOException
    {
        final int[] cardinalities = { 0, 1, 10, 100, 1000, 10000, 100000 };
        final int[] ps = { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
        final int[] sps = { 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 };

        for (int cardinality : cardinalities)
        {
            for (int j = 0; j < ps.length; j++)
            {
                for (int sp : sps)
                {
                    if (sp < ps[j])
                    {
                        continue;
                    }
                    System.out.println(ps[j] + "-" + sp);
                    HyperLogLogPlus hllPlus = new HyperLogLogPlus(ps[j], sp);
                    for (int l = 0; l < cardinality; l++)
                    {
                        hllPlus.offer(Math.random());
                    }
                    HyperLogLogPlus deserialized = HyperLogLogPlus.Builder.build(hllPlus.getBytes());
                    assertEquals(hllPlus.cardinality(), deserialized.cardinality());
                    ICardinality merged = hllPlus.merge(deserialized);
                    assertEquals(hllPlus.cardinality(), merged.cardinality());
                }
            }
        }

    }

    @Test
    public void testMerge_Sparse() throws CardinalityMergeException
    {
        int numToMerge = 4;
        int bits = 18;
        int cardinality = 4000;

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
        double err = Math.abs(mergedEstimate - expectedCardinality) / (double) expectedCardinality;
        System.out.println("Percentage error  " + err);
        assertTrue(err < .1);

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

    @Test
    public void testLegacyCodec_normal() throws IOException
    {
        int bits = 18;
        int cardinality = 1000000;

        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
        for (int j = 0; j < cardinality; j++)
        {
            double val = Math.random();
            baseline.offer(val);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(bits);
        dos.writeInt(25);
        dos.writeInt(0);
        dos.writeInt(baseline.getRegisterSet().size * 4);
        for (int x : baseline.getRegisterSet().bits())
        {
            dos.writeInt(x);
        }

        byte[] legacyBytes = baos.toByteArray();

        // decode legacy
        HyperLogLogPlus decoded = HyperLogLogPlus.Builder.build(legacyBytes);
        assertEquals(baseline.cardinality(), decoded.cardinality());
        byte[] newBytes = baseline.getBytes();
        assertTrue(newBytes.length < legacyBytes.length);

    }

    @Test
    public void testLegacyCodec_sparse() throws IOException
    {
        int bits = 18;
        int cardinality = 5000;

        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
        for (int j = 0; j < cardinality; j++)
        {
            double val = Math.random();
            baseline.offer(val);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(bits);
        dos.writeInt(25);
        dos.writeInt(1);
        baseline.mergeTempList();
        for (byte[] bytes : baseline.getSparseSet())
        {
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
        dos.writeInt(-1);

        byte[] legacyBytes = baos.toByteArray();

        // decode legacy
        HyperLogLogPlus decoded = HyperLogLogPlus.Builder.build(legacyBytes);
        assertEquals(baseline.cardinality(), decoded.cardinality());
        byte[] newBytes = baseline.getBytes();
        assertTrue(newBytes.length < legacyBytes.length);

    }

    @Test
    public void testMerge_ManySparse() throws CardinalityMergeException
    {
        int numToMerge = 20;
        int bits = 18;
        int cardinality = 10000;

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
