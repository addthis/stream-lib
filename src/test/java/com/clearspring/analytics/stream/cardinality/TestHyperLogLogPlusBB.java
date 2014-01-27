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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.clearspring.analytics.util.Varint;
import java.nio.ByteBuffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHyperLogLogPlusBB {

    public static void main(final String[] args) throws Throwable {
        long startTime = System.currentTimeMillis();

        int numSets = 10;
        int setSize = 1 * 1000 * 1000;
        int repeats = 5;

        HyperLogLogPlusBB[] counters = new HyperLogLogPlusBB[numSets];
        for (int i = 0; i < numSets; i++) {
            counters[i] = new HyperLogLogPlusBB(15, 15);
        }
        for (int i = 0; i < numSets; i++) {
            for (int j = 0; j < setSize; j++) {
                String val = UUID.randomUUID().toString();
                for (int z = 0; z < repeats; z++) {
                    counters[i].offer(val);
                }
            }
        }

        ICardinality merged = counters[0];
        long sum = merged.cardinality();
        for (int i = 1; i < numSets; i++) {
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
    public void testComputeCount() {
        HyperLogLogPlusBB hyperLogLogPlus = new HyperLogLogPlusBB(14, 25);
        int count = 70000;
        for (int i = 0; i < count; i++) {
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
        HyperLogLogPlusBB hyperLogLogPlus = new HyperLogLogPlusBB(14, 25);
        int count = 15000;
        int maxAttempts = 200;
        Random r = new Random();
        for (int i = 0; i < count; i++) {
            int n = r.nextInt(maxAttempts) + 1;
            for (int j = 0; j < n; j++) {
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
//        HyperLogLogPlusBB hll = new HyperLogLogPlusBB(14, 25);
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
    public void testSerialization_Normal() throws IOException {
        HyperLogLogPlusBB hll = new HyperLogLogPlusBB(5, 25);
        for (int i = 0; i < 100000; i++) {
            hll.offer("" + i);
        }
        System.out.println(hll.cardinality());

        ByteBuffer buff = ByteBuffer.allocate(hll.getBytes().length);
        buff.put(hll.getBytes());
        HyperLogLogPlusBB hll2 = HyperLogLogPlusBB.Builder.build(buff);
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testSerialization_Sparse() throws IOException {
        HyperLogLogPlusBB hll = new HyperLogLogPlusBB(14, 25);
        hll.offer("a");
        hll.offer("b");
        hll.offer("c");
        hll.offer("d");
        hll.offer("e");

        ByteBuffer buff = ByteBuffer.allocate(hll.getBytes().length);
        buff.put(hll.getBytes());

        HyperLogLogPlusBB hll2 = HyperLogLogPlusBB.Builder.build(buff);
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testHighCardinality() {
        long start = System.currentTimeMillis();
        HyperLogLogPlusBB hyperLogLogPlus = new HyperLogLogPlusBB(18, 25);
        int size = 10000000;
        for (int i = 0; i < size; i++) {
            hyperLogLogPlus.offer(TestICardinality.streamElement(i));
        }
        System.out.println("expected: " + size + ", estimate: " + hyperLogLogPlus.cardinality() + ", time: " + (System.currentTimeMillis() - start));
        long estimate = hyperLogLogPlus.cardinality();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println("Percentage error  " + err);
        assertTrue(err < .1);
    }

    @Test
    public void testSortEncodedSet() {
        int[] testSet = new int[3];
        testSet[0] = 655403;
        testSet[1] = 655416;
        testSet[2] = 655425;

        ByteBuffer buffer = ByteBuffer.allocate(testSet.length * 4);
        for (int i = 0; i < buffer.capacity() / 4; i++) {
            buffer.putInt(i * 4, testSet[i]);
        }

        HyperLogLogPlusBB hyperLogLogPlus = new HyperLogLogPlusBB(14, 25);
        buffer = hyperLogLogPlus.sortEncodedSet(buffer, 3);
        assertEquals(655403, buffer.getInt(0));
        assertEquals(655425, buffer.getInt(4));
        assertEquals(655416, buffer.getInt(8));

    }

    @Test
    public void testMergeSelf_forceNormal() throws CardinalityMergeException, IOException {
        final int[] cardinalities = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
        for (int cardinality : cardinalities) {
            for (int j = 4; j < 24; j++) {
                System.out.println("p=" + j);
                HyperLogLogPlusBB hllPlus = new HyperLogLogPlusBB(j, 0);
                for (int l = 0; l < cardinality; l++) {
                    hllPlus.offer(Math.random());
                }
                System.out.println("hllcardinality=" + hllPlus.cardinality() + " cardinality=" + cardinality);

                ByteBuffer buff = ByteBuffer.allocate(hllPlus.getBytes().length);
                buff.put(hllPlus.getBytes());

                HyperLogLogPlusBB deserialized = HyperLogLogPlusBB.Builder.build(buff);
                assertEquals(hllPlus.cardinality(), deserialized.cardinality());
                ICardinality merged = hllPlus.merge(deserialized);
                System.out.println(merged.cardinality() + " : " + hllPlus.cardinality());
                assertEquals(hllPlus.cardinality(), merged.cardinality());
            }
        }
    }

    @Test
    public void testMergeSelf() throws CardinalityMergeException, IOException {
        final int[] cardinalities = {0, 1, 10, 100, 1000, 10000, 100000};
        final int[] ps = {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
        final int[] sps = {16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

        for (int cardinality : cardinalities) {
            for (int j = 0; j < ps.length; j++) {
                for (int sp : sps) {
                    if (sp < ps[j]) {
                        continue;
                    }
                    System.out.println(ps[j] + "-" + sp + ": " + cardinality);
                    HyperLogLogPlusBB hllPlus = new HyperLogLogPlusBB(ps[j], sp);
                    for (int l = 0; l < cardinality; l++) {
                        hllPlus.offer(Math.random());
                    }

                    ByteBuffer buff = ByteBuffer.allocate(hllPlus.getBytes().length*4);
                    buff.put(hllPlus.getBytes());

                    HyperLogLogPlusBB deserialized = HyperLogLogPlusBB.Builder.build(buff);
                    assertEquals(hllPlus.cardinality(), deserialized.cardinality());
                    ICardinality merged = hllPlus.merge(deserialized);
                    assertEquals(hllPlus.cardinality(), merged.cardinality());
                }
            }
        }

    }

    @Test
    public void testOne() throws IOException {
        HyperLogLogPlusBB one = new HyperLogLogPlusBB(8, 25);
        one.offer("a");
        assertEquals(1, one.cardinality());
    }

    @Test
    public void testSparseSpace() throws IOException {
        HyperLogLogPlusBB hllp = new HyperLogLogPlusBB(14, 14);
        for (int i = 0; i < 10000; i++) {
            hllp.offer(i);
        }
        System.out.println("Size: " + hllp.getBytes().length);
    }

    @Test
    public void testMerge_Sparse() throws CardinalityMergeException {
        int numToMerge = 4;
        int bits = 18;
        int cardinality = 4000;

        HyperLogLogPlusBB[] hyperLogLogs = new HyperLogLogPlusBB[numToMerge];
        HyperLogLogPlusBB baseline = new HyperLogLogPlusBB(bits, 25);
        for (int i = 0; i < numToMerge; i++) {
            hyperLogLogs[i] = new HyperLogLogPlusBB(bits, 25);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                hyperLogLogs[i].offer(val);
                baseline.offer(val);
            }
        }

        long expectedCardinality = numToMerge * cardinality;
        HyperLogLogPlusBB hll = hyperLogLogs[0];
        hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLogPlusBB[0]);
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
    public void testMerge_Normal() throws CardinalityMergeException {
        int numToMerge = 4;
        int bits = 18;
        int cardinality = 5000;

        HyperLogLogPlusBB[] hyperLogLogs = new HyperLogLogPlusBB[numToMerge];
        HyperLogLogPlusBB baseline = new HyperLogLogPlusBB(bits, 25);
        for (int i = 0; i < numToMerge; i++) {
            hyperLogLogs[i] = new HyperLogLogPlusBB(bits, 25);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                hyperLogLogs[i].offer(val);
                baseline.offer(val);
            }
        }

        long expectedCardinality = numToMerge * cardinality;
        HyperLogLogPlusBB hll = hyperLogLogs[0];
        hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLogPlusBB[0]);
        long mergedEstimate = hll.merge(hyperLogLogs).cardinality();
        double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
    }

    @Test
    public void testLegacyCodec_normal() throws IOException {
        int bits = 18;
        int cardinality = 1000000;

        HyperLogLogPlusBB baseline = new HyperLogLogPlusBB(bits, 25);
        for (int j = 0; j < cardinality; j++) {
            double val = Math.random();
            baseline.offer(val);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(bits);
        dos.writeInt(25);
        dos.writeInt(0);
        dos.writeInt(baseline.getRegisterSet().size * 4);
        for (int x = 0; x < baseline.getRegisterSet().readOnlyBits().capacity() / 4; x++) {
            int i = baseline.getRegisterSet().readOnlyBits().getInt(x * 4);
            dos.writeInt(i);
        }

        byte[] legacyBytes = baos.toByteArray();

        ByteBuffer buff = ByteBuffer.allocate(legacyBytes.length);
        buff.put(legacyBytes);
        // decode legacy
        HyperLogLogPlusBB decoded = HyperLogLogPlusBB.Builder.build(buff);
        assertEquals(baseline.cardinality(), decoded.cardinality());
        byte[] newBytes = baseline.getBytes();
        assertTrue(newBytes.length < legacyBytes.length);

    }

    @Test
    public void testLegacyCodec_sparse() throws IOException {
        int bits = 18;
        int cardinality = 5000;

        HyperLogLogPlusBB baseline = new HyperLogLogPlusBB(bits, 25);
        for (int j = 0; j < cardinality; j++) {
            double val = Math.random();
            baseline.offer(val);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(bits);
        dos.writeInt(25);
        dos.writeInt(1);
        baseline.mergeTempList();
        ByteBuffer sparseSet = baseline.getSparseSet();
        List<byte[]> sparseBytes = new ArrayList<byte[]>(sparseSet.capacity() / 4);

        int prevDelta = 0;
        for (int i = 0; i < sparseSet.capacity() / 4; i++) {
            int k = sparseSet.getInt(i * 4);
            sparseBytes.add(Varint.writeUnsignedVarInt(k - prevDelta));
            prevDelta = k;
        }
        for (byte[] bytes : sparseBytes) {
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
        dos.writeInt(-1);

        byte[] legacyBytes = baos.toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(legacyBytes.length);
        buffer.put(legacyBytes);

        //  decode legacy
        HyperLogLogPlusBB decoded = HyperLogLogPlusBB.Builder.build(buffer);
        assertEquals(baseline.cardinality(), decoded.cardinality());
        byte[] newBytes = baseline.getBytes();
        assertTrue(newBytes.length < legacyBytes.length);

    }

    @Test
    public void testMerge_ManySparse() throws CardinalityMergeException {
        int numToMerge = 20;
        int bits = 18;
        int cardinality = 10000;

        HyperLogLogPlusBB[] hyperLogLogs = new HyperLogLogPlusBB[numToMerge];
        HyperLogLogPlusBB baseline = new HyperLogLogPlusBB(bits, 25);
        for (int i = 0; i < numToMerge; i++) {
            hyperLogLogs[i] = new HyperLogLogPlusBB(bits, 25);
            for (int j = 0; j < cardinality; j++) {
                double val = Math.random();
                hyperLogLogs[i].offer(val);
                baseline.offer(val);
            }
        }

        long expectedCardinality = numToMerge * cardinality;
        HyperLogLogPlusBB hll = hyperLogLogs[0];
        hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLogPlusBB[0]);
        long mergedEstimate = hll.merge(hyperLogLogs).cardinality();
        double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

        System.out.println("Expect estimate: " + mergedEstimate + " is between " + (expectedCardinality - (3 * se)) + " and " + (expectedCardinality + (3 * se)));

        assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
        assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
    }

    @Test
    public void testMerge_SparseIntersection() throws CardinalityMergeException {
        HyperLogLogPlusBB a = new HyperLogLogPlusBB(11, 16);
        HyperLogLogPlusBB b = new HyperLogLogPlusBB(11, 16);

        // Note that only one element, 41, is shared amongst the two sets,
        // and so the number of total unique elements is 14.
        int[] aInput = {12, 13, 22, 34, 38, 40, 41, 46, 49};
        int[] bInput = {2, 6, 19, 29, 41, 48};

        Set<Integer> testSet = new HashSet<Integer>();
        for (Integer in : aInput) {
            testSet.add(in);
            a.offer(in);
        }

        for (Integer in : bInput) {
            testSet.add(in);
            b.offer(in);
        }

        assertEquals(14, testSet.size());
        assertEquals(9, a.cardinality());
        assertEquals(6, b.cardinality());

        a.addAll(b);
        assertEquals(14, a.cardinality());
    }
}
