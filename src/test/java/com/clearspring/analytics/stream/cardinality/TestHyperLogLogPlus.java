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

import com.clearspring.analytics.TestUtils;
import com.clearspring.analytics.util.Varint;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class TestHyperLogLogPlus {
    private static final Logger log = LoggerFactory.getLogger(TestHyperLogLogPlus.class);

    @Test
    public void testEquals() {
        HyperLogLogPlus hll1 = new HyperLogLogPlus(5, 25);
        HyperLogLogPlus hll2 = new HyperLogLogPlus(5, 25);
        hll1.offer("A");
        hll2.offer("A");
        assertEquals(hll1, hll2);
        hll2.offer("B");
        hll2.offer("C");
        hll2.offer("D");
        assertNotEquals(hll1, hll2);
        HyperLogLogPlus hll3 = new HyperLogLogPlus(5, 25);
        for (int i = 0; i < 50000; i++) {
            hll3.offer("" + i);
        }
        assertNotEquals(hll1, hll3);
    }
    
    @Test
    public void consistentBytes() throws Throwable {
        int[] NUM_STRINGS = {30, 50, 100, 200, 300, 500, 1000, 10000, 100000};
        for (int n : NUM_STRINGS) {

            String[] strings = new String[n];

            for (int i = 0; i < n; i++) {
                strings[i] = RandomStringUtils.randomAlphabetic(20);
            }

            HyperLogLogPlus hllpp1 = new HyperLogLogPlus(5, 5);
            HyperLogLogPlus hllpp2 = new HyperLogLogPlus(5, 5);
            for (int i = 0; i < n; i++) {
                hllpp1.offer(strings[i]);
                hllpp2.offer(strings[n - 1 - i]);
            }
            // calling these here ensures their internal state (format type) is stable for the rest of these checks.
            // (end users have no need for this because they cannot access the format directly anyway)
            hllpp1.mergeTempList();
            hllpp2.mergeTempList();
            log.debug("n={} format1={} format2={}", n, hllpp1.format, hllpp2.format);
            try {
                if (hllpp1.format == hllpp2.format) {
                    assertEquals(hllpp1, hllpp2);
                    assertEquals(hllpp1.hashCode(), hllpp2.hashCode());
                    assertArrayEquals(hllpp1.getBytes(), hllpp2.getBytes());
                } else {
                    assertNotEquals(hllpp1, hllpp2);
                }
            } catch (Throwable any) {
                log.error("n={} format1={} format2={}", n, hllpp1.format, hllpp2.format, any);
                throw any;
            }
        }
    }

    public static void main(final String[] args) throws Throwable {
        long startTime = System.currentTimeMillis();

        int numSets = 10;
        int setSize = 1 * 1000 * 1000;
        int repeats = 5;

        HyperLogLogPlus[] counters = new HyperLogLogPlus[numSets];
        for (int i = 0; i < numSets; i++) {
            counters[i] = new HyperLogLogPlus(15, 15);
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
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
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
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
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

    @Test
    public void testSerialization_Normal() throws IOException {
        HyperLogLogPlus hll = new HyperLogLogPlus(5, 25);
        for (int i = 0; i < 100000; i++) {
            hll.offer("" + i);
        }
        System.out.println(hll.cardinality());
        HyperLogLogPlus hll2 = HyperLogLogPlus.Builder.build(hll.getBytes());
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        HyperLogLogPlus hll = new HyperLogLogPlus(5, 25);
        for (int i = 0; i < 100000; i++) {
            hll.offer("" + i);
        }
        System.out.println(hll.cardinality());
        HyperLogLogPlus hll2 = (HyperLogLogPlus) TestUtils.deserialize(TestUtils.serialize(hll));
        assertEquals(hll.cardinality(), hll2.cardinality());
    }

    @Test
    public void testSerialization_Sparse() throws IOException {
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
    public void testHighPrecisionInitialization() {
        for (int sp = 4; sp <= 32; sp++) {
            int expectedSm = (int)Math.pow(2, sp);

            for (int p = 4; p <= sp; p++) {
                int expectedM = (int)Math.pow(2, p);

                HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(p, sp);
                assertEquals(expectedM, hyperLogLogPlus.getM());
                assertEquals(expectedSm, hyperLogLogPlus.getSm());
            }
        }
    }

    @Test
    public void testHighCardinality() {
        long start = System.currentTimeMillis();
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(18, 25);
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
        HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14, 25);
        testSet = hyperLogLogPlus.sortEncodedSet(testSet, 3);
        assertEquals(655403, testSet[0]);
        assertEquals(655425, testSet[1]);
        assertEquals(655416, testSet[2]);

    }

    @Test
    public void testMergeSelf_forceNormal() throws CardinalityMergeException, IOException {
        final int[] cardinalities = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
        for (int cardinality : cardinalities) {
            for (int j = 4; j < 24; j++) {
                System.out.println("p=" + j);
                HyperLogLogPlus hllPlus = new HyperLogLogPlus(j, 0);
                for (int l = 0; l < cardinality; l++) {
                    hllPlus.offer(Math.random());
                }
                System.out.println("hllcardinality=" + hllPlus.cardinality() + " cardinality=" + cardinality);
                HyperLogLogPlus deserialized = HyperLogLogPlus.Builder.build(hllPlus.getBytes());
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
                    HyperLogLogPlus hllPlus = new HyperLogLogPlus(ps[j], sp);
                    for (int l = 0; l < cardinality; l++) {
                        hllPlus.offer(Math.random());
                    }
                    HyperLogLogPlus deserialized = HyperLogLogPlus.Builder.build(hllPlus.getBytes());
                    System.out.println(ps[j] + "-" + sp + ": " + cardinality + " -> " + hllPlus.cardinality());
                    assertEquals(hllPlus.cardinality(), deserialized.cardinality());
                    ICardinality merged = hllPlus.merge(deserialized);
                    assertEquals(hllPlus.cardinality(), merged.cardinality());
                }
            }
        }

    }

    @Test
    public void testOne() throws IOException {
        HyperLogLogPlus one = new HyperLogLogPlus(8, 25);
        one.offer("a");
        assertEquals(1, one.cardinality());
    }

    @Test
    public void testSparseSpace() throws IOException {
        HyperLogLogPlus hllp = new HyperLogLogPlus(14, 14);
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

        HyperLogLogPlus[] hyperLogLogs = new HyperLogLogPlus[numToMerge];
        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
        for (int i = 0; i < numToMerge; i++) {
            hyperLogLogs[i] = new HyperLogLogPlus(bits, 25);
            for (int j = 0; j < cardinality; j++) {
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
    public void testMerge_Normal() throws CardinalityMergeException {
        int numToMerge = 4;
        int bits = 18;
        int cardinality = 5000;

        HyperLogLogPlus[] hyperLogLogs = new HyperLogLogPlus[numToMerge];
        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
        for (int i = 0; i < numToMerge; i++) {
            hyperLogLogs[i] = new HyperLogLogPlus(bits, 25);
            for (int j = 0; j < cardinality; j++) {
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
    public void testLegacyCodec_normal() throws IOException {
        int bits = 18;
        int cardinality = 1000000;

        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
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
        for (int x : baseline.getRegisterSet().readOnlyBits()) {
            dos.writeInt(x);
        }

        dos.close();
        byte[] legacyBytes = baos.toByteArray();

        // decode legacy
        HyperLogLogPlus decoded = HyperLogLogPlus.Builder.build(legacyBytes);
        assertEquals(baseline.cardinality(), decoded.cardinality());
        byte[] newBytes = baseline.getBytes();
        assertTrue(newBytes.length < legacyBytes.length);

    }

    @Test
    public void testLegacyCodec_sparse() throws IOException {
        int bits = 18;
        int cardinality = 5000;

        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
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
        int[] sparseSet = baseline.getSparseSet();
        List<byte[]> sparseBytes = new ArrayList<byte[]>(sparseSet.length);

        int prevDelta = 0;
        for (int k : sparseSet) {
            sparseBytes.add(Varint.writeUnsignedVarInt(k - prevDelta));
            prevDelta = k;
        }
        for (byte[] bytes : sparseBytes) {
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
        dos.writeInt(-1);
        dos.close();

        byte[] legacyBytes = baos.toByteArray();

        //  decode legacy
        HyperLogLogPlus decoded = HyperLogLogPlus.Builder.build(legacyBytes);
        assertEquals(baseline.cardinality(), decoded.cardinality());
        byte[] newBytes = baseline.getBytes();
        assertTrue(newBytes.length < legacyBytes.length);

    }

    @Test
    public void testMerge_ManySparse() throws CardinalityMergeException {
        int numToMerge = 20;
        int bits = 18;
        int cardinality = 10000;

        HyperLogLogPlus[] hyperLogLogs = new HyperLogLogPlus[numToMerge];
        HyperLogLogPlus baseline = new HyperLogLogPlus(bits, 25);
        for (int i = 0; i < numToMerge; i++) {
            hyperLogLogs[i] = new HyperLogLogPlus(bits, 25);
            for (int j = 0; j < cardinality; j++) {
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
    public void testMerge_SparseIntersection() throws CardinalityMergeException {
        HyperLogLogPlus a = new HyperLogLogPlus(11, 16);
        HyperLogLogPlus b = new HyperLogLogPlus(11, 16);

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
    
    @Test
    public void testSerializationWithNewSortMethod() throws IOException {
        HyperLogLogPlus hll = new HyperLogLogPlus(14, 25);
        hll.offerHashed(0x0000000000000000l);
        hll.offerHashed(0x7FFFFFFFFFFFFFFFl);
        hll.offerHashed(0x8000000000000000l);
        hll.offerHashed(0xFFFFFFFFFFFFFFFFl);

        // test against old serialization
        assertArrayEquals(new byte[]{-1, -1, -1, -2, 14, 25, 1, 4, 25, -27, -1, -1, 15, -101, -128, -128, -16, 7, -27, -1, -1, -97, 8}, hll.getBytes());
    }
}
