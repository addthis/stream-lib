/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
* 
*/
package com.clearspring.analytics.stream.membership;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.clearspring.analytics.stream.membership.KeyGenerator.RandomStringGenerator;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class BloomFilterTest {

    public BloomFilter bf;
    public BloomFilter bf2;
    public BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(0.0001);
    static final int ELEMENTS = 10000;

    public BloomFilterTest() {
        bf = new BloomFilter(ELEMENTS, spec.bucketsPerElement);
        bf2 = new BloomFilter(ELEMENTS, spec.bucketsPerElement);
        assertNotNull(bf);
    }

    @Before
    public void clear() {
        bf.clear();
    }

    @Test
    public void testOne() {
        bf.add("a");
        assertTrue(bf.isPresent("a"));
        assertFalse(bf.isPresent("b"));
    }

    @Test
    public void testMerge() {
        bf.add("a");
        bf2.add("c");
        BloomFilter[] bfs = new BloomFilter[1];
        bfs[0] = bf;
        BloomFilter mergeBf = (BloomFilter) bf2.merge(bf);
        assertTrue(mergeBf.isPresent("a"));
        assertFalse(mergeBf.isPresent("b"));
        assertTrue(mergeBf.isPresent("c"));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMergeException() {
        BloomFilter bf3 = new BloomFilter(ELEMENTS*10, 1);
        BloomFilter[] bfs = new BloomFilter[1];
        bfs[0] = bf;
        BloomFilter mergeBf = (BloomFilter) bf3.merge(bf);
    }

    @Test
    public void testFalsePositivesInt() {
        FilterTest.testFalsePositives(bf, FilterTest.intKeys(), FilterTest.randomKeys2());
    }

    @Test
    public void testFalsePositivesRandom() {
        FilterTest.testFalsePositives(bf, FilterTest.randomKeys(), FilterTest.randomKeys2());
    }

    @Test
    public void testWords() {
        if (KeyGenerator.WordGenerator.WORDS == 0) {
            return;
        }
        BloomFilter bf2 = new BloomFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTest.spec.bucketsPerElement);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTest.testFalsePositives(bf2,
                new KeyGenerator.WordGenerator(skipEven, 2),
                new KeyGenerator.WordGenerator(1, 2));
    }

    @Test
    public void testSerialize() throws IOException {
        FilterTest.testSerialize(bf);
    }

    @Test
    public void testGetFalsePositiveProbability() {
        // These probabilities are taken from the bloom filter probability table at
        // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
        System.out.println("expectedFalsePositiveProbability");
        BloomFilter instance = new BloomFilter(100, 10);
        double expResult = 0.00819; // m/n=10, k=7
        double result = BloomCalculations.getFalsePositiveProbability(10, 7);
        assertEquals(7, instance.getHashCount());
        assertEquals(expResult, result, 0.000009);

        instance = new BloomFilter(10, 10);
        expResult = 0.00819; // m/n=10, k=7
        result = BloomCalculations.getFalsePositiveProbability(10, 7);
        assertEquals(7, instance.getHashCount());
        assertEquals(expResult, result, 0.000009);

        instance = new BloomFilter(10, 2);
        expResult = 0.393; // m/n=2, k=1
        result = BloomCalculations.getFalsePositiveProbability(2, 1);
        assertEquals(instance.getHashCount(), 1);
        assertEquals(expResult, result, 0.0005);

        instance = new BloomFilter(10, 11);
        expResult = 0.00509; // m/n=11, k=8
        result = BloomCalculations.getFalsePositiveProbability(11, 8);
        assertEquals(8, instance.getHashCount());
        assertEquals(expResult, result, 0.00001);
    }

    /**
     * Test error rate
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testFalsePositiveRate() throws UnsupportedEncodingException {
        // Numbers are from // http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html

        for (int j = 10; j < 21; j++) {
            System.out.print(j - 9 + "/11");
            Set<String> v = new HashSet<String>();
            BloomFilter instance = new BloomFilter(100, j);

            for (int i = 0; i < 100; i++) {
                String key = UUID.randomUUID().toString();
                v.add(key);
                instance.add(key);
            }

            long r = 0;
            double tests = 100000;
            for (int i = 0; i < tests; i++) {
                String s = UUID.randomUUID().toString();
                if (instance.isPresent(s)) {
                    if (!v.contains(s)) {
                        r++;
                    }
                }
            }

            double ratio = r / tests;
            double expectedFalsePositiveProbability = j < BloomCalculations.probs.length ? BloomCalculations.probs[j][instance.getHashCount()] : BloomCalculations.getFalsePositiveProbability(j, instance.getHashCount());
            System.out.println(" - got " + ratio + ", math says " + expectedFalsePositiveProbability);
            assertEquals(expectedFalsePositiveProbability, ratio, 0.01);
        }
    }

    /**
     * Test for correct k *
     */
    @Test
    public void testHashCount() {
        // Numbers are from http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
        BloomFilter instance = null;

        instance = new BloomFilter(1, 2);
        assertEquals(1, instance.getHashCount());

        instance = new BloomFilter(1, 3);
        assertEquals(2, instance.getHashCount());

        instance = new BloomFilter(1, 4);
        assertEquals(3, instance.getHashCount());

        instance = new BloomFilter(1, 5);
        assertEquals(3, instance.getHashCount());

        instance = new BloomFilter(1, 6);
        assertEquals(4, instance.getHashCount());

        instance = new BloomFilter(1, 7);
        assertEquals(5, instance.getHashCount());
        
        /*
         * Although technically 8*ln(2) = 5.545...
         * we round down here for speed
         */
        instance = new BloomFilter(1, 8);
        assertEquals(5, instance.getHashCount());

        instance = new BloomFilter(1, 9);
        assertEquals(6, instance.getHashCount());

        instance = new BloomFilter(1, 10);
        assertEquals(7, instance.getHashCount());

        instance = new BloomFilter(1, 11);
        assertEquals(8, instance.getHashCount());

        instance = new BloomFilter(1, 12);
        assertEquals(8, instance.getHashCount());
    }

    @Test
    public void testSizing() throws IOException {
        BloomFilter f = null;

        assertEquals(128, (f = new BloomFilter(10, 0.05)).buckets());
        assertEquals(93, serialize(f).length);

        assertEquals(768, new BloomFilter(100, 0.05).buckets());
        assertEquals(7040, new BloomFilter(1000, 0.05).buckets());
        assertEquals(70080, new BloomFilter(10000, 0.05).buckets());
        assertEquals(700032, new BloomFilter(100000, 0.05).buckets());
        assertEquals(7000064, new BloomFilter(1000000, 0.05).buckets());

        assertEquals(128, new BloomFilter(10, 0.01).buckets());
        assertEquals(1024, new BloomFilter(100, 0.01).buckets());
        assertEquals(10048, (f = new BloomFilter(1000, 0.01)).buckets());
        assertEquals(1333, serialize(f).length);

        assertEquals(100032, (f = new BloomFilter(10000, 0.01)).buckets());
        assertEquals(12581, serialize(f).length);

        assertEquals(1000064, (f = new BloomFilter(100000, 0.01)).buckets());
        assertEquals(125085, serialize(f).length);

        assertEquals(10000064, (f = new BloomFilter(1000000, 0.01)).buckets());
        assertEquals(1250085, serialize(f).length);

        for (String s : new RandomStringGenerator(new Random().nextInt(), 1000000)) {
            f.add(s);
        }
        assertEquals(10000064, f.buckets());
        assertEquals(1250085, serialize(f).length);

    }

    @Ignore
    @Test
    public void timeSerialize() throws IOException {
        for (int i = 0; i < 1000; i++) {
            BloomFilter f = new BloomFilter(1000000, 0.01);
            serialize(f);
        }
    }

    private byte[] serialize(BloomFilter f) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        f.getSerializer().serialize(f, out);
        out.close();
        return out.getData();
    }

    /* TODO move these into a nightly suite (they take 5-10 minutes each) */
    @Ignore
    @Test
    // run with -mx1G
    public void testBigInt() {
        int size = 100 * 1000 * 1000;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                new KeyGenerator.IntGenerator(size),
                new KeyGenerator.IntGenerator(size, size * 2));
    }

    @Ignore
    @Test
    public void testBigRandom() {
        int size = 100 * 1000 * 1000;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        FilterTest.testFalsePositives(bf,
                new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
    }

    @Ignore
    @Test
    public void timeit() {
        int size = 300 * FilterTest.ELEMENTS;
        bf = new BloomFilter(size, FilterTest.spec.bucketsPerElement);
        for (int i = 0; i < 10; i++) {
            FilterTest.testFalsePositives(bf,
                    new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size),
                    new KeyGenerator.RandomStringGenerator(new Random().nextInt(), size));
            bf.clear();
        }
    }

}
