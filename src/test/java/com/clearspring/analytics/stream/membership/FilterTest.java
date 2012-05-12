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
*/
package com.clearspring.analytics.stream.membership;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterTest
{
    public void testManyHashes(Iterator<String> keys)
    {
        int MAX_HASH_COUNT = 128;
        Set<Integer> hashes = new HashSet<Integer>();
        int collisions = 0;
        while (keys.hasNext())
        {
            hashes.clear();
            for (int hashIndex : Filter.getHashBuckets(keys.next(), MAX_HASH_COUNT, 1024 * 1024))
            {
                hashes.add(hashIndex);
            }
            collisions += (MAX_HASH_COUNT - hashes.size());
        }
        assertTrue("Collisions: "+collisions, collisions <= 100);
    }

    @Test
    public void testManyRandom()
    {
        testManyHashes(randomKeys());
    }

    // used by filter subclass tests

    static final double MAX_FAILURE_RATE = 0.1;
    public static final BloomCalculations.BloomSpecification spec = BloomCalculations.computeBucketsAndK(MAX_FAILURE_RATE);
    static final int ELEMENTS = 10000;

    static final ResetableIterator<String> intKeys()
    {
        return new KeyGenerator.IntGenerator(ELEMENTS);
    }

    static final ResetableIterator<String> randomKeys()
    {
        return new KeyGenerator.RandomStringGenerator(314159, ELEMENTS);
    }

    static final ResetableIterator<String> randomKeys2()
    {
        return new KeyGenerator.RandomStringGenerator(271828, ELEMENTS);
    }

    public static void testFalsePositives(Filter f, ResetableIterator<String> keys, ResetableIterator<String> otherkeys)
    {
        assertEquals(keys.size(), otherkeys.size());

        while (keys.hasNext())
        {
            f.add(keys.next());
        }

        int fp = 0;
        while (otherkeys.hasNext())
        {
            if (f.isPresent(otherkeys.next()))
            {
                fp++;
            }
        }

        double fp_ratio = fp / (keys.size() * BloomCalculations.probs[spec.bucketsPerElement][spec.K]);
        assertTrue("FP ratio: "+fp_ratio, fp_ratio < 1.03);
    }

}
