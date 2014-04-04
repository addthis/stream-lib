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

package com.clearspring.analytics.stream;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestConcurrentStreamSummary {

    private static final int NUM_ITERATIONS = 100000;

    @Test
    public void testStreamSummary() {
        ConcurrentStreamSummary<String> vs = new ConcurrentStreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "A", "A"};
        for (String i : stream) {
            vs.offer(i);
            /*
		for(String s : vs.poll(3))
		System.out.print(s+" ");
			 */
            System.out.println(vs);
        }
    }

    @Test
    public void testTopK() {
        ConcurrentStreamSummary<String> vs = new ConcurrentStreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i);
        }
        List<ScoredItem<String>> topK = vs.peekWithScores(3);
        for (ScoredItem<String> c : topK) {
            assertTrue(Arrays.asList("A", "C", "X").contains(c.getItem()));
        }
    }

    @Test
    public void testTopKWithIncrement() {
        ConcurrentStreamSummary<String> vs = new ConcurrentStreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i, 10);
        }
        List<ScoredItem<String>> topK = vs.peekWithScores(3);
        for (ScoredItem<String> c : topK) {
            assertTrue(Arrays.asList("A", "C", "X").contains(c.getItem()));
        }
    }

    @Test
    public void testGeometricDistribution() {
        ConcurrentStreamSummary<Integer> vs = new ConcurrentStreamSummary<Integer>(10);
        RandomEngine re = RandomEngine.makeDefault();

        for (int i = 0; i < NUM_ITERATIONS; i++) {
            int z = Distributions.nextGeometric(0.25, re);
            vs.offer(z);
        }

        List<Integer> top = vs.peek(5);
        System.out.println("Geometric:");
        for (Integer e : top) {
            System.out.println(e);
        }

        int tippyTop = top.get(0);
        assertEquals(0, tippyTop);
        System.out.println(vs);
    }

}
