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

/**
 *
 */
package com.clearspring.analytics.stream;

import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;
import static org.junit.Assert.assertTrue;


public class TestStochasticTopper {

    private static final int NUM_ITERATIONS = 100000;
    private static final int NUM_ELEMENTS = 10;
    private StochasticTopper<Integer> vs;
    private Random random;

    @Before
    public void setUp() {
        vs = new StochasticTopper<Integer>(200);
        random = new Random(340340990L);
    }


    @Test
    public void testGaussianDistribution() {
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            vs.offer(new Integer((int) Math.round((random.nextGaussian() * NUM_ELEMENTS))));
        }

        List<Integer> top = vs.peek(5);
        System.out.println("Gaussian:");
        for (Integer e : top) {
            System.out.println(e);
        }

        int tippyTop = top.get(0);
        assertTrue(tippyTop > -15 && tippyTop < 15);
    }

    @Test
    public void testZipfianDistribution() {
        RandomEngine re = RandomEngine.makeDefault();

        for (int i = 0; i < NUM_ITERATIONS; i++) {
            int z = Distributions.nextZipfInt(1.2D, re);
            vs.offer(z);
        }

        List<Integer> top = vs.peek(5);
        System.out.println("Zipfian:");
        for (Integer e : top) {
            System.out.println(e);
        }

        int tippyTop = top.get(0);
        assertTrue(tippyTop < 3);
    }

    @Test
    public void testGeometricDistribution() {
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
        assertTrue(tippyTop < 3);
    }

    @Test
    public void testRandomEngine() {
        int[] maxcounts = new int[10];
        int[] counts = new int[20];

        RandomEngine re = RandomEngine.makeDefault();

        for (int i = 0; i < NUM_ITERATIONS; i++) {
//            int z = Distributions.nextZipfInt(1.2D, re);
            int z = Distributions.nextGeometric(0.25, re);
            if (z > Integer.MAX_VALUE - 9) {
                maxcounts[Integer.MAX_VALUE - z]++;
            }
            if (z < 20) {
                counts[z]++;
            }
        }

        for (int i = 0; i < 20; i++) {
            System.out.println(i + ": " + counts[i]);
        }

        for (int i = 9; i >= 0; i--) {
            System.out.println((Integer.MAX_VALUE - i) + ": " + maxcounts[i]);
        }

    }
}
