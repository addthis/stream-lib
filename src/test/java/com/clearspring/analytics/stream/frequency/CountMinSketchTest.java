package com.clearspring.analytics.stream.frequency;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class CountMinSketchTest {
    @Test
    public void testAccuracy() {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        int[] xs = new int[numItems];
        int maxScale = 20;
        for(int i = 0; i < xs.length; ++i) {
            int scale = r.nextInt(maxScale);
            xs[i] = r.nextInt(1 << scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for(int x : xs) sketch.add(x, 1);

        int[] actualFreq = new int[1 << maxScale];
        for(int x : xs) actualFreq[x]++;

        int numErrors = 0;
        for(int i = 0; i < actualFreq.length; ++i) {
            double ratio = 1.0 * (sketch.estimateCount(i) - actualFreq[i])/xs.length;
            if(ratio > 1.0001) {
                numErrors++;
            }
        }
        double pCorrect = 1 - 1.0 * numErrors / actualFreq.length;
        assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
    }
}
