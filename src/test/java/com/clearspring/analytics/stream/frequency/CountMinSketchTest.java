package com.clearspring.analytics.stream.frequency;

import org.junit.Test;

import com.clearspring.analytics.stream.frequency.CountMinSketch.CMSMergeException;

import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class CountMinSketchTest
{
    @Test
    public void testAccuracy()
    {
        int seed = 7364181;
        Random r = new Random(seed);
        int numItems = 1000000;
        int[] xs = new int[numItems];
        int maxScale = 20;
        for (int i = 0; i < xs.length; ++i)
        {
            int scale = r.nextInt(maxScale);
            xs[i] = r.nextInt(1 << scale);
        }

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;

        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);
        for (int x : xs)
        {
            sketch.add(x, 1);
        }

        int[] actualFreq = new int[1 << maxScale];
        for (int x : xs)
        {
            actualFreq[x]++;
        }

        sketch = CountMinSketch.deserialize(CountMinSketch.serialize(sketch));

        int numErrors = 0;
        for (int i = 0; i < actualFreq.length; ++i)
        {
            double ratio = 1.0 * (sketch.estimateCount(i) - actualFreq[i]) / xs.length;
            if (ratio > 1.0001)
            {
                numErrors++;
            }
        }
        double pCorrect = 1 - 1.0 * numErrors / actualFreq.length;
        assertTrue("Confidence not reached: required " + confidence + ", reached " + pCorrect, pCorrect > confidence);
    }

    @Test
    public void merge() throws CMSMergeException
    {
        int numToMerge = 5;
        int cardinality = 1000000;

        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;

        int maxScale = 20;
        Random r = new Random();
        TreeSet<Integer> vals = new TreeSet<Integer>();

        CountMinSketch baseline = new CountMinSketch(epsOfTotalCount, confidence, seed);
        CountMinSketch[] sketchs = new CountMinSketch[numToMerge];
        for (int i = 0; i < numToMerge; i++)
        {
            sketchs[i] = new CountMinSketch(epsOfTotalCount, confidence, seed);
            for (int j = 0; j < cardinality; j++)
            {
                int scale    = r.nextInt(maxScale);
                int val      = r.nextInt(1 << scale);
                vals.add(val);
                sketchs[i].add(val, 1);
                baseline.add(val, 1);
            }
        }

        CountMinSketch merged = CountMinSketch.merge(sketchs);

        assertEquals(baseline.size(), merged.size());
        assertEquals(baseline.getConfidence(), merged.getConfidence(), baseline.getConfidence() / 100);
        assertEquals(baseline.getRelativeError(), merged.getRelativeError(), baseline.getRelativeError() / 100);
        for (int val : vals)
        {
            assertEquals(baseline.estimateCount(val), merged.estimateCount(val));
        }
    }

    @Test
    public void testMergeEmpty() throws CMSMergeException
    {
        assertNull(CountMinSketch.merge());
    }

    @Test(expected = CMSMergeException.class)
    public void testUncompatibleMerge() throws CMSMergeException
    {
        CountMinSketch cms1 = new CountMinSketch(1, 1, 0);
        CountMinSketch cms2 = new CountMinSketch(0.1, 0.1, 0);
        CountMinSketch.merge(cms1, cms2);
    }
}
