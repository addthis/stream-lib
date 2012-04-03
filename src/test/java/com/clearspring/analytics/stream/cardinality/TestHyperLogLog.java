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

import static org.junit.Assert.*;

public class TestHyperLogLog
{
    @Test
    public void testComputeCount()
    {
        HyperLogLog hyperLogLog = new HyperLogLog(.05);
        hyperLogLog.offer(0);
        hyperLogLog.offer(1);
		hyperLogLog.offer(2);
		hyperLogLog.offer(3);
		hyperLogLog.offer(16);
		hyperLogLog.offer(17);
		hyperLogLog.offer(18);
		hyperLogLog.offer(19);
		hyperLogLog.offer(19);
        assertEquals(8, hyperLogLog.cardinality());
    }

    
    @Test
    public void testSerialization() throws IOException
	{
        HyperLogLog hll = new HyperLogLog(.05);
        hll.offer("a");
        hll.offer("b");
		hll.offer("c");
		hll.offer("d");
		hll.offer("e");

		HyperLogLog hll2 = new HyperLogLog(HyperLogLog.getBits(hll.getBytes()), .05);
        assertArrayEquals(hll.bitVector.bits(), hll2.bitVector.bits());
        assertEquals(hll.cardinality(), hll2.cardinality());
        assertEquals(hll.registerSize, hll2.registerSize);
    }

	@Test
	public void testICardinality()
	{
		HyperLogLog hyperLogLog = new HyperLogLog(.005);
		int size = 5000000;
		for (int i = 0; i < size; i++)
		{
			hyperLogLog.offer(TestICardinality.streamElement(i));
		}

		long estimate = hyperLogLog.cardinality();
		double err = Math.abs(estimate - size) / (double) size;
		assertTrue(err < .01);
	}
    
    @Test
    public void testMerge() throws CardinalityMergeException
    {
        int numToMerge = 5;
        double rds = 0.005;
        int cardinality = 1000;

		HyperLogLog[] hyperLogLogs = new HyperLogLog[numToMerge];

        for(int i=0; i<numToMerge; i++)
        {
            hyperLogLogs[i] = new HyperLogLog(rds);
            for(int j=0; j<cardinality; j++)
                hyperLogLogs[i].offer(Math.random());
        }

		int expectedCardinality = numToMerge*cardinality;
        HyperLogLog hll = hyperLogLogs[0];
        hyperLogLogs = Arrays.asList(hyperLogLogs).subList(1, hyperLogLogs.length).toArray(new HyperLogLog[0]);
        long mergedEstimate = hll.merge(hyperLogLogs).cardinality();
        double error = Math.abs(mergedEstimate - expectedCardinality) / (double)expectedCardinality;
        assertTrue(error < .01);
    }
}
