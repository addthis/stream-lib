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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;


public class TestStreamSummary
{
    private static final int NUM_ITERATIONS = 100000;

    @Test
    public void testStreamSummary()
    {
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "A", "A"};
        for(String i : stream)
        {
            vs.offer(i);
            /*
        for(String s : vs.poll(3))
        System.out.print(s+" ");
             */
            System.out.println(vs);
        }
    }

    @Test
    public void testTopK()
    {
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for(String i : stream)
            vs.offer(i);
        List<Counter<String>> topK = vs.topK(3);
        for(Counter<String> c : topK)
        {           
            assertTrue(Arrays.asList("A", "C", "X").contains(c.getItem()));
            //System.out.println(c);
        }
    }
    
    @Test
    public void testGeometricDistribution()
    {   
        StreamSummary<Integer> vs = new StreamSummary<Integer>(10);
        RandomEngine re = RandomEngine.makeDefault();

        for(int i=0; i<NUM_ITERATIONS; i++)
        {
            int z = Distributions.nextGeometric(0.25, re);
            vs.offer(z);        
        }

        List<Integer> top = vs.peek(5);
        System.out.println("Geometric:");
        for(Integer e : top)
            System.out.println(e);

        int tippyTop = top.get(0);
        assertEquals(0, tippyTop);
        System.out.println(vs);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testCounterSerialization() throws IOException, ClassNotFoundException
    {       
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for(String i : stream)
            vs.offer(i);
        List<Counter<String>> topK = vs.topK(3);
        for(Counter<String> c : topK)
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutput oo = new ObjectOutputStream(baos);
            oo.writeObject(c);
            oo.close();
            
            ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            Counter<String> clone = (Counter<String>)oi.readObject();
            assertEquals(c.getCount(), clone.getCount());
            assertEquals(c.getError(), clone.getError());
            assertEquals(c.getItem(), clone.getItem());
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException
    {       
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for(String i : stream)
            vs.offer(i);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutput oo = new ObjectOutputStream(baos);
        oo.writeObject(vs);
        oo.close();
            
        ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        StreamSummary<String> clone = (StreamSummary<String>)oi.readObject();

        assertEquals(vs.toString(), clone.toString());      
    }
    
    
    @Test
    public void testByteSerialization() throws IOException, ClassNotFoundException
    {
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for(String i : stream)
            vs.offer(i);

        testSerialization(vs);
        
        // Empty
        vs = new StreamSummary<String>(0);
        testSerialization(vs);
    }
    
    private void testSerialization(StreamSummary<?> vs) throws IOException, ClassNotFoundException
    {
        byte[] bytes = vs.toBytes();
        StreamSummary<String> clone = new StreamSummary<String>(bytes);

        assertEquals(vs.toString(), clone.toString());      
    }
}
