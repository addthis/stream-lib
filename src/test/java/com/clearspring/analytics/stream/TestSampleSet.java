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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestSampleSet {

    private SampleSet<String> set;
    private String[] e;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        set = new SampleSet<String>();

        e = new String[3];
        for (int i = 0; i < e.length; i++) {
            e[i] = "Element_" + i;
        }
    }


    @Test
    public void testPeekK() {
        set.put(e[0]);
        for (int i = 0; i < 2; i++) {
            set.put(e[1]);
        }

        for (int i = 0; i < 3; i++) {
            set.put(e[2]);
        }

        List<String> top = null;
        // Negative
        boolean caught = false;
        try {
            top = set.peek(-1);
        } catch (IllegalArgumentException e) {
            caught = true;
        }
        assertTrue(caught);

        // 0
        top = set.peek(0);
        assertEquals(0, top.size());

        // 1
        top = set.peek(1);
        assertEquals(1, top.size());
        assertEquals(set.peek(), top.get(0));

        // 2 (more than one but less than size)
        top = set.peek(2);
        assertEquals(2, top.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(e[2 - i], top.get(i));
        }

        // 3 (size)
        top = set.peek(3);
        assertEquals(3, top.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(e[2 - i], top.get(i));
        }

        // 4 (more than size)
        top = set.peek(4);
        assertEquals(3, top.size());
        for (int i = 0; i < 3; i++) {
            assertEquals(e[2 - i], top.get(i));
        }
    }

    @Test
    public void testPut() {
        // Empty set
        assertEquals(1L, set.put(e[0]));
        assertEquals(e[0], set.peek());
        assertEquals(e[0], ((SampleSet<String>) set).peekMin());
    }

    @Test
    public void testPutWithIncrement() {
        // Empty set
        assertEquals(10L, set.put(e[0], 10));
        assertEquals(e[0], set.peek());
        assertEquals(e[0], ((SampleSet<String>) set).peekMin());

    }

    @Test
    public void testRemoveMin() {
        // Empty set
        assertNull(set.removeMin());
        assertEquals(0, set.size());
        assertEquals(0L, set.count());

        // Maintaining order        
        set.put(e[0]);
        for (int i = 0; i < 2; i++) {
            set.put(e[1]);
        }

        for (int i = 0; i < 3; i++) {
            set.put(e[2]);
        }

        assertEquals(3, set.size());
        assertEquals(6L, set.count());

        assertEquals(e[0], set.removeMin());
        assertEquals(2, set.size());
        assertEquals(5L, set.count());

        assertEquals(e[1], set.removeMin());
        assertEquals(1, set.size());
        assertEquals(3L, set.count());

        assertEquals(e[2], set.removeMin());
        assertEquals(0, set.size());
        assertEquals(0L, set.count());

        assertEquals(null, set.removeMin());
        assertEquals(0, set.size());
        assertEquals(0L, set.count());
    }
}
