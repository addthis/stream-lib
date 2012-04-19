/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
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

import static junit.framework.Assert.assertEquals;

public class RegisterSetTest
{
    @Test
    public void testGetAndSet() throws Exception
    {
        RegisterSet rs = new RegisterSet((int) Math.pow(2, 4));
        rs.set(0, 11);
        assertEquals(11, rs.get(0));
    }

    @Test
    public void testGetAndSet_allPositions() throws Exception
    {
        RegisterSet rs = new RegisterSet((int) Math.pow(2, 4));
        for (int i = 0; i < Math.pow(2, 4); i++)
        {
            rs.set(i, i % 31);
            assertEquals(i % 31, rs.get(i));
        }
    }

    @Test
    public void testGetAndSet_withSmallBits() throws Exception
    {
        RegisterSet rs = new RegisterSet(6);
        rs.set(0, 11);
        assertEquals(11, rs.get(0));
    }

}
