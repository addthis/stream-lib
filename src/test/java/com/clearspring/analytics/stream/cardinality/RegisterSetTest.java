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
