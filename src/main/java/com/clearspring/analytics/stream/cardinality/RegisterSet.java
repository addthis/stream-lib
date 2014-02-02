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

import java.nio.ByteBuffer;

public class RegisterSet
{
    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;
    public final int size;

    private final ByteBuffer M;

    public RegisterSet(int count)
    {
        this(count, null);
    }

    public RegisterSet(int count, ByteBuffer initialValues)
    {
        this.count = count;

        if (initialValues == null)
        {
            this.M = ByteBuffer.allocate(getSizeForCount(count)*4);
        }
        else
        {
            this.M = initialValues;
        }
        this.size = this.M.capacity()/4;
    }

    public static int getBits(int count)
    {
        return count / LOG2_BITS_PER_WORD;
    }

    public static int getSizeForCount(int count)
    {
        int bits = getBits(count);
        if (bits == 0)
        {
            return 1;
        }
        else if (bits % Integer.SIZE == 0)
        {
            return bits;
        }
        else
        {
            return bits + 1;
        }
    }

    public void set(int position, int value)
    {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.M.putInt(bucketPos*4, (this.M.getInt(bucketPos*4) & ~(0x1f << shift)) | (value << shift));
    }

    public int get(int position)
    {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M.getInt(bucketPos*4) & (0x1f << shift)) >>> shift;
    }

    public boolean updateIfGreater(int position, int value)
    {
        int bucket = position / LOG2_BITS_PER_WORD;
        int bucketPos = bucket * 4;
        int shift  = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift;

        // Use long to avoid sign issues with the left-most shift
        long curVal = this.M.getInt(bucketPos) & mask;
        long newVal = value << shift;
        if (curVal < newVal) {
            this.M.putInt(bucketPos,(int) ((this.M.getInt(bucketPos) & ~mask) | newVal));
            return true;
        } else {
            return false;
        }
    }

    public void merge(RegisterSet that)
    {
        for (int bucket = 0; bucket < M.capacity()/4; bucket++)
        {
            int word = 0;
            int bucketPos = bucket*4;
            for (int j = 0; j < LOG2_BITS_PER_WORD; j++)
            {
                int mask = 0x1f << (REGISTER_SIZE * j);

                int thisVal = (this.M.getInt(bucketPos) & mask);
                int thatVal = (that.M.getInt(bucketPos) & mask);
                word |= (thisVal < thatVal) ? thatVal : thisVal;
            }
            this.M.putInt(bucketPos, word);
        }
    }

    ByteBuffer readOnlyBits()
    {
        return M;
    }

    public ByteBuffer bits()
    {
        return M.duplicate();
    }
}
