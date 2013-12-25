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

import java.util.concurrent.atomic.AtomicIntegerArray;

public class RegisterSet
{
    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;
    public final int size;

    private final AtomicIntegerArray M;

    public RegisterSet(int count)
    {
        this(count, null);
    }

    public RegisterSet(int count, int[] initialValues)
    {
        this.count = count;
        int bits = getBits(count);

        if (initialValues == null)
        {
            if (bits == 0)
            {
                this.M = new AtomicIntegerArray(1);
            }
            else if (bits % Integer.SIZE == 0)
            {
                this.M = new AtomicIntegerArray(bits);
            }
            else
            {
                this.M = new AtomicIntegerArray(bits + 1);
            }
        }
        else
        {
            this.M = new AtomicIntegerArray(initialValues);
        }
        this.size = this.M.length();
    }

    public static int getBits(int count)
    {
        return count / LOG2_BITS_PER_WORD;
    }

    public void set(int position, int value)
    {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        int currentVal; 
        do {
            currentVal = this.M.get(bucketPos);
        } while(!this.M.compareAndSet(bucketPos, currentVal, (currentVal & ~(0x1f << shift)) | (value << shift)));
    }

    public int get(int position)
    {
        int bucketPos = position / LOG2_BITS_PER_WORD;
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M.get(bucketPos) & (0x1f << shift)) >>> shift;
    }

    public boolean updateIfGreater(int position, int value)
    {
        int bucket = position / LOG2_BITS_PER_WORD;
        int shift  = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
        int mask = 0x1f << shift;

        // Use long to avoid sign issues with the left-most shift
        int curM; 
        long newVal = value << shift;
        long curVal;
        while(true) {
            curM = this.M.get(bucket);
            curVal = curM & mask;
            if (curVal < newVal) {
                if (this.M.compareAndSet(bucket, curM, (int)((curM & ~mask) | newVal))) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    public void merge(RegisterSet that)
    {
        for (int bucket = 0; bucket < M.length(); bucket++)
        {
            int word = 0;
            int thisM;
            int thatM;
            do 
            {
                thisM = this.M.get(bucket);
                thatM = that.M.get(bucket);
                for (int j = 0; j < LOG2_BITS_PER_WORD; j++)
                {
                    int mask = 0x1f << (REGISTER_SIZE * j);
                    
                    int thisVal = (thisM & mask);
                    int thatVal = (thatM & mask);
                    word |= (thisVal < thatVal) ? thatVal : thisVal;
                }
            } while(!this.M.compareAndSet(bucket, thisM, word));
        }
    }

    int[] readOnlyBits()
    {
        return M;
    }

    public int[] bits()
    {
        int[] copy = new int[size];
        for (int i = 0; i < size; i++) {
            copy[i] = M.get(i);
        }
        return copy;
    }
}
