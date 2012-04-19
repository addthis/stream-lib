package com.clearspring.analytics.stream.cardinality;

public class RegisterSet
{
    public final static int LOG2_BITS_PER_WORD = 6;
    public final static int REGISTER_SIZE = 5;

    public final int count;
    public final int size;

    private final int[] M;

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
                this.M = new int[1];
            }
            else if (bits % Integer.SIZE == 0)
            {
                this.M = new int[bits];
            }
            else
            {
                this.M = new int[bits + 1];
            }
        }
        else
        {
            this.M = initialValues;
        }
        this.size = this.M.length;
    }

    public static int getBits(int count)
    {
        return (int) Math.floor(count / LOG2_BITS_PER_WORD);
    }

    public void set(int position, int value)
    {
        int bucketPos = (int) Math.floor(position / LOG2_BITS_PER_WORD);
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        this.M[bucketPos] = (this.M[bucketPos] & ~(0x1f << shift)) | (value << shift);
    }

    public int get(int position)
    {
        int bucketPos = (int) Math.floor(position / LOG2_BITS_PER_WORD);
        int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
        return (this.M[bucketPos] & (0x1f << shift)) >>> shift;
    }

    public int[] bits()
    {
        int[] copy = new int[size];
        System.arraycopy(M, 0, copy, 0, M.length);
        return copy;
    }
}
