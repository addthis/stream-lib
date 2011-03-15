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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SampleSet<T> implements ISampleSet<T>
{
    private Map<T,Node<T>> sampleMap;
    private int size;
    private long count;
    private Random random;

    /**
     * Element with the highest frequency in the set
     */
    private Node<T> head;

    /**
     * Element with the lowest frequency in the set
     */
    private Node<T> tail;

    public SampleSet()
    {
        this(7);
    }

    public SampleSet(int capacity)
    {
        this(capacity, new Random());
    }

    public SampleSet(int capacity, Random random)
    {
        sampleMap = new HashMap<T,Node<T>>(capacity);
        this.random = random;
    }

    public T peek()
    {return (head != null) ? head.element : null;}

    public List<T> peek(int k)
    {
        List<T> topK = new ArrayList<T>(k);
        Node<T> itr = head;
        int i=0;
        while(i++<k && itr != null)
        {
            topK.add(itr.element);
            itr = itr.next;
       } 
        return topK;
    }

    public long put(T element)
    {
        Node<T> node = sampleMap.get(element);
        if(node != null)
        {
            node.count++;
            promote(node);
        }
        else
        {
            node = new Node<T>(element);
            node.count = 1;
            node.prev = tail;
            if(tail != null)
            {
                tail.next = node;
            }
            tail = node;
            if(head == null)
            {
                head = node;
            }
            sampleMap.put(element, node);
            size++;
        }
        count++;
        return node.count;
    }

    public T removeRandom()
    {
        double p = random.nextDouble();
        Node<T> itr = head;
        long weight = 0;
        while(itr != null)
        {
            weight += itr.count;
            if(p < weight/(double)count)
            {
                break;
            }
            itr = itr.next;
        }

        T removed = null;
        if(itr != null)
        {
            removed = itr.element;
            itr.count--;
            count--;
            demote(itr);
            if(itr.count == 0)
            {
                removeMin();
            }
        }

        return removed;
    }

    protected T removeMin()
    {
        T minElement = null;
        if(tail != null)
        {
            size--;
            count -= tail.count;
            minElement = tail.element;
            tail = tail.prev;
            if(tail != null)
            {
                tail.next = null;
            }
            sampleMap.remove(minElement);
        }
        return minElement;
    }

    public int size()
    {return size;}

    public long count()
    {return count;}

    protected T peekMin()
    {return tail.element;}

    protected int promote(Node<T> node)
    {
        int promotion = 0;
        while(node.prev != null && node.count > node.prev.count)
        {
            node.prev.next = node.next;
            node.next = node.prev;
            node.prev = node.next.prev;
            node.next.prev = node;
            if(node.prev != null)
            {
                node.prev.next = node;
            }
            else
            {
                head = node;
            }
            if(node.next.next != null)
            {
                node.next.next.prev = node.next;
            }
            else
            {
                tail = node.next;
            }

            promotion++;
        }
        return promotion;
    }

    protected int demote(Node<T> node)
    {
        int demotion = 0;

        while(node.next != null && node.count < node.next.count)
        {
            node.next.prev = node.prev;
            node.prev = node.next;
            node.next = node.prev.next;
            node.prev.next = node;
            if(node.next != null)
            {
                node.next.prev = node;
            }
            else
            {
                tail = node;
            }
            if(node.prev.prev != null)
            {
                node.prev.prev.next = node.prev;
            }
            else
            {
                head = node.prev;
            }

            demotion++;
        }

        return demotion;
    }

    private class Node<E>
    {
        private Node<E> next;
        private Node<E> prev;
        private E element;
        private long count;

        public Node(E element)
        {
            this.element = element;
        }
    }

}
