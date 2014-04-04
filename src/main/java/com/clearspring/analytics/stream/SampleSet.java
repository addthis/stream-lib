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

public class SampleSet<T> implements ISampleSet<T> {

    private Map<T, Node<T>> sampleMap;
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

    public SampleSet() {
        this(7);
    }

    public SampleSet(int capacity) {
        this(capacity, new Random());
    }

    public SampleSet(int capacity, Random random) {
        sampleMap = new HashMap<T, Node<T>>(capacity);
        this.random = random;
    }

    public T peek() {
        return (head != null) ? head.element : null;
    }

    public List<T> peek(int k) {
        List<T> topK = new ArrayList<T>(k);
        for (Node<T> itr = head; itr != null && topK.size() < k; itr = itr.next) {
            topK.add(itr.element);
        }
        return topK;
    }

    public long put(T element) {
        return put(element, 1);
    }

    public long put(T element, int incrementCount) {
        Node<T> node = sampleMap.get(element);
        if (node != null) {
            node.count = node.count + incrementCount;
            promote(node);
        } else {
            node = new Node<T>();
            node.element = element;
            node.count = incrementCount;
            node.prev = tail;
            if (tail != null) {
                tail.next = node;
            }
            tail = node;
            if (head == null) {
                head = node;
            }
            sampleMap.put(element, node);
            size++;
        }
        count++;
        return node.count;
    }

    public T removeRandom() {
        double p = random.nextDouble();
        long weight = 0;
        for (Node<T> itr = head; itr != null; itr = itr.next) {
            weight += itr.count;
            if (p < weight / (double) count) {
                itr.count--;
                count--;
                demote(itr);
                if (itr.count == 0) {
                    removeMin();
                }
                return itr.element;
            }
        }
        return null;
    }

    protected T removeMin() {
        if (tail == null) {
            return null;
        }
        size--;
        count -= tail.count;
        T minElement = tail.element;
        tail = tail.prev;
        if (tail != null) {
            tail.next = null;
        }
        sampleMap.remove(minElement);
        return minElement;
    }

    public int size() {
        return size;
    }

    public long count() {
        return count;
    }

    protected T peekMin() {
        return tail.element;
    }

    protected void promote(Node<T> node) {
        // Bring node closer to the head as necessary
        while (node.prev != null && node.count > node.prev.count) {
            // BEFORE head... [A]node.prev.prev --> [B]node.prev --> [C]node --> [D]node.next ...tail
            // AFTER  head... [A]node.prev.prev --> [C]node --> [B]node.prev --> [D]node.next ...tail
            Node<T> b = node.prev, c = node, d = node.next, a = (b == null) ? null : b.prev;

            // Re-link each of 3 neighboring pairs
            if (a != null) {
                a.next = c;
            }
            c.prev = a;

            c.next = b;
            b.prev = c;

            b.next = d;
            if (d != null) {
                d.prev = b;
            }

            // B and C may have switched head/tail roles
            if (head == b) {
                head = c;
            }
            if (tail == c) {
                tail = b;
            }
        }
    }

    protected void demote(Node<T> node) {
        // Bring node closer to the tail as necessary
        while (node.next != null && node.count < node.next.count) {
            // BEFORE head... [A]node.prev --> [B]node --> [C]node.next --> [D]node.next.next ...tail
            // AFTER  head... [A]node.prev --> [C]node.next --> [B]node --> [D]node.next.next ...tail
            Node<T> a = node.prev, b = node, c = node.next, d = (c == null) ? null : c.next;

            // Re-link each of 3 neighboring pairs
            if (a != null) {
                a.next = c;
            }
            c.prev = a;

            c.next = b;
            b.prev = c;

            if (d != null) {
                d.prev = b;
            }
            b.next = d;

            // B and C may have switched head/tail roles
            if (head == b) {
                head = c;
            }
            if (tail == c) {
                tail = b;
            }
        }
    }

    private class Node<E> {

        private Node<E> next;
        private Node<E> prev;
        private E element;
        private long count;
    }
}
