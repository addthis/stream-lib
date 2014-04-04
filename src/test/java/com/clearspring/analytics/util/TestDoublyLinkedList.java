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

package com.clearspring.analytics.util;

import java.util.ConcurrentModificationException;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDoublyLinkedList {

    @Test
    public void testDoublyLinkedList() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        assertIsEmpty(list);
    }

    @Test
    public void testAdd() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        list.add(1);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        assertArrayEquals(new Integer[]{1}, list.toArray());
        list.add(2);
        assertFalse(list.isEmpty());
        assertEquals(2, list.size());
        assertArrayEquals(new Integer[]{1, 2}, list.toArray());
        list.add(3);
        assertFalse(list.isEmpty());
        assertEquals(3, list.size());
        assertArrayEquals(new Integer[]{1, 2, 3}, list.toArray());
        assertEquals(new Integer(1), list.first());
    }

    @Test
    public void testEnqueue() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        list.enqueue(1);
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        assertArrayEquals(new Integer[]{1}, list.toArray());
        list.enqueue(2);
        assertFalse(list.isEmpty());
        assertEquals(2, list.size());
        assertArrayEquals(new Integer[]{2, 1}, list.toArray());
        list.enqueue(3);
        assertFalse(list.isEmpty());
        assertEquals(3, list.size());
        assertArrayEquals(new Integer[]{3, 2, 1}, list.toArray());
        assertEquals(new Integer(3), list.first());
        assertEquals(new Integer(1), list.last());
    }

    @Test
    public void testAddNode() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        list.add(new ListNode2<Integer>(1));
        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
        assertArrayEquals(new Integer[]{1}, list.toArray());
        list.add(new ListNode2<Integer>(2));
        assertFalse(list.isEmpty());
        assertEquals(2, list.size());
        assertArrayEquals(new Integer[]{1, 2}, list.toArray());
        list.add(new ListNode2<Integer>(3));
        assertFalse(list.isEmpty());
        assertEquals(3, list.size());
        assertArrayEquals(new Integer[]{1, 2, 3}, list.toArray());
        assertEquals(new Integer(1), list.first());
    }

    @Test
    public void testAddAfter() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        list.add(1);
        ListNode2<Integer> node2 = list.add(2);
        ListNode2<Integer> node4 = list.add(4);

        list.addAfter(node2, 3);
        assertEquals(4, list.size());
        assertArrayEquals(new Integer[]{1, 2, 3, 4}, list.toArray());

        ListNode2<Integer> node5 = list.addAfter(node4, 5);
        assertEquals(5, list.size());
        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5}, list.toArray());
        assertEquals(new Integer(5), list.last());
        assertEquals(node5, list.head());
    }

    @Test
    public void testRemove() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        ListNode2<Integer> node1 = list.add(1);
        list.remove(node1);

        node1 = list.add(1);
        ListNode2<Integer> node2 = list.add(2);
        list.remove(node1);
        assertEquals(1, list.size());
        assertEquals(new Integer(2), list.first());
        assertEquals(node2, list.head());
        assertArrayEquals(new Integer[]{2}, list.toArray());
        list.remove(node2);
        assertIsEmpty(list);

        node1 = list.add(1);
        node2 = list.add(2);
        list.remove(node2);
        assertEquals(1, list.size());
        assertEquals(new Integer(1), list.first());
        assertEquals(node1, list.head());
        assertArrayEquals(new Integer[]{1}, list.toArray());

        node2 = list.add(2);
        list.add(3);
        assertEquals(3, list.size());
        assertArrayEquals(new Integer[]{1, 2, 3}, list.toArray());
        list.remove(node2);
        assertEquals(2, list.size());
        assertEquals(node1, list.tail());
        assertEquals(new Integer(3), list.last());
        assertArrayEquals(new Integer[]{1, 3}, list.toArray());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testConcurrentModification() {
        DoublyLinkedList<Integer> list = new DoublyLinkedList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);

        for (int i : list) {
            if (i == 2) {
                list.add(4);
            }
        }
    }

    private <T> void assertIsEmpty(DoublyLinkedList<T> list) {
        assertNull(list.tail());
        assertNull(list.head());
        assertNull(list.first());
        assertNull(list.last());
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        for (T i : list) {
            fail("What is this: " + i + " ?");
        }
    }

}
