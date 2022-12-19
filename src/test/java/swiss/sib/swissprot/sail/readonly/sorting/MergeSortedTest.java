/*******************************************************************************
 * Copyright (c) 2022 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package swiss.sib.swissprot.sail.readonly.sorting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Test;

import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.ReducingIterator;

public class MergeSortedTest {

	@Test
	public void basicTest() {
		List<Iterator<String>> readers = List.of(List.of("a", "c").iterator(), List.of("b", "d").iterator());
		Iterator<String> mergeSort = Iterators.<String>mergeDistinctSorted(String::compareTo, readers);
		verify(List.of("a", "b", "c", "d"), mergeSort);
	}

	@Test
	public void basicTestWithEmpty() {
		List<Iterator<String>> readers = List.of(List.of("a", "c").iterator(), List.of("b", "d").iterator(),
				Collections.emptyIterator());
		Iterator<String> mergeSort = Iterators.<String>mergeDistinctSorted(String::compareTo, readers);
		verify(List.of("a", "b", "c", "d"), mergeSort);
	}

	@Test
	public void basicParralelTest() {
		List<Iterator<String>> readers = List.of(List.of("a", "c").iterator(), List.of("b", "d").iterator());
		Iterator<String> mergeSort = Iterators.<String>mergeDistinctSorted(String::compareTo, readers);
		verify(List.of("a", "b", "c", "d"), mergeSort);
	}

	private <T> void verify(List<T> of2, Iterator<T> mergeSort) {
		Iterator<T> t = of2.iterator();
		while (t.hasNext()) {
			assertTrue(mergeSort.hasNext());
			assertEquals(t.next(), mergeSort.next());
		}
		assertFalse(mergeSort.hasNext());
	}

	@Test
	public void basicTest2() {
		List<Iterator<String>> readers = List.of(List.of("a", "d").iterator(), List.of("b", "c").iterator());
		Iterator<String> mergeSort = Iterators.<String>mergeDistinctSorted(String::compareTo, readers);
		verify(List.of("a", "b", "c", "d"), mergeSort);
	}

	@Test
	public void basicParralelTest2() {
		List<Iterator<String>> readers = List.of(List.of("a", "d").iterator(), List.of("b", "c").iterator());
		Iterator<String> mergeSort = Iterators.<String>mergeDistinctSorted(String::compareTo, readers);
		verify(List.of("a", "b", "c", "d"), mergeSort);
	}

	@Test
	public void basicTest3() {
		List<Iterator<String>> readers = List.of(List.of("a", "d").iterator(), List.of("b", "c").iterator(),
				List.of("e", "f").iterator());
		Iterator<String> mergeSort = Iterators.<String>mergeDistinctSorted(String::compareTo, readers);
		verify(List.of("a", "b", "c", "d", "e", "f"), mergeSort);
	}

	@Test
	public void basicTest4() {
		List<Iterator<Integer>> readers = List.of(List.of(1, 2).iterator(), List.of(3, 4).iterator(),
				List.of(5, 6).iterator());
		Iterator<Integer> mergeSort = Iterators.<Integer>mergeDistinctSorted(Integer::compareTo, readers);
		verify(List.of(1, 2, 3, 4, 5, 6), mergeSort);
	}

	@Test
	public void basicTest5() {
		Iterator<Integer> a = IntStream.range(0, 1000).iterator();
		Iterator<Integer> b = IntStream.range(1000, 2000).iterator();
		Iterator<Integer> c = IntStream.range(2000, 5000).iterator();
		List<Iterator<Integer>> readers = List.of(a, b, c);
		Iterator<Integer> mergeSort = Iterators.<Integer>mergeDistinctSorted(Integer::compareTo, readers);
		List<Integer> expected = new ArrayList<>();
		IntStream.range(0, 5000).forEach(expected::add);
		verify(expected, mergeSort);
	}

	@Test
	public void basicParallelTest5() {
		Iterator<Integer> a = IntStream.range(0, 1000).iterator();
		Iterator<Integer> b = IntStream.range(1000, 2000).iterator();
		Iterator<Integer> c = IntStream.range(2000, 5000).iterator();
		List<Iterator<Integer>> readers = List.of(a, b, c);
		Iterator<Integer> mergeSort = Iterators.<Integer>mergeDistinctSorted(Integer::compareTo, readers);
		List<Integer> expected = new ArrayList<>();
		IntStream.range(0, 5000).forEach(expected::add);
		verify(expected, mergeSort);
	}

	@Test
	public void overlappingTest1() {
		List<Iterator<Integer>> readers = new ArrayList<>();
		for (int i = 0; i <= 256; i++) {
			Iterator<Integer> a = IntStream.range(i, i + 512).iterator();
			readers.add(a);
		}
		Iterator<Integer> mergeSort = Iterators.<Integer>mergeDistinctSorted(Integer::compareTo, readers);
		Iterator<Integer> reducedMerge = new ReducingIterator<>(mergeSort, Integer::compareTo);
		for (int i = 0; i < 256 + 512; i++) {
			assertTrue(reducedMerge.hasNext());
			assertEquals(Integer.valueOf(i), reducedMerge.next());
		}
		assertFalse(reducedMerge.hasNext());
	}

	@Test
	public void overlappingTest2() {
		Iterator<Integer> a = IntStream.range(0, 1000).iterator();
		Iterator<Integer> b = IntStream.range(500, 2500).iterator();
		Iterator<Integer> c = IntStream.range(2000, 5000).iterator();
		List<Iterator<Integer>> readers = List.of(a, b, c);
		Iterator<Integer> mergeSort = Iterators.<Integer>mergeDistinctSorted(Integer::compareTo, readers);
		Iterator<Integer> reducedMerge = new ReducingIterator<>(mergeSort, Integer::compareTo);
		for (int i = 0; i < 5000; i++) {
			assertTrue(reducedMerge.hasNext());
			assertEquals(Integer.valueOf(i), reducedMerge.next());
		}
		assertFalse(reducedMerge.hasNext());
	}

	@Test
	public void overlappingTest3() {
		List<Iterator<Integer>> readers = new ArrayList<>();
		for (int i = 0; i <= 256; i++) {
			Iterator<Integer> a = IntStream.range(i, i + 512).iterator();
			readers.add(a);
		}
		for (int i = 0; i <= 256; i++) {
			Iterator<Integer> a = IntStream.range(i, i + 512).iterator();
			readers.add(a);
		}
		Iterator<Integer> mergeSort = Iterators.<Integer>mergeDistinctSorted(Integer::compareTo, readers);
		Iterator<Integer> reducedMerge = new ReducingIterator<>(mergeSort, Integer::compareTo);
		for (int i = 0; i < 256 + 512; i++) {
			assertTrue(reducedMerge.hasNext());
			Integer next = reducedMerge.next();
			assertEquals(Integer.valueOf(i), next);
		}
		assertFalse(reducedMerge.hasNext());
	}

}
