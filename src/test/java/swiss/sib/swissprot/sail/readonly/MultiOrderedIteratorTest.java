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
package swiss.sib.swissprot.sail.readonly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

public class MultiOrderedIteratorTest {

	@Test
	public void testSimple() {
		Iterable<Long> oneA = List.of(1l);
		Iterable<Long> oneB = List.of(1l);
		Iterable<Long> twoA = List.of(2l);
		Iterable<Long> twoThreeA = List.of(2l, 3l);
		Iterable<Long> ten = List.of(10l);
		long[] exp = new long[] { 1, 1, 2, 2, 3, 10 };
		Iterator<Long> iter = Iterators.<Long>mergeSorted(Long::compare,
				List.of(oneA.iterator(), twoA.iterator(), oneB.iterator(), ten.iterator(), twoThreeA.iterator()));
		for (long e : exp) {
			assertTrue(iter.hasNext());
			assertEquals(e, iter.next().longValue());
		}
		iter = Iterators.<Long>mergeDistinctSorted(Long::compare,
				List.of(oneA.iterator(), twoA.iterator(), oneB.iterator(), ten.iterator(), twoThreeA.iterator()));
		exp = new long[] { 1, 2, 3, 10 };
		for (long e : exp) {
			assertTrue(iter.hasNext());
			assertEquals(e, iter.next().longValue());
		}
		testEmpty();
	}

	@Test
	public void testEmpty() {
		Iterator<Long> iterator = List.<Long>of().iterator();
		Iterator<Long> iterator2 = Iterators.<Long>mergeSorted(Long::compare, List.of(iterator));
		assertFalse(iterator2.hasNext());
	}

	@Test
	public void testSingle() {
		Iterator<Long> iterator = List.<Long>of(200l).iterator();
		Iterator<Long> iterator2 = Iterators.<Long>mergeSorted(Long::compare, List.of(iterator));
		assertTrue(iterator2.hasNext());
		assertEquals(200l, iterator2.next().longValue());
		assertFalse(iterator2.hasNext());
	}

	@Test
	public void testNone() {
		Iterator<Long> iterator2 = Iterators.<Long>mergeSorted(Long::compare, List.of());
		assertFalse(iterator2.hasNext());
	}
}
