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

import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMap.KeyValue;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.FilteredKeyValueOrderIterator;

public class FilteredKeyValueOrderIteratorTest {

	@Test
	public void testOne() {
		Iterator<KeyValue> base = List.of(new KeyValue(1, 1, 0)).iterator();
		Iterator<Long> filterRepeater = List.of(0l, 0l).iterator();
		Iterator<KeyValue> supply = FilteredKeyValueOrderIterator.supply(base, filterRepeater);
		assertTrue(supply.hasNext());
		KeyValue next = supply.next();
		assertEquals(1, next.key());
		assertTrue(supply.hasNext());
		next = supply.next();
		assertEquals(1, next.key());
		assertFalse(supply.hasNext());
	}

	@Test
	public void testMore() {
		Iterator<KeyValue> base = List.of(new KeyValue(1, 1, 0), new KeyValue(2, 2, 5)).iterator();
		Iterator<Long> filterRepeater = List.of(0l, 0l, 3l, 5l).iterator();
		Iterator<KeyValue> supply = FilteredKeyValueOrderIterator.supply(base, filterRepeater);
		assertTrue(supply.hasNext());
		KeyValue next = supply.next();
		assertEquals(1, next.key());
		assertTrue(supply.hasNext());
		next = supply.next();
		assertEquals(1, next.key());
		assertTrue(supply.hasNext());
		next = supply.next();
		assertEquals(2, next.key());
		assertEquals(5, next.position());
		assertFalse(supply.hasNext());
	}

	@Test
	public void testLess() {
		Iterator<KeyValue> base = List.of(new KeyValue(1, 1, 0), new KeyValue(2, 2, 5)).iterator();
		Iterator<Long> filterRepeater = List.of(0l).iterator();
		Iterator<KeyValue> supply = FilteredKeyValueOrderIterator.supply(base, filterRepeater);
		assertTrue(supply.hasNext());
		KeyValue next = supply.next();
		assertEquals(1, next.key());
		assertFalse(supply.hasNext());
	}
}
