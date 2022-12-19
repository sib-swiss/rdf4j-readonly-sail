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
package swiss.sib.swissprot.sail.readonly.datastructures.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class ReducingIteratorTest {
	@Test
	public void basicTest() {
		Iterator<Integer> sorted = List.of(0, 1, 1, 2, 3, 3, 3, 4, 4, 5, 5, 5, 6, 7, 8, 9).iterator();
		Iterator<Integer> reducedMerge = new ReducingIterator<>(sorted, Integer::compareTo);
		for (int i = 0; i < 10; i++) {
			assertTrue(reducedMerge.hasNext());
			Integer next = reducedMerge.next();
			assertEquals(Integer.valueOf(i), next);
		}
		assertFalse(reducedMerge.hasNext());
	}

	@Test
	public void basicTest2() {
		Iterator<Integer> sorted = List.of(0, 0, 0, 1, 1, 2, 3, 3, 3, 4, 4, 5, 5, 5, 6, 7, 8, 9).iterator();
		Iterator<Integer> reducedMerge = new ReducingIterator<>(sorted, Integer::compareTo);
		for (int i = 0; i < 10; i++) {
			assertTrue(reducedMerge.hasNext());
			Integer next = reducedMerge.next();
			assertEquals(Integer.valueOf(i), next);
		}
		assertFalse(reducedMerge.hasNext());
	}
}
