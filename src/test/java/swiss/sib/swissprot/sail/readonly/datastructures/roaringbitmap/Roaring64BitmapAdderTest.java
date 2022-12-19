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
package swiss.sib.swissprot.sail.readonly.datastructures.roaringbitmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.LongIterator;

public class Roaring64BitmapAdderTest {

	@Test
	public void oneToOneMillion() {
		Roaring64BitmapAdder ad = new Roaring64BitmapAdder();
		for (int i = 0; i < 1_000_000; i++) {
			ad.add(i);
		}
		LongBitmapDataProvider build = ad.build();
		assertEquals(1_000_000L, build.getLongCardinality());
		LongIterator longIterator = build.getLongIterator();
		for (int i = 0; i < 1_000_000; i++) {
			assertTrue(longIterator.hasNext());
			assertEquals(i, longIterator.next());
		}
	}

	@Test
	public void oneToOneMillionByStep3() {
		Roaring64BitmapAdder ad = new Roaring64BitmapAdder();
		for (int i = 0; i < 1_000_000; i += 3) {
			ad.add(i);
		}
		LongBitmapDataProvider build = ad.build();
		assertEquals(333_334, build.getLongCardinality());
		LongIterator longIterator = build.getLongIterator();
		for (int i = 0; i < 1_000_000; i += 3) {
			assertTrue(longIterator.hasNext());
			assertEquals(i, longIterator.next());
		}
	}

	@Test
	public void oneToOneMillionWithSpace() {
		Roaring64BitmapAdder ad = new Roaring64BitmapAdder();
		for (int i = 0; i < 1_000_000; i += 3) {
			ad.add(i);
		}
		LongBitmapDataProvider build = ad.build();
		assertEquals(333_334L, build.getLongCardinality());
		LongIterator longIterator = build.getLongIterator();
		for (int i = 0; i < 1_000_000; i += 3) {
			assertTrue(longIterator.hasNext());
			assertEquals(i, longIterator.next());
		}
	}
}
