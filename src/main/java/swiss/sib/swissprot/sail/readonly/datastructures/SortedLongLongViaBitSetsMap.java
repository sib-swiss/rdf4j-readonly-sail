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
package swiss.sib.swissprot.sail.readonly.datastructures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.LongIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

/**
 * Store an inverted index of objects->subjects via bitsets.
 *
 */
public class SortedLongLongViaBitSetsMap implements SortedLongLongMap {
	private static final Logger logger = LoggerFactory.getLogger(SortedLongLongViaBitSetsMap.class);

	public static final String POSTFIX = "-bitsets";

	private final long[] values;
	private final LongBitmapDataProvider[] keys;
	private final long[] cumalitiveSize;

	public SortedLongLongViaBitSetsMap(long[] values, LongBitmapDataProvider[] keys) {
		this.keys = keys;
		this.values = values;
		this.cumalitiveSize = new long[keys.length];
		long cumal = 0;
		for (int i = 0; i < keys.length; i++) {
			cumalitiveSize[i] = cumal;
			cumal = cumal + keys[i].getLongCardinality();
		}
	}

	public Iterator<KeyValue> iteratorForKey(long key) {
		List<Integer> idx = new ArrayList<>();
		for (int i = 0; i < keys.length; i++) {
			if (keys[i].contains(key)) {
				idx.add(i);
			}
		}
		return new Iterator<>() {
			Iterator<Integer> iter = idx.iterator();

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public KeyValue next() {
				int idx = iter.next();
				return new KeyValue(key, values[idx], cumalitiveSize[idx] + keys[idx].rankLong(key));
			}

		};
	}

	public Iterator<KeyValue> iteratorForValue(long value) {
		for (int i = 0; i < values.length; i++) {
			long potvalue = values[i];
			if (potvalue == value) {
				LongIterator iter = keys[i].getLongIterator();
				long size = cumalitiveSize[i];
				return new Iterator<>() {
					private long rank = size;

					@Override
					public boolean hasNext() {
						return iter.hasNext();
					}

					@Override
					public KeyValue next() {
						long key = iter.next();
						return new KeyValue(key, value, rank++);
					}
				};
			}
		}
		return Collections.emptyIterator();
	}

	public Iterator<KeyValue> iterator() {
		logger.debug("Asked for iterator()");
		List<Iterator<KeyValue>> kvs = new ArrayList<>();
		for (int i = 0; i < keys.length; i++) {
			kvs.add(new KVIterator(values[i], keys[i].getLongIterator(), cumalitiveSize[i]));
		}

		logger.debug("Asked for iterator for keys: " + keys.length);
		return Iterators.concat(kvs);
	}

	@Override
	public Iterator<KeyValue> subjectOrderedIterator() {
		List<Iterator<KeyValue>> kvs = new ArrayList<>();
		for (int i = 0; i < keys.length; i++) {
			kvs.add(new KVIterator(values[i], keys[i].getLongIterator(), cumalitiveSize[i]));
		}
		return Iterators.mergeSorted(SortedLongLongMap.compareByKeyValuePosition(), kvs);
	}

	private static class KVIterator implements Iterator<KeyValue> {
		private final long value;
		private final LongIterator keys;
		private long rank;

		public KVIterator(long value, LongIterator keys, long rank) {
			super();
			this.value = value;
			this.keys = keys;
			this.rank = rank;
		}

		@Override
		public boolean hasNext() {
			return keys.hasNext();
		}

		@Override
		public KeyValue next() {
			long key = keys.next();
			return new KeyValue(key, value, rank++);
		}
	}

	@Override
	public long size() {
		long size = 0;
		for (long ls : cumalitiveSize) {
			size += ls;
		}
		// add the last long cardinality which was not in the cumalitiveSize array;
		size += keys[keys.length - 1].getLongCardinality();

		return size;
	}
}
