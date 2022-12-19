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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaLongBuffersIO;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

public class SortedLongLongMapViaLongBuffers implements SortedLongLongMap {

	public static final int SECTION_SIZE = 2048;
	public static final String POSTFIX = "-compr";
	public List<LongLongSection> sections;

	public SortedLongLongMapViaLongBuffers(List<LongLongSection> sections) {
		this.sections = sections;
	}

	public static void addSection(List<LongLongSection> sections, long offsetInBuffers, long first, long firstValue,
			ByteBuffer[] buffers, long sectionId, int sectionSize) throws IOException {
		sections.add(new LongLongSection(sectionId, first, firstValue, buffers, offsetInBuffers, sectionSize));
	}

	public Iterator<KeyValue> iteratorForKey(long key) {
		LongLongSection searchFor = new LongLongSection(0, key, 0, null, 0, 0);
		int binarySearch = Collections.binarySearch(sections, searchFor,
				SortedLongLongMapViaLongBuffers::compareSectionAsRange);
		ListIterator<LongLongSection> listIterator;
		if (binarySearch >= 0) {
			listIterator = sections.listIterator(binarySearch);
		} else {
			listIterator = sections.listIterator(-(binarySearch + 2));
		}
		rewindIteratorTillKeyNotPresent(key, listIterator);
		Iterator<LongLongSection> filter = Iterators.filter(listIterator, (s) -> {
			return s.first <= key && (s.first == key || s.findByBinarySearch(key) != null);
		});
		Iterator<KeyValue> kvs = Iterators.concat(Iterators.map(filter, LongLongSection::iterator));
		// TODO early termination.
		return Iterators.filter(kvs, kv -> kv.key() == key);
	}

	public Iterator<KeyValue> iteratorForValue(long value) {
		return Iterators.filter(iterator(), (kv) -> kv.value() == value);
	}

	void rewindIteratorTillKeyNotPresent(long key, ListIterator<LongLongSection> listIterator) {
		// rewind if needed.
		while (listIterator.hasPrevious()) {
			LongLongSection previous = listIterator.previous();
			if (previous.findByBinarySearch(key) == null) {
				listIterator.next();
				break;
			}
		}
	}

	@Override
	public Iterator<KeyValue> iterator() {
		return new KeyValueInSectionsIterator(sections.iterator());
	}

	@Override
	public Iterator<KeyValue> subjectOrderedIterator() {
		return new KeyValueInSectionsIterator(sections.iterator());
	}

	private static final class KeyValueInSectionsIterator implements Iterator<KeyValue> {
		private final Iterator<LongLongSection> siter;
		private Iterator<KeyValue> spiter = null;

		public KeyValueInSectionsIterator(Iterator<LongLongSection> siter) {
			super();
			this.siter = siter;
		}

		@Override
		public boolean hasNext() {
			if (spiter != null) {
				if (spiter.hasNext())
					return true;
				else
					spiter = null;
			}
			while (spiter == null && siter.hasNext()) {
				spiter = siter.next().iterator();
				if (!spiter.hasNext())
					spiter = null;
				else
					return true;
			}
			return false;
		}

		@Override
		public KeyValue next() {
			KeyValue next = spiter.next();
			return next;
		}
	}

	public static class LongLongSection implements Iterable<KeyValue> {
		private final long first;
		private final long firstValue;
		private final ByteBuffer[] buffers;
		private final long startOffSetInBuffers;
		private final long id;
		private final int sectionSize;

		public LongLongSection(long id, long first, long firstValue, ByteBuffer[] buffers, long startOffSetInBuffers,
				int sectionSize) {
			super();
			this.id = id;
			this.first = first;
			this.firstValue = firstValue;
			this.buffers = buffers;
			this.startOffSetInBuffers = startOffSetInBuffers;
			this.sectionSize = sectionSize;
		}

		public LongLongSection(long id, ByteBuffer[] buffers, long startOffSetInBuffers, int sectionSize) {
			super();
			this.id = id;
			this.buffers = buffers;
			this.startOffSetInBuffers = startOffSetInBuffers;
			this.sectionSize = sectionSize;
			this.first = readKeys().get(0);
			this.firstValue = readValues().get(0);
		}

		public KeyValue get(int index) {
			if (index == 0) {
				return new KeyValue(first, firstValue, id % SECTION_SIZE);
			} else {
				LongBuffer keys = readKeys();
				LongBuffer values = readValues();
				long key = keys.get(index);
				long value = values.get(index);
				return new KeyValue(key, value, (id % SECTION_SIZE) + index);
			}
		}

		private LongBuffer readValues() {
			int keysLength = BufferUtils.getIntAtIndexInByteBuffers(startOffSetInBuffers, buffers);
			int readNoOfKeyBytesBytes = SortedLongLongMapViaLongBuffersIO.readNoOfBytes(keysLength);
			long valuesOffset = startOffSetInBuffers + Integer.BYTES + readNoOfKeyBytesBytes;
			int valuesLength = BufferUtils.getIntAtIndexInByteBuffers(valuesOffset, buffers);
//			int readNoOfValuesBytes = SortedLongLongMapViaLongBuffersIO.readNoOfBytes(valuesLength);
			return SortedLongLongMapViaLongBuffersIO.readLongsBuffers(valuesOffset + Integer.BYTES, buffers,
					valuesLength, false);
		}

		private LongBuffer readKeys() {
			int length = BufferUtils.getIntAtIndexInByteBuffers(startOffSetInBuffers, buffers);
//			int readNoOfBytes = SortedLongLongMapViaLongBuffersIO.readNoOfBytes(length);
			return SortedLongLongMapViaLongBuffersIO.readLongsBuffers(startOffSetInBuffers + Integer.BYTES, buffers,
					length, true);
		}

		public KeyValue findByBinarySearch(long element) {
			LongBuffer keys = readKeys();
			List<Long> l = new AbstractList<>() {

				@Override
				public Long get(int arg0) {
					return keys.get(arg0);
				}

				@Override
				public int size() {
					return keys.limit();
				}
			};
			int binarySearch = Collections.binarySearch(l, element);
			if (binarySearch < 0)
				return null;
			else
				return get(binarySearch);
		}

		@Override
		public Iterator<KeyValue> iterator() {
			LongBuffer keys = readKeys();
			LongBuffer values = readValues();
			return new Iterator<>() {
				int at = 0;

				@Override
				public boolean hasNext() {
					return at < sectionSize;
				}

				@Override
				public KeyValue next() {
					if (at == 0) {
						at++;
						return new KeyValue(first, firstValue, id % SECTION_SIZE);
					} else {
						int c = at;
						long key = keys.get(at);
						long value = values.get(at);
						at++;
						return new KeyValue(key, value, (id % SECTION_SIZE) + c);
					}
				}
			};
		}
	}

	private static int compareSectionAsRange(LongLongSection incol, LongLongSection other) {
		return Long.compare(incol.first, other.first);
	}

	@Override
	public long size() {
		long size = 0;
		for (LongLongSection s : sections)
			size += s.sectionSize;
		return size;
	}
}
