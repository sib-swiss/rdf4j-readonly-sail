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
package swiss.sib.swissprot.sail.readonly.datastructures.pages;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 *
 @author <a href="mailto:jerven.bolleman@sib.swiss">Jerven Bolleman</a>
 */
public class BasicPage<T> implements Page<T> {

	long[] keys = new long[LongLongSpinalList.CHUNK_SIZE];
	long[] values = new long[LongLongSpinalList.CHUNK_SIZE];
	int size = 0;
	final LongLongToObj<T> reconstructor;
	final ToLong<T> getKey;
	final ToLong<T> getValue;
	final Comparator<T> comparator;

	public BasicPage(LongLongToObj<T> reconstructor, ToLong<T> getKey, ToLong<T> getValue, Comparator<T> comparator) {
		this.reconstructor = reconstructor;
		this.getKey = getKey;
		this.getValue = getValue;
		this.comparator = comparator;
	}

	@Override
	public boolean isFull() {
		return size == LongLongSpinalList.CHUNK_SIZE;
	}

	@Override
	public void sort() {
		if (size > 1) {
			long[] sortedKeys = new long[size];
			long[] sortedValues = new long[size];
			List<T> toSort = new ArrayList<>(size);
			Iterator<T> iterator = iterator();
			while (iterator.hasNext()) {
				toSort.add(iterator.next());
			}
			toSort.sort(comparator);
			iterator = toSort.iterator();
			int i = 0;
			while (iterator.hasNext()) {
				T next = iterator.next();
				sortedKeys[i] = getKey.apply(next);
				sortedValues[i] = getValue.apply(next);
				i++;
			}
			keys = sortedKeys;
			values = sortedValues;
		} else if (size == 1) {
			long key = keys[0];
			long value = values[0];
			keys = new long[] { key };
			values = new long[] { value };
		}
	}

	public void add(T t) {
		keys[size] = getKey.apply(t);
		values[size] = getValue.apply(t);
		size++;
	}

	@Override
	public Iterator<T> iterator() {
		return iterator(0);
	}

	@Override
	public Iterator<T> iterator(int start) {
		return new Iterator<>() {
			int cursor = start;

			@Override
			public boolean hasNext() {
				return cursor < size;
			}

			@Override
			public T next() {
				return reconstructor.apply(keys[cursor], values[cursor++]);
			}
		};
	}

	@Override
	public PrimitiveIterator.OfLong keyIterator() {
		return new PrimitiveIterator.OfLong() {
			int cursor = 0;

			@Override
			public long nextLong() {
				return keys[cursor++];
			}

			@Override
			public boolean hasNext() {
				return cursor < size;
			}
		};
	}

	@Override
	public PrimitiveIterator.OfLong valueIterator() {
		return new PrimitiveIterator.OfLong() {
			int cursor = 0;

			@Override
			public long nextLong() {
				return values[cursor++];
			}

			@Override
			public boolean hasNext() {
				return cursor < size;
			}
		};
	}

	@Override
	public T first() {
		return reconstructor.apply(keys[0], values[0]);
	}

	PrimitiveIterator.OfInt indexesOfKey(long key) {
		return new PrimitiveIterator.OfInt() {
			int start = skipToKey(key);

			@Override
			public int nextInt() {
				return start++;
			}

			@Override
			public boolean hasNext() {
				if (start >= 0 && start < keys.length) {
					if (keys[start] == key) {
						return true;
					}
				}
				return false;
			}
		};
	}

	private int skipToKey(long key) {
		int index = Arrays.binarySearch(keys, 0, size, key);
		while (index > 1 && keys[index - 1] == key) {
			index--;
		}
		return index;
	}

	@Override
	public Iterator<T> fromKey(long key, ToLong<T> getKey) {
		PrimitiveIterator.OfInt i = indexesOfKey(key);
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return i.hasNext();
			}

			@Override
			public T next() {
				int index = i.nextInt();
				long key = keys[index];
				long value = values[index];
				return reconstructor.apply(key, value);
			}
		};
	}

	@Override
	public long firstKey() {
		return keys[0];
	}

	@Override
	public T last() {
		return reconstructor.apply(keys[size - 1], values[size - 1]);
	}

	@Override
	public long lastKey() {
		return keys[size - 1];
	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public void toStream(DataOutputStream stream) throws IOException {
		sort();
		new BasicBufferedPage<>(this).toStream(stream);
	}

	@Override
	public Type getType() {
		return Type.BASIC;
	}
}
