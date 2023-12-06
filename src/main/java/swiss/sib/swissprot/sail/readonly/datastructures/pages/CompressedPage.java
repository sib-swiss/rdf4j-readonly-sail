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
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

/**
 *
 @author <a href="mailto:jerven.bolleman@sib.swiss">Jerven Bolleman</a>
 * @param <T>
 */
public class CompressedPage<T> implements Page<T> {

	private final LongLongToObj<T> reconstructor;
	final int firstKey;
	final int firstValue;
	final int lastKey;
	final int lastValue;
	final int[] compressedKeys;
	final int[] compressedValues;
	final int size;
	private static final int MAX = 1 << 30;
	private final ToLong<T> getKey;

	public CompressedPage(BasicPage<T> from, LongLongToObj<T> reconstructor, ToLong<T> getKey, ToLong<T> getValue) {
		this.size = from.size;
		IntegratedIntCompressor iic = new IntegratedIntCompressor();
		int[] rawKeys = new int[from.keys.length - 2];
		for (int i = 1; i < from.keys.length - 1; i++) {
			// We rotate right to make sure we have a positive number
			rawKeys[i - 1] = (int) rotateRight(from.keys[i]);
		}
		compressedKeys = iic.compress(rawKeys);
		int[] rawValues = new int[from.values.length - 2];
		for (int i = 1; i < from.values.length - 1; i++) {
			// We rotate right to make sure we have a positive number
			rawValues[i - 1] = (int) rotateRight(from.values[i]);
		}
		this.getKey = getKey;
		IntCompressor ic = new IntCompressor();
		compressedValues = ic.compress(rawValues);
		firstKey = (int) rotateRight(getKey.apply(from.first()));
		firstValue = (int) rotateRight(getValue.apply(from.first()));
		lastKey = (int) rotateRight(getKey.apply(from.last()));
		lastValue = (int) rotateRight(getValue.apply(from.last()));
		this.reconstructor = reconstructor;
	} // We rotate right to make sure we have a positive number
		// We rotate right to make sure we have a positive number

	public static <T> boolean canCompress(BasicPage<T> c, ToLong<T> getKey, ToLong<T> getValue) {
		long id = getKey.apply(c.last());
		if (Math.abs(id) > MAX) {
			return false;
		}
		for (int i = 0; i < c.values.length; i++) {
			if (Math.abs(c.values[i]) > MAX) {
				return false;
			}
		}
		return true;
	}

	static long rotateRight(long id) {
		return id << 1;
	}

	static long rotateLeft(long id) {
		return id >> 1;
	}

	@Override
	public boolean isFull() {
		return true;
	}

	@Override
	public void sort() {
		// already sorted;
	}

	@Override
	public Iterator<T> iterator(int start) {
		PrimitiveIterator.OfLong keys = keyIterator(start);
		PrimitiveIterator.OfLong values = valueIterator(start);
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return keys.hasNext();
			}

			@Override
			public T next() {
				long key = keys.next();
				long value = values.next();
				return reconstructor.apply(key, value);
			}
		};
	}

	@Override
	public PrimitiveIterator.OfLong keyIterator() {
		return keyIterator(0);
	}

	public PrimitiveIterator.OfLong keyIterator(int start) {
		int[] intkeys = decompressKeys();
		return new PrimitiveIterator.OfLong() {
			int cursor = start;

			int currentInt() {
				if (cursor == 0) {
					return firstKey;
				} else if (cursor == intkeys.length + 1) {
					return lastKey;
				} else {
					int intkey = intkeys[cursor - 1];
					return intkey;
				}
			}

			@Override
			public long nextLong() {
				long rotateLeft = rotateLeft(currentInt());
				cursor++;
				return rotateLeft;
			}

			@Override
			public boolean hasNext() {
				return cursor < (intkeys.length + 2);
			}
		};
	}

	@Override
	public PrimitiveIterator.OfLong valueIterator() {
		return valueIterator(0);
	}

	public PrimitiveIterator.OfLong valueIterator(int start) {
		int[] intValues = decompressValues();
		return new PrimitiveIterator.OfLong() {
			int cursor = start;

			int currentInt() {
				if (cursor == 0) {
					return firstValue;
				} else if (cursor == intValues.length + 1) {
					return lastValue;
				} else {
					int intkey = intValues[cursor - 1];
					return intkey;
				}
			}

			@Override
			public long nextLong() {
				long rotateLeft = rotateLeft(currentInt());
				cursor++;
				return rotateLeft;
			}

			@Override
			public boolean hasNext() {
				return cursor < (intValues.length + 2);
			}
		};
	}

	private int[] decompressValues() {
		return new IntCompressor().uncompress(compressedValues);
	}

	@Override
	public boolean hasKey(long key) {
		if (key == firstKey()) {
			return true;
		} else if (key == lastKey()) {
			return true;
		} else if (Math.abs(key) > MAX) {
			return false;
		}
		int keyAsInt = (int) rotateRight(key);
		int[] intkeys = decompressKeys();
		int index = skipToKey(keyAsInt, intkeys);
		return index >= 0;
	}

	@Override
	public Iterator<T> fromKey(long key, ToLong<T> getKey) {
		if (key == firstKey()) {
			Iterator<T> iterator = iterator(0);
			return Iterators.earlyTerminate(iterator, n -> getKey.apply(n) == key);
		} else if (Math.abs(key) > MAX) {
			return null;
		}
		int keyAsInt = (int) rotateRight(key);
		int[] intkeys = decompressKeys();
		int index = skipToKey(keyAsInt, intkeys);
		if (index < 0) {
			if (key == lastKey()) {
				return Arrays.asList(last()).iterator();
			}
			return null;
		}
		return Iterators.earlyTerminate(iterator(index + 1), t -> keyEquals(t, key));
	}

	private boolean keyEquals(T t, long key) {
		return getKey.apply(t) == key;
	}

	private int skipToKey(int key, int[] keys) {
		int index = Arrays.binarySearch(keys, key);
		while (index > 0 && keys[index - 1] == key) {
			index--;
		}
		return index;
	}

	private int[] decompressKeys() {
		return new IntegratedIntCompressor().uncompress(compressedKeys);
	}

	@Override
	public T first() {
		return reconstruct(firstKey, firstValue, reconstructor);
	}

	private T reconstruct(int left, int right, LongLongToObj<T> reconstructor) {
		long leftId = rotateLeft(left);
		long rightId = rotateLeft(right);
		return reconstructor.apply(leftId, rightId);
	}

	@Override
	public T last() {
		return reconstruct(lastKey, lastValue, reconstructor);
	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public long firstKey() {
		return rotateLeft(firstKey);
	}

	@Override
	public long lastKey() {
		return rotateLeft(lastKey);
	}

	@Override
	public void toStream(DataOutputStream stream) throws IOException {
		sort();
		new CompressedBufferedPage<>(this, reconstructor, getKey).toStream(stream);
	}

	@Override
	public Type getType() {
		return Type.COMPRESSED;
	}
}
