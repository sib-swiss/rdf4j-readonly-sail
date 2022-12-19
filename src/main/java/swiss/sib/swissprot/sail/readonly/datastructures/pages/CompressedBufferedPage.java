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

import static swiss.sib.swissprot.sail.readonly.datastructures.pages.CompressedPage.rotateLeft;
import static swiss.sib.swissprot.sail.readonly.datastructures.pages.CompressedPage.rotateRight;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

/**
 *
 * @author Jerven Bolleman <jerven.bolleman@sib.swiss>
 * @param <T>
 */
public class CompressedBufferedPage<T> implements Page<T> {

	private final LongLongToObj<T> reconstructor;
	private final ByteBuffer buffer;
	private static final int MAX = 1 << 30;
	private final ToLong<T> getKey;

	private static final int FIRST_KEY_INDEX = 0;
	private static final int FIRST_VALUE_INDEX = Integer.BYTES;
	private static final int LAST_KEY_INDEX = Integer.BYTES * 2;
	private static final int LAST_VALUE_INDEX = Integer.BYTES * 3;
	private static final int SIZE_INDEX = Integer.BYTES * 4;
	private static final int KEYS_LENGTH_INDEX = Integer.BYTES * 5;
	private static final int VALUES_LENGTH_INDEX = Integer.BYTES * 6;
	private static final int KEYS_INDEX = Integer.BYTES * 7;

	public CompressedBufferedPage(CompressedPage<T> from,
			LongLongToObj<T> reconstructor,
			ToLong<T> getKey
	) {
		this.buffer = intoBuffer(from.size,
				from.compressedKeys,
				from.compressedValues,
				from.firstKey,
				from.firstValue,
				from.lastKey,
				from.lastValue);
		this.reconstructor = reconstructor;
		this.getKey = getKey;
	}

	public CompressedBufferedPage(ByteBuffer buffer,
			LongLongToObj<T> reconstructor,
			ToLong<T> getKey
	) {
		this.buffer = buffer;
		this.reconstructor = reconstructor;
		this.getKey = getKey;
	}

	private static ByteBuffer intoBuffer(int size, int[] compressedKeys, int[] compressedValues, int firstKey,
			int firstValue, int lastKey, int lastValue) {
		int bufferlength = compressedKeys.length
				+ // keys
				compressedValues.length
				+ // values
				2
				+ // number of keys + //number of values
				1
				+ // 1 for firstKey
				1
				+ // 1 for first value
				1
				+ // 1 for size
				1
				+ // 1 for last key
				1; // 1 for last value
		byte[] tobuffer = new byte[bufferlength * Integer.BYTES];
		ByteBuffer buffer = ByteBuffer.wrap(tobuffer);
		buffer.putInt(FIRST_KEY_INDEX, firstKey);
		buffer.putInt(FIRST_VALUE_INDEX, firstValue);

		buffer.putInt(LAST_KEY_INDEX, lastKey);
		buffer.putInt(LAST_VALUE_INDEX, lastValue);
		buffer.putInt(KEYS_LENGTH_INDEX, compressedKeys.length);
		buffer.putInt(VALUES_LENGTH_INDEX, compressedValues.length);
		buffer.putInt(SIZE_INDEX, size);
		int j = KEYS_INDEX;
		for (int i = 0; i < compressedKeys.length; i++, j += Integer.BYTES) {
			buffer.putInt(j, compressedKeys[i]);
		}
		for (int i = 0; i < compressedValues.length; i++, j += Integer.BYTES) {
			buffer.putInt(j, compressedValues[i]);
		}
		return buffer;
	}

//    private static long rotateRight(long id) {
//        return id << 1;
//    }
//
//    private static long rotateLeft(long id) {
//        return id >> 1;
//    }
	@Override
	public boolean isFull() {
		return true;
	}

	@Override
	public void sort() {
		// already sorted;
	}

	@Override
	public Iterator<T> iterator() {
		return iterator(0);
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
					return buffer.getInt(FIRST_KEY_INDEX);
				} else if (cursor == intkeys.length + 1) {
					return buffer.getInt(LAST_KEY_INDEX);
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
					return buffer.getInt(FIRST_VALUE_INDEX);
				} else if (cursor == intValues.length + 1) {
					return buffer.getInt(LAST_VALUE_INDEX);
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
		int compressedKeySize = buffer.getInt(KEYS_LENGTH_INDEX);

		int[] ints = bufferOffsetToIntegerArray(KEYS_INDEX, compressedKeySize);

		return new IntegratedIntCompressor().uncompress(ints);
	}

	private int[] decompressValues() {
		int compressedKeySize = buffer.getInt(KEYS_LENGTH_INDEX);
		int compressedValueSize = buffer.getInt(VALUES_LENGTH_INDEX);

		int valuesStartOffset = KEYS_INDEX + compressedKeySize * Integer.BYTES;
		int[] ints = bufferOffsetToIntegerArray(valuesStartOffset, compressedValueSize);
		return new IntCompressor().uncompress(ints);
	}

	private int[] bufferOffsetToIntegerArray(int offset, int length) {
		final int[] intarr = new int[length];
		int j = offset;
		for (int i = 0; i < length; i++, j += Integer.BYTES) {
			intarr[i] = buffer.getInt(j);
		}
		return intarr;
	}

	@Override
	public T first() {
		return reconstruct(buffer.getInt(FIRST_KEY_INDEX),
				buffer.getInt(FIRST_VALUE_INDEX),
				reconstructor);
	}

	private T reconstruct(int left, int right, LongLongToObj<T> reconstructor) {
		long leftId = rotateLeft(left);
		long rightId = rotateLeft(right);
		return reconstructor.apply(leftId, rightId);
	}

	@Override
	public T last() {
		return reconstruct(buffer.getInt(LAST_KEY_INDEX),
				buffer.getInt(LAST_VALUE_INDEX),
				reconstructor);
	}

	@Override
	public long size() {
		return buffer.getInt(SIZE_INDEX);
	}

	@Override
	public long firstKey() {
		return rotateLeft(buffer.getInt(FIRST_KEY_INDEX));
	}

	@Override
	public long lastKey() {
		return rotateLeft(buffer.getInt(LAST_KEY_INDEX));
	}

	@Override
	public void toStream(DataOutputStream stream) throws IOException {
		stream.writeInt(buffer.limit());
		if (buffer.hasArray()) {
			stream.write(buffer.array());
		} else {
			for (int i = 0; i < buffer.limit(); i++) {
				stream.write(buffer.get(i));
			}
		}
	}

	@Override
	public Type getType() {
		return Type.COMPRESSED_BUFFERED;
	}
}
