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
package swiss.sib.swissprot.sail.readonly.datastructures.list;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;

import me.lemire.integercompression.IntCompressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.datastructures.BufferUtils;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;

class LZ4Section<T> implements Section<T> {

	private static final int TWO_INTS_IN_BYTES = Integer.BYTES * 2;
	private static final LZ4Compressor FAST_COMPRESSOR = LZ4Factory.fastestInstance().highCompressor(17);
	private static final LZ4FastDecompressor FAST_DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();
	private final byte[] first;
	private final ByteBuffer[] buffers;
	private final long startOffSetInBuffers;
	private final long sectionId;
	private final Function<byte[], T> reconstructor;
	private final Function<T, byte[]> deconstuctor;
	private final Comparator<byte[]> comparator;
	private volatile WeakReference<List<byte[]>> cached;

	static byte[] compress(List<byte[]> lines) throws IOException {
		IntCompressor iic = new IntCompressor();
		int[] elementLengths = populateLengthsArray(lines);
		int[] compressedLengths = iic.compress(elementLengths);
		byte[] lengthsAsArray = new byte[(compressedLengths.length * Integer.BYTES) + TWO_INTS_IN_BYTES];
		IntBuffer asIntBuffer = ByteBuffer.wrap(lengthsAsArray).asIntBuffer();
		asIntBuffer.put(0, compressedLengths.length);
		// Note the length of compressed values array will be written in between.
		asIntBuffer.put(2, compressedLengths);
		int decompressedLength = sum(elementLengths);
		byte[] uncompressed = new byte[decompressedLength];
		int offset = 0;
		for (int i = 0; i < lines.size(); i++) {
			byte[] src = lines.get(i);
			System.arraycopy(src, 0, uncompressed, offset, src.length);
			offset += src.length;
		}
		byte[] compressedValues = FAST_COMPRESSOR.compress(uncompressed);
		asIntBuffer.put(1, compressedValues.length);
		byte[] raw = Arrays.copyOf(lengthsAsArray, lengthsAsArray.length + compressedValues.length);
		System.arraycopy(compressedValues, 0, raw, lengthsAsArray.length, compressedValues.length);
		return raw;
	}

	static void write(List<byte[]> lines, OutputStream out) throws IOException {
		byte[] raw = compress(lines);
		out.write(raw);
	}

	static <T> LZ4Section<T> read(InputStream bis, long sectionId, long at, ByteBuffer[] buffers,
			Function<byte[], T> reconstructor, Function<T, byte[]> deconstructor, Comparator<byte[]> comparator)
			throws IOException {
		byte[] readLengthBA = new byte[TWO_INTS_IN_BYTES];
		ByteBuffer readLengthBB = ByteBuffer.wrap(readLengthBA);
		readByteArray(bis, readLengthBA);
		int compressedLengthsLengths = readLengthBB.getInt(0);
		int compressedValuesLength = readLengthBB.getInt(Integer.BYTES);
		int lengthBytes = compressedLengthsLengths * Integer.BYTES;
		int[] elementLengths = readLengthsArray(bis, compressedLengthsLengths, lengthBytes);

		int firstLength = elementLengths[0];
		byte[] first = new byte[firstLength];
		int decompressedLength = sum(elementLengths);
		byte[] valueCompressed = new byte[decompressedLength];
		byte[] compressedValues = new byte[compressedValuesLength];
		readByteArray(bis, compressedValues);
		FAST_DECOMPRESSOR.decompress(compressedValues, valueCompressed);
		System.arraycopy(valueCompressed, 0, first, 0, firstLength);
		return new LZ4Section<>(sectionId, first, buffers, at, reconstructor, deconstructor, comparator);
	}

	private static void readByteArray(InputStream bis, byte[] readLengthBA) throws IOException, EOFException {
		int totalRead = 0;
		while (totalRead != -1 && totalRead < readLengthBA.length) {
			int read = bis.readNBytes(readLengthBA, totalRead, readLengthBA.length - totalRead);
			if (read == -1) {
				throw new EOFException("Expected more input");
			}
			totalRead += read;
		}
	}

	@Override
	public long sizeOnDisk() {
		ByteBuffer keyValues = BufferUtils.getByteBufferAtIndexInByteBuffers(startOffSetInBuffers, TWO_INTS_IN_BYTES,
				buffers);
		final int compressedLengthsLengths = keyValues.getInt(0);
		final int compressedValueLength = keyValues.getInt(Integer.BYTES);
		final int cllInBytes = compressedLengthsLengths * Integer.BYTES;
		return Integer.BYTES + Integer.BYTES + cllInBytes + compressedValueLength;
	}

	private static List<byte[]> buildSectionContentsAsListOfByteArrays(long startOffSetInBuffers, ByteBuffer[] buffers,
			byte[] first) {
		ByteBuffer readLengthBytes = BufferUtils.getByteBufferAtIndexInByteBuffers(startOffSetInBuffers,
				TWO_INTS_IN_BYTES, buffers);
		final int lengthCompressedSize = readLengthBytes.getInt(0);
		final int compressedValueLength = readLengthBytes.getInt(Integer.BYTES);
		final int lengthBytes = lengthCompressedSize * Integer.BYTES;

		final int[] elementLengths = readLengths(lengthCompressedSize, lengthBytes,
				startOffSetInBuffers + TWO_INTS_IN_BYTES, buffers);
		final int[] lengthsBefore = new int[elementLengths.length];
		initLengthsBefore(elementLengths, lengthsBefore);

		int decompressedLength = sum(elementLengths);

		ByteBuffer compressedValues = BufferUtils.getByteBufferAtIndexInByteBuffers(
				startOffSetInBuffers + TWO_INTS_IN_BYTES + lengthBytes, compressedValueLength, buffers);

		byte[] decompressedValues = new byte[decompressedLength];
		FAST_DECOMPRESSOR.decompress(compressedValues, ByteBuffer.wrap(decompressedValues));

		return new SectionContentsAsListOfByteArrays(decompressedValues, lengthsBefore, first);
	}

	private static int sum(int[] lengths) {
		int sum = 0;
		for (int i = 0; i < lengths.length; i++) {
			sum += lengths[i];
		}
		return sum;
	}

	private static int[] readLengthsArray(InputStream bis, int lengthCompressedSize, int lengthBytes)
			throws IOException {
		byte[] ba = new byte[lengthBytes];
		readByteArray(bis, ba);
		int[] compressedLengths = new int[lengthCompressedSize];
		IntBuffer compressedLengthsBytes = ByteBuffer.wrap(ba).asIntBuffer().get(compressedLengths);
		compressedLengthsBytes.get(0, compressedLengths);
		return new IntCompressor().uncompress(compressedLengths);
	}

	private static int[] populateLengthsArray(List<byte[]> lines) {
		int[] lengths = new int[lines.size()];
		for (int i = 0; i < lines.size(); i++) {
			lengths[i] = lines.get(i).length;
		}
		return lengths;
	}

	public LZ4Section(long id, byte[] first, ByteBuffer[] buffers, long startOffSetInBuffers,
			Function<byte[], T> reconstructor, Function<T, byte[]> deconstuctor, Comparator<byte[]> comparator) {
		super();
		this.sectionId = id;
		this.first = first;
		this.buffers = buffers;
		this.startOffSetInBuffers = startOffSetInBuffers;
		this.reconstructor = reconstructor;
		this.deconstuctor = deconstuctor;
		this.comparator = comparator;
	}

	public TPosition<T> get(int index) {
		if (index == 0) {
			return new TPosition<>(reconstructor.apply(first), sectionStart(sectionId));
		}
		List<byte[]> retrieveAsByteList = retrieveAsByteList();

		T string = reconstructor.apply(retrieveAsByteList.get(index));
		long position = (sectionStart(sectionId)) + index;
		return new TPosition<>(string, position);

	}

	public TPosition<T> findByBinarySearch(T element) {
		List<byte[]> list = retrieveAsByteList();

		final byte[] elB = deconstuctor.apply(element);
		int binarySearch = Collections.binarySearch(list, elB, comparator);
		if (binarySearch < 0)
			return null;
		else {
			T string = reconstructor.apply(list.get(binarySearch));
			long position = (sectionStart(sectionId)) + binarySearch;
			return new TPosition<>(string, position);
		}
	}

	public long findPositionByBinarySearch(T element) {
		List<byte[]> list = retrieveAsByteList();

		final byte[] elB = deconstuctor.apply(element);
		int binarySearch = Collections.binarySearch(list, elB, comparator);
		if (binarySearch < 0) {
			return WriteOnce.NOT_FOUND;
		} else {
			return (sectionStart(sectionId)) + binarySearch;
		}
	}

	private List<byte[]> retrieveAsByteList() {
		List<byte[]> list;
		if (cached == null) {
			list = buildSectionContentsAsListOfByteArrays(startOffSetInBuffers, buffers, first);
			cached = new WeakReference<>(list);
		} else {
			list = cached.get();
			if (list == null) {
				list = buildSectionContentsAsListOfByteArrays(startOffSetInBuffers, buffers, first);
				cached = new WeakReference<>(list);
			}
		}
		return list;
	}

	private static void initLengthsBefore(final int[] lengths, final int[] lengthsBefore) {
		for (int i = 1; i < lengthsBefore.length; i++) {
			int aLength = lengths[i - 1];
			lengthsBefore[i] = lengthsBefore[i - 1] + aLength;
		}
	}

	@Override
	public Iterator<TPosition<T>> iterator() {
		return listIterator();
	}

	@Override
	public ListIterator<TPosition<T>> listIterator() {
		List<byte[]> retrieveAsByteList = retrieveAsByteList();
		ListIterator<byte[]> iter = retrieveAsByteList.listIterator();
		return new ListIterator<>() {
			int at = 0;

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public TPosition<T> next() {

				int c = at;
				T string = reconstructor.apply(iter.next());
				at++;
				return new TPosition<>(string, (sectionStart(sectionId)) + c);
			}

			@Override
			public boolean hasPrevious() {
				return iter.hasPrevious();
			}

			@Override
			public TPosition<T> previous() {
				int c = at;
				T string = reconstructor.apply(iter.previous());
				at--;
				return new TPosition<>(string, (sectionStart(sectionId)) + c);
			}

			@Override
			public int nextIndex() {
				return at++;
			}

			@Override
			public int previousIndex() {

				return at--;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void set(TPosition<T> e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void add(TPosition<T> e) {
				throw new UnsupportedOperationException();
			}
		};
	}

	private static int[] readLengths(int lengthCompressedSize, int lengthBytes, long startOffSetInBuffers,
			ByteBuffer[] buffers) {
		ByteBuffer bb = BufferUtils.getByteBufferAtIndexInByteBuffers(startOffSetInBuffers, lengthBytes, buffers);
		int[] compressedLengths = new int[lengthCompressedSize];
		return readLengths(compressedLengths, bb);
	}

	private static int[] readLengths(int[] compressedLengths, ByteBuffer bb) {
		IntBuffer compressedLengthsBytes = bb.asIntBuffer();
		compressedLengthsBytes.get(compressedLengths);
		return new IntCompressor().uncompress(compressedLengths);
	}

	private static final class SectionContentsAsListOfByteArrays extends AbstractList<byte[]> {
		private final byte[] bb;
		private final int[] lengthsBefore;
		private final byte[] first;

		private SectionContentsAsListOfByteArrays(byte[] bb, int[] lengthsBefore, byte[] first) {
			this.bb = bb;
			this.lengthsBefore = lengthsBefore;
			this.first = first;
		}

		@Override
		public byte[] get(int index) {
			if (index == 0) {
				return first;
			} else if (index + 1 == lengthsBefore.length) {
				int lengthBefore = lengthsBefore[index];
				byte[] elB = new byte[bb.length - lengthBefore];
				System.arraycopy(bb, lengthBefore, elB, 0, elB.length);
				return elB;
			} else {
				int lengthBefore = lengthsBefore[index];
				byte[] elB = new byte[lengthsBefore[index + 1] - lengthBefore];
				System.arraycopy(bb, lengthBefore, elB, 0, elB.length);
				return elB;
			}
		}

		@Override
		public int size() {
			return lengthsBefore.length;
		}
	}

	public byte[] first() {
		return first;
	}

	@Override
	public long sectionId() {
		return sectionId;
	}

	private static long sectionStart(long sectionId) {
		return sectionId * SortedListInSections.SECTION_SIZE;
	}

}
