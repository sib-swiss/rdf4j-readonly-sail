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

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;

import swiss.sib.swissprot.sail.readonly.datastructures.BufferUtils;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;

class BasicSection<T> implements Section<T> {

	private final byte[] first;
	private final ByteBuffer[] buffers;
	private final long startOffSetInBuffers;
	private final long sectionId;
	private final Function<byte[], T> reconstructor;
	private final Function<T, byte[]> deconstuctor;
	private final Comparator<byte[]> comparator;
	private WeakReference<List<byte[]>> cached;

	static void write(List<byte[]> lines, DataOutputStream out) throws IOException {
		int length = 0;
		for (int i = 0; i < lines.size(); i++) {
			length += (lines.get(i).length + 1 + 8);
		}
		out.write(leftPadBytes(length));
		out.write('\n');
		for (int i = 0; i < lines.size(); i++) {
			out.write(leftPadBytes(lines.get(i).length));
			out.write(lines.get(i));
			out.write('\n');
		}
	}

	private static byte[] leftPadBytes(int toPad) {
		byte[] bToPad = Integer.toString(toPad).getBytes(StandardCharsets.UTF_8);
		if (bToPad.length == 8) {
			return bToPad;
		} else {
			byte[] padded = new byte[8];
			Arrays.fill(padded, (byte) '0');
			System.arraycopy(bToPad, 0, padded, 8 - bToPad.length, bToPad.length);
			return padded;
		}
	}

	private static int readLeftPadBytes(InputStream is) throws EOFException, IOException {
		byte[] raw = new byte[8];
		readByteArray(is, raw);
		return readLeftPadBytes(raw);
	}

	private static int readLeftPadBytes(byte[] raw) throws EOFException, IOException {
		String string = new String(raw, StandardCharsets.UTF_8);
		return Integer.parseInt(string);
	}

	static <T> BasicSection<T> read(InputStream bis, long sectionId, long at, ByteBuffer[] buffers,
			Function<byte[], T> reconstructor, Function<T, byte[]> deconstructor, Comparator<byte[]> comparator)
			throws IOException {
		int length = readLeftPadBytes(bis);
		readNewLine(bis);
		List<byte[]> lines = readLines(bis, length);

		return new BasicSection<>(sectionId, lines.get(0), buffers, at, reconstructor, deconstructor, comparator);
	}

	private static List<byte[]> readLines(InputStream bis, int length) throws EOFException, IOException {
		int read = 0;
		List<byte[]> lines = new ArrayList<>();
		while (read < length) {
			int lineLength = readLeftPadBytes(bis);
			read += (8 + 1 + lineLength);
			byte[] line = new byte[lineLength];
			readByteArray(bis, line);
			lines.add(line);
			readNewLine(bis);
		}
		return lines;
	}

	private static void readNewLine(InputStream bis) throws IOException {
		int read = bis.read();
		if (read == -1) {
			throw new EOFException();
		}
		assert read == '\n';
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
		int length;
		try {
			length = readLeftPadBytes(BufferUtils.getByteArrayAtIndexInByteBuffers(startOffSetInBuffers, 8, buffers))
					+ 8 + 1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return length;
	}

	public BasicSection(long sectionId, byte[] first, ByteBuffer[] buffers, long startOffSetInBuffers,
			Function<byte[], T> reconstructor, Function<T, byte[]> deconstuctor, Comparator<byte[]> comparator) {
		super();
		this.sectionId = sectionId;
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
		} else {
			List<byte[]> bs = retrieveAsByteList();
			T string = reconstructor.apply(bs.get(index));
			long position = sectionStart(sectionId) + index;
			return new TPosition<>(string, position);
		}
	}

	public TPosition<T> findByBinarySearch(T element) {
		final byte[] elB = deconstuctor.apply(element);
		List<byte[]> list = retrieveAsByteList();

		int binarySearch = Collections.binarySearch(list, elB, comparator);
		if (binarySearch < 0)
			return null;
		else {
			T string = reconstructor.apply(list.get(binarySearch));
			long position = sectionStart(sectionId) + binarySearch;
			return new TPosition<>(string, position);
		}
	}

	private static long sectionStart(long sectionId) {
		return sectionId * SortedListInSections.SECTION_SIZE;
	}

	private List<byte[]> retrieveAsByteList() {
		List<byte[]> list;
		if (cached == null) {
			list = buildSectionContentsAsListOfByterArrays();
			cached = new WeakReference<>(list);
		} else {
			list = cached.get();
			if (list == null) {
				list = buildSectionContentsAsListOfByterArrays();
				cached = new WeakReference<>(list);
			}
		}
		return list;
	}

	private List<byte[]> buildSectionContentsAsListOfByterArrays() {
		int length;
		try {
			length = readLeftPadBytes(BufferUtils.getByteArrayAtIndexInByteBuffers(startOffSetInBuffers, 8, buffers));
			byte[] contents = BufferUtils.getByteArrayAtIndexInByteBuffers(startOffSetInBuffers + 8 + 1, length,
					buffers);
			try (InputStream bis = new ByteArrayInputStream(contents)) {
				List<byte[]> lines = readLines(bis, length);
				return lines;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public Iterator<TPosition<T>> iterator() {
		return listIterator();
	}

	@Override
	public ListIterator<TPosition<T>> listIterator() {
		List<byte[]> retrieveAsByteList = retrieveAsByteList();
		return new ListIterator<>() {
			int at = 0;

			@Override
			public boolean hasNext() {
				return at < retrieveAsByteList.size();
			}

			@Override
			public TPosition<T> next() {
				if (at == 0) {
					at++;
					return new TPosition<>(reconstructor.apply(first), sectionStart(sectionId));
				} else {
					int c = at;
					T string = reconstructor.apply(retrieveAsByteList.get(at));
					at++;
					return new TPosition<>(string, (sectionStart(sectionId)) + c);
				}
			}

			@Override
			public boolean hasPrevious() {
				return at > 0;
			}

			@Override
			public TPosition<T> previous() {
				if (at == 0) {
					at--;
					return new TPosition<>(reconstructor.apply(first), sectionStart(sectionId));
				} else {
					int c = at;
					T string = reconstructor.apply(retrieveAsByteList.get(at));
					at--;
					return new TPosition<>(string, (sectionStart(sectionId)) + c);
				}
			}

			@Override
			public int nextIndex() {
				return at + 1;
			}

			@Override
			public int previousIndex() {
				return at - 1;
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

	public byte[] first() {
		return first;
	}

	@Override
	public long sectionId() {
		return sectionId;
	}

}
