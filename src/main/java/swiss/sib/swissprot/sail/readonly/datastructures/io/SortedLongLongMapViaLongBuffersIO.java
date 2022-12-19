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
package swiss.sib.swissprot.sail.readonly.datastructures.io;

import static swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMapViaLongBuffers.SECTION_SIZE;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.ObjIntConsumer;
import java.util.function.ToLongFunction;

import org.eclipse.rdf4j.model.Value;

import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import swiss.sib.swissprot.sail.readonly.TempSortedFile;
import swiss.sib.swissprot.sail.readonly.TempSortedFile.SubjectObjectGraph;
import swiss.sib.swissprot.sail.readonly.datastructures.BufferUtils;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMapViaLongBuffers;

public class SortedLongLongMapViaLongBuffersIO {

	public SortedLongLongMapViaLongBuffersIO() {
	}

	public static void rewrite(TempSortedFile uncompressed, File targetFile, ToLongFunction<Value> subjectStringToLong,
			ToLongFunction<Value> objectToLong, ObjIntConsumer<Long> forGraphs) throws IOException {

		try (DataInputStream dis = uncompressed.openSubjectObjectGraph()) {
			Lines lines = new Lines();
			Iterator<SubjectObjectGraph> iter = uncompressed.iterator(dis);
			try (FileOutputStream fos = new FileOutputStream(targetFile);
					OutputStream bos = new BufferedOutputStream(fos)) {
				readLines(subjectStringToLong, objectToLong, forGraphs, iter, lines, bos, uncompressed);
				if (!lines.isEmpty()) {
					writeSection(lines, bos);
				}
			}
		}
	}

	/**
	 * Simple class to pair the two arrays that are constant and never reallocated just reused.
	 *
	 */
	private static class Lines {
		private final long[] keys = new long[SECTION_SIZE];
		private final long[] values = new long[SECTION_SIZE];

		private int at = 0;

		void add(long key, long value) {
			keys[at] = key;
			values[at++] = value;
		}

		void clear() {
			at = 0;
		}

		int size() {
			return at;
		}

		boolean isEmpty() {
			return at == 0;
		}
	}

	/**
	 * We grab a bunch of lines and will after wards write them out as a section.
	 *
	 * @param subjectStringToLong
	 * @param objectToLong
	 * @param forGraphs
	 * @param iter
	 * @param lines
	 * @param bos
	 * @param uncompressed
	 * @throws IOException
	 */
	static void readLines(ToLongFunction<Value> subjectStringToLong, ToLongFunction<Value> objectToLong,
			ObjIntConsumer<Long> forGraphs, Iterator<SubjectObjectGraph> iter, Lines lines, OutputStream bos,
			TempSortedFile uncompressed) throws IOException {
		byte[] prevSubject = null;
		byte[] prevObject = null;
		long counter = 0;
		while (iter.hasNext()) {
			SubjectObjectGraph n = iter.next();
			if ((prevSubject == null && prevObject == null)) {
				prevSubject = n.subject();
				prevObject = n.object();
				addLine(subjectStringToLong, objectToLong, forGraphs, lines, counter, n, uncompressed);
				counter++;

			} else if (Arrays.equals(prevSubject, n.subject()) && Arrays.equals(prevObject, n.object())) {
				forGraphs.accept(counter, n.graphId());
			} else {
				prevSubject = n.subject();
				prevObject = n.object();
				addLine(subjectStringToLong, objectToLong, forGraphs, lines, counter, n, uncompressed);
				counter++;
			}
			if (lines.size() == (int) SECTION_SIZE) {
				writeSection(lines, bos);
			}
		}
	}

	static void addLine(ToLongFunction<Value> subjectToLong, ToLongFunction<Value> objectToLong,
			ObjIntConsumer<Long> forGraphs, Lines lines, long counter, SubjectObjectGraph fields,
			TempSortedFile uncompressed) {

		long subject = subjectToLong.applyAsLong(uncompressed.subjectValue(fields.subject()));
		long object = objectToLong.applyAsLong(uncompressed.objectValue(fields.object()));
		lines.add(subject, object);
		forGraphs.accept(counter, fields.graphId());
	}

	private static void writeSection(Lines lines, OutputStream bos) throws IOException {

		if ((lines.keys[0] - lines.keys[lines.size() - 1]) < (long) Integer.MAX_VALUE) {
			writeCompressedArray(bos, lines.keys, lines.size(), lines.keys[0], true);
		} else {
			writeLongs(bos, lines.keys, lines.size());
		}
		long valuemin = lines.values[0];
		long valuemax = lines.values[0];
		for (int i = 1; i < lines.size(); i++) {
			long v = lines.values[i];
			valuemin = Math.min(valuemin, v);
			valuemax = Math.max(valuemax, v);
		}
		if ((valuemax - valuemin) < (long) Integer.MAX_VALUE) {
			writeCompressedArray(bos, lines.values, lines.size(), valuemin, false);
		} else {
			writeLongs(bos, lines.values, lines.size());
		}
		lines.clear();
	}

	private static void writeLongs(OutputStream bos, long[] keys, int size) throws IOException {
		byte[] keysAsArray = new byte[(size * Long.BYTES) + Integer.BYTES];
		ByteBuffer wrap = ByteBuffer.wrap(keysAsArray);
		wrap.putInt(size);
		wrap.slice().asLongBuffer().put(keys);
		bos.write(keysAsArray);
	}

	private static void writeCompressedArray(OutputStream bos, long[] keys, int size, long valuemin,
			boolean expectSorted) throws IOException {
		int[] ikeys = new int[size];
		for (int i = 0; i < size; i++) {
			ikeys[i] = (int) (keys[i] - valuemin);
		}
		int[] compress;
		if (expectSorted) {
			IntegratedIntCompressor iic = new IntegratedIntCompressor();
			compress = iic.compress(ikeys);
		} else {
			IntCompressor iic = new IntCompressor();
			compress = iic.compress(ikeys);
		}
		byte[] lengthsAsArray = new byte[(compress.length * Integer.BYTES) + Integer.BYTES + Long.BYTES];
		ByteBuffer wrapped = ByteBuffer.wrap(lengthsAsArray);
		IntBuffer asIntBuffer = wrapped.asIntBuffer();
		wrapped.putLong(lengthsAsArray.length - Long.BYTES, valuemin);
		int length = -(compress.length);
		asIntBuffer.put(0, length);
		asIntBuffer.put(1, compress);
		bos.write(lengthsAsArray);
	}

	public static LongBuffer readCompressedLongsAsBuffer(long at, ByteBuffer[] buffers, int length,
			boolean expectInSortedOrder) {
		length = Math.abs(length);
		int lengthInBytes = length * Integer.BYTES;
		IntBuffer compressedBuffer = BufferUtils.readByteBufferAtOfLength(at, buffers, lengthInBytes).asIntBuffer();
		at += lengthInBytes;
		int[] compressed = new int[length];
		compressedBuffer.get(compressed);
		int[] uncompress;
		if (expectInSortedOrder) {
			IntegratedIntCompressor iic = new IntegratedIntCompressor();
			uncompress = iic.uncompress(compressed);
		} else {
			IntCompressor ic = new IntCompressor();
			uncompress = ic.uncompress(compressed);
		}
		long min = BufferUtils.getLongAtIndexInByteBuffers(at, buffers);
		long[] longs = new long[uncompress.length];
		for (int i = 0; i < uncompress.length; i++) {
			longs[i] = uncompress[i] + min;
		}
		return LongBuffer.wrap(longs);
	}

	public static SortedLongLongMapViaLongBuffers readin(File target) throws FileNotFoundException, IOException {
		long size = target.length();
		List<SortedLongLongMapViaLongBuffers.LongLongSection> sections = new ArrayList<>();
		ByteBuffer[] buffers = BufferUtils.openByteBuffer(target.toPath());
		long at = 0;
		long sectionId = 0;
		while (at < size) {
			long np = at;
			int keyLength = BufferUtils.getIntAtIndexInByteBuffers(at, buffers);
			at += Integer.BYTES;
			LongBuffer keys = SortedLongLongMapViaLongBuffersIO.readLongsBuffers(at, buffers, keyLength, true);
			at += SortedLongLongMapViaLongBuffersIO.readNoOfBytes(keyLength);
			int valueLength = BufferUtils.getIntAtIndexInByteBuffers(at, buffers);
			at += Integer.BYTES;
			LongBuffer values = SortedLongLongMapViaLongBuffersIO.readLongsBuffers(at, buffers, valueLength, false);
			at += SortedLongLongMapViaLongBuffersIO.readNoOfBytes(valueLength);
			long firstKey = keys.get(0);
			long firstValue = values.get(0);
			SortedLongLongMapViaLongBuffers.addSection(sections, np, firstKey, firstValue, buffers, sectionId,
					keys.limit());
			sectionId++;
		}

		return new SortedLongLongMapViaLongBuffers(sections);
	}

	public static int readNoOfBytes(int length) {
		if (length < 0) {
			return ((Math.abs(length)) * Integer.BYTES) + Long.BYTES;
		} else
			return (length * Long.BYTES);
	}

	public static LongBuffer readUncompressedLongs(long postion, ByteBuffer[] buffers, int length) {
		return BufferUtils.readByteBufferAtOfLength(postion, buffers, Long.BYTES * length).asLongBuffer();
	}

	public static long[] readUncompressedLongs(InputStream is, int length) throws IOException {
		long[] longs = new long[length];
		byte[] bytes = is.readNBytes(Long.BYTES * length);
		LongBuffer asLongBuffer = ByteBuffer.wrap(bytes).asLongBuffer();
		asLongBuffer.get(longs);
		return longs;
	}

	public static LongBuffer readLongsBuffers(long position, ByteBuffer[] in, int length, boolean expectInSortedOrder) {

		if (length < 0) {
			return readCompressedLongsAsBuffer(position, in, length, expectInSortedOrder);
		} else {
			return readUncompressedLongs(position, in, length);
		}
	}
}
