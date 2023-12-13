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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.ObjIntConsumer;
import java.util.function.ToLongFunction;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.VariableByte;
import swiss.sib.swissprot.sail.readonly.TempSortedFile;
import swiss.sib.swissprot.sail.readonly.TempSortedFile.SubjectObjectGraph;
import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongViaBitSetsMap;
import swiss.sib.swissprot.sail.readonly.datastructures.roaringbitmap.Roaring64BitmapAdder;

public class SortedLongLongMapViaBitSetsIO {
	private static final int MAX_DISTINCT_VALUES = Short.MAX_VALUE;
	private static final int NO_OF_TEMP_FILES = 32;
	private static final Logger logger = LoggerFactory.getLogger(SortedLongLongMapViaBitSetsIO.class);

	public static boolean rewrite(TempSortedFile uncompressed, File targetFile, ToLongFunction<Value> subjectToLong,
			ToLongFunction<Value> objectToLong, ObjIntConsumer<Long> forGraphs) throws IOException {

		Instant start = Instant.now();
		Set<Long> objectIds = new HashSet<>();
		if (!testIfNotToManyDistinctObjects(uncompressed, objectToLong, objectIds)) {
			return false;
		} else {
			File[] tempFiles = makeTempFiles(targetFile);
			long[] objectIdsArray = objectIds.stream().mapToLong(Long::valueOf).sorted().toArray();

			long[] triples = writeTempObjSubjGraphId(uncompressed, subjectToLong, objectToLong, objectIdsArray,
					tempFiles);
			try (FileOutputStream fos = new FileOutputStream(targetFile);
					BufferedOutputStream bos = new BufferedOutputStream(fos);
					ObjectOutputStream dos = new ObjectOutputStream(bos)) {
				dos.writeInt(objectIdsArray.length);
				for (long objectId : objectIdsArray)
					dos.writeLong(objectId);
				writeSubjectBitmap(tempFiles, forGraphs, objectIdsArray, dos, triples);
			}
			for (int i = 0; i < tempFiles.length; i++) {
				tempFiles[i].delete();
			}
		}
		Instant end = Instant.now();
		logger.info("Rewrote into " + targetFile.getPath() + " took: " + Duration.between(start, end));
		return true;
	}

	private static File[] makeTempFiles(File targetFile) {
		File[] tempFiles = new File[NO_OF_TEMP_FILES];
		for (int i = 0; i < tempFiles.length; i++) {
			File tempFile = new File(targetFile.getParentFile(),
					"temp_object_subject_graph_as_longs_" + targetFile.getName() + "_" + i);
			tempFiles[i] = tempFile;
		}
		return tempFiles;
	}

	private static long[] writeTempObjSubjGraphId(TempSortedFile uncompressed, ToLongFunction<Value> subjectToLong,
			ToLongFunction<Value> objectToLong, long[] objectIdsArray, File[] tempFiles)
			throws IOException, FileNotFoundException {
		long[] triples = new long[tempFiles.length];
//		TempSection ts = new TempSection();
		IntCompressor icc = intCompressor();
		DataOutputStream[] doss = new DataOutputStream[tempFiles.length];
		TempSection[] tss = new TempSection[tempFiles.length];
		for (int i = 0; i < tempFiles.length; i++) {
			FileOutputStream fos = new FileOutputStream(tempFiles[i]);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			doss[i] = new DataOutputStream(bos);
			tss[i] = new TempSection();
		}
		try (DataInputStream dis = uncompressed.openSubjectObjectGraph()) {
			Iterator<SubjectObjectGraph> iterator = uncompressed.iterator(dis);

			while (iterator.hasNext()) {
				SubjectObjectGraph line = iterator.next();
				Value objectValue = uncompressed.objectValue(line.object());
				long readObject = objectToLong.applyAsLong(objectValue);
				Resource subjectValue = uncompressed.subjectValue(line.subject());
				long subject = subjectToLong.applyAsLong(subjectValue);
				int graph = line.graphId();
				assert graph != WriteOnce.NOT_FOUND : line.graphId();
				assert subject != WriteOnce.NOT_FOUND : subjectValue + " not found";
				assert readObject != WriteOnce.NOT_FOUND : objectValue + " not found";
				int objectIdIndex = Arrays.binarySearch(objectIdsArray, readObject);
				int arrayIndex = objectIdIndex % tempFiles.length;
				TempSection ts = tss[arrayIndex];
				ts.add(objectIdIndex, subject, graph);
				triples[arrayIndex]++;
				if (ts.isFull()) {
					ts.write(doss[arrayIndex], icc);
					ts.reset();
				}
			}
		}
		for (int i = 0; i < tempFiles.length; i++) {
			TempSection ts = tss[i];

			if (!ts.isEmpty()) {
				ts.write(doss[i], icc);
				ts.reset();
			}
			doss[i].close();
		}
		return triples;
	}

	private static final class TempSection {
		private static final int MAX = Character.MAX_VALUE;
		private final int[] objectIndexes;
		private final long[] subjects;
		private final int[] graphs;
		private int length = 0;

		public TempSection() {
			objectIndexes = new int[MAX];
			subjects = new long[MAX];
			graphs = new int[MAX];
		}

		public boolean isEmpty() {
			return length == 0;
		}

		public void add(int objectIndex, long subject, int graph) {
			this.objectIndexes[length] = objectIndex;
			this.graphs[length] = graph;
			this.subjects[length] = subject;
			this.length++;
		}

		public boolean isFull() {
			return length == MAX;
		}

		public void write(DataOutputStream out, IntCompressor icc) throws IOException {
			out.writeChar((char) length);
			int[] compressedObjectIndexes;
			int[] compressedGraphs;
			if (length != MAX) {
				compressedObjectIndexes = icc.compress(Arrays.copyOf(objectIndexes, length));
				compressedGraphs = icc.compress(Arrays.copyOf(graphs, length));
			} else {
				compressedObjectIndexes = icc.compress(objectIndexes);
				compressedGraphs = icc.compress(graphs);
			}

			out.writeInt(compressedObjectIndexes.length);
			out.writeInt(compressedGraphs.length);
			int[] lowSubjectBits = new int[length];
			int[] highSubjectBits = new int[length];
			encodeLongsIntoTwoIntArrays(lowSubjectBits, highSubjectBits);

			int[] lowSubjectBitsCompressed = icc.compress(lowSubjectBits);
			int[] highSubjectBitsCompressed = icc.compress(highSubjectBits);
			out.writeInt(lowSubjectBitsCompressed.length);
			out.writeInt(highSubjectBitsCompressed.length);
			write(out, compressedObjectIndexes);
			write(out, compressedGraphs);
			write(out, lowSubjectBitsCompressed);
			write(out, highSubjectBitsCompressed);

		}

		private void encodeLongsIntoTwoIntArrays(int[] lowSubjectBits, int[] highSubjectBits) {
			for (int i = 0; i < length; i++) {
				lowSubjectBits[i] = (int) (subjects[i] >>> Integer.SIZE);
				highSubjectBits[i] = (int) subjects[i];
			}
		}

		private void write(DataOutputStream out, int[] ints) throws IOException {
			byte[] bs = new byte[Integer.BYTES * ints.length];
			ByteBuffer.wrap(bs).asIntBuffer().put(ints);
			out.write(bs);
		}

		public void reset() {
			length = 0;
		}
	}

	private static final class TempLazySection {
		private final int[] objectIndexes;
		private long[] subjects;
		private int[] lowSubjectsBitsCompressed;
		private int[] highSubjectsBitsCompressed;
		private int[] graphs;

		private final IntCompressor ic;

		public TempLazySection(int[] objects2, int[] graphs2, int[] lowSubjects2, int[] highSubjects2, int length,
				IntCompressor ic) {
			this.objectIndexes = objects2;
			this.graphs = graphs2;
			this.lowSubjectsBitsCompressed = lowSubjects2;
			this.highSubjectsBitsCompressed = highSubjects2;
			this.ic = ic;
		}

		public void decompress() {
			if (subjects == null) {
				graphs = ic.uncompress(graphs);
				subjects = new long[objectIndexes.length];
				decompressIntoLowLongBits(ic, lowSubjectsBitsCompressed, subjects);
				decompressIntoHighLongBits(ic, subjects, highSubjectsBitsCompressed);
			}
		}

		public static TempLazySection read(InputStream in, IntCompressor ic) throws IOException {
			ByteBuffer lengths = ByteBuffer.wrap(in.readNBytes(Character.BYTES + Integer.BYTES * 4));
			int length = lengths.getChar(0);
			int compressedObjectIndexLength = lengths.getInt(Character.BYTES);
			int compressedGraphsLength = lengths.getInt(Character.BYTES + Integer.BYTES);
			int lowSubjectBitsCompressedLength = lengths.getInt(Character.BYTES + (Integer.BYTES * 2));
			int highSubjectBitsCompressedLength = lengths.getInt(Character.BYTES + (Integer.BYTES * 3));
			int[] compressedObjectIndex = readBytesAsInt(in, compressedObjectIndexLength);

			int[] objectIndex = ic.uncompress(compressedObjectIndex);
			int[] compressedGraphs = readBytesAsInt(in, compressedGraphsLength);

			int[] lowSubjectBitsCompressed = readBytesAsInt(in, lowSubjectBitsCompressedLength);
			int[] highSubjectBitsCompressed = readBytesAsInt(in, highSubjectBitsCompressedLength);
			return new TempLazySection(objectIndex, compressedGraphs, lowSubjectBitsCompressed,
					highSubjectBitsCompressed, length, ic);
		}

		private static void decompressIntoHighLongBits(IntCompressor ic, long[] subjects,
				int[] highSubjectBitsCompressed) {
			int[] highSubjectsMap = ic.uncompress(highSubjectBitsCompressed);
			for (int i = 0; i < subjects.length; i++) {
				subjects[i] |= ((long) highSubjectsMap[i]);
			}
		}

		private static void decompressIntoLowLongBits(IntCompressor ic, int[] lowSubjectBitsCompressed,
				long[] subjects) {
			int[] lowSubjectsMap = ic.uncompress(lowSubjectBitsCompressed);
			for (int i = 0; i < subjects.length; i++) {
				subjects[i] = ((long) lowSubjectsMap[i]) << Integer.SIZE;
			}
		}

		private static int[] readBytesAsInt(InputStream in, int valuesToRead) throws IOException {
			byte[] bytes = in.readNBytes(valuesToRead * Integer.BYTES);
			int[] ints = new int[valuesToRead];
			ByteBuffer.wrap(bytes).asIntBuffer().get(ints);
			return ints;
		}

		public int size() {
			return objectIndexes.length;
		}
	}

	private static IntCompressor intCompressor() {
		return new IntCompressor(new SkippableComposition(new FastPFOR(), new VariableByte()));
	}

	private static void writeSubjectBitmap(File[] tempFiles, ObjIntConsumer<Long> forGraphs, long[] objectIdsArray,
			ObjectOutputStream dos, long[] triples) throws IOException, FileNotFoundException {

		long at = 0;
		for (int i = 0; i < objectIdsArray.length; i++) {
			long objectId = objectIdsArray[i];
			at = readIntoBitMapAndGraph(forGraphs, dos, at, objectId, i, tempFiles, triples);
		}

	}

	private static boolean testIfNotToManyDistinctObjects(TempSortedFile uncompressed,
			ToLongFunction<Value> objectToLong, Set<Long> objectIds) throws IOException, FileNotFoundException {
		try (DataInputStream dis = uncompressed.openObjects()) {
			int distinctObjects = 0;
			Iterator<Value> iter = uncompressed.objectIterator(dis);
			while (iter.hasNext()) {
				if (distinctObjects++ > MAX_DISTINCT_VALUES)
					return false;
				Value next = iter.next();
				objectIds.add(objectToLong.applyAsLong(next));
			}
		}
		return true;
	}

	private static long readIntoBitMapAndGraph(ObjIntConsumer<Long> forGraphs, ObjectOutputStream dos, long at,
			long objectId, int objectIdIndex, File[] tempFiles, long[] triples) throws IOException {
		Roaring64BitmapAdder collector = new Roaring64BitmapAdder(false);
		long prevSubject = WriteOnce.NOT_FOUND;

		int arrayIndex = objectIdIndex % tempFiles.length;
		IntCompressor intCompressor = intCompressor();
		try (FileInputStream fis = new FileInputStream(tempFiles[arrayIndex]);
				BufferedInputStream bis = new BufferedInputStream(fis)) {
			long max = triples[arrayIndex];
			while (0 < max) {
				TempLazySection ts = TempLazySection.read(bis, intCompressor);
				max -= ts.size();
				for (int i = 0; i < ts.size(); i++) {
					int readObject = ts.objectIndexes[i];
					assert WriteOnce.NOT_FOUND != readObject;
					if (objectIdIndex == readObject) {
						ts.decompress();
						long subject = ts.subjects[i];
						assert WriteOnce.NOT_FOUND != subject;
						int graph = ts.graphs[i];

						if (prevSubject != subject) {
							forGraphs.accept(at++, graph);
							collector.add(subject);
							prevSubject = subject;
						} else {
							forGraphs.accept(at, graph);
						}
					}
				}
			}
			LongBitmapDataProvider values = collector.build();
			Roaring64BitmapAdder.writeLongBitmapDataProvider(dos, values);
			return at;
		}
	}

	public static SortedLongLongViaBitSetsMap readin(File target) throws FileNotFoundException, IOException {
		try (FileInputStream fis = new FileInputStream(target); ObjectInputStream bis = new ObjectInputStream(fis)) {
			int noOfKeys = bis.readInt();
			long[] values = new long[noOfKeys];
			for (int i = 0; i < noOfKeys; i++) {
				values[i] = bis.readLong();
			}
			LongBitmapDataProvider[] keys = new LongBitmapDataProvider[noOfKeys];
			for (int i = 0; i < noOfKeys; i++) {
				keys[i] = Roaring64BitmapAdder.readLongBitmapDataProvider(bis);
			}
			return new SortedLongLongViaBitSetsMap(values, keys);
		}
	}
}
