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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;

import swiss.sib.swissprot.sail.readonly.Compression.CompressionTask;
import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.BufferUtils;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO.IO;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.sorting.Comparators;

public class SortedListInSections<T> implements SortedList<T> {

	public static final int SECTION_SIZE = 1024 * 2;
	private final List<Section<T>> sections;
	private final Function<byte[], T> reconstructor;
	private final Function<T, byte[]> deconstructor;
	private final Comparator<byte[]> comparator;

	public SortedListInSections(List<Section<T>> sections, Function<byte[], T> reconstructor,
			Function<T, byte[]> deconstrutor, Comparator<byte[]> comparator, File backingFile) {
		this.sections = sections;
		this.reconstructor = reconstructor;
		this.deconstructor = deconstrutor;
		this.comparator = comparator;
	}

	public static void rewrite(Iterator<byte[]> sortedInput, File targetFile) throws IOException {
		try (FileOutputStream fos = new FileOutputStream(targetFile);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				SectionOutputStream<?> sos = new SectionOutputStream<>(bos)) {
			rewrite(sortedInput, sos);
		}
	}

	private static void rewrite(Iterator<byte[]> sortedInput, SectionOutputStream<?> sos) throws IOException {
		List<byte[]> lines = new ArrayList<>(SECTION_SIZE);
		while (sortedInput.hasNext()) {
			byte[] bl = sortedInput.next();
			lines.add(bl);
			if (lines.size() == (int) SECTION_SIZE) {
				sos.write(lines);
				lines = new ArrayList<>(SECTION_SIZE);
			}
		}
		if (!lines.isEmpty()) {
			sos.write(lines);
			lines = new ArrayList<>(SECTION_SIZE);
		}
	}

	public static void rewriteIRIs(Iterator<IRI> sortedInput, File targetFile, Function<IRI, byte[]> deconstructor)
			throws IOException {
		rewrite(Iterators.map(sortedInput, deconstructor), targetFile);
	}

	public static void rewriteValues(Iterator<Value> sortedInput, File targetFile,
			Function<Value, byte[]> deconstructor) throws IOException {
		rewrite(Iterators.map(sortedInput, deconstructor), targetFile);
	}

	private static final int MAX_QUEUED_COMPRESSION_TASKS = Runtime.getRuntime().availableProcessors() * 2;

	private static class Lz4SectionCompressionTask extends CompressionTask<List<byte[]>> {

		public Lz4SectionCompressionTask(List<byte[]> input) {
			super(input);
		}

		@Override
		public byte[] compressAction(List<byte[]> input) {
			try {
				if (input == null)
					return output;
				return LZ4Section.compress(input);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}

	private static final class SectionOutputStream<T> implements AutoCloseable {
		private final OutputStream wrapped;
		private final ExecutorService execs = Executors
				.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() - 2));
		private final Deque<Lz4SectionCompressionTask> cts = new ArrayDeque<>(MAX_QUEUED_COMPRESSION_TASKS);

		public SectionOutputStream(OutputStream os) {
			super();
			this.wrapped = os;
		}

		private void submitCompressionTask(List<byte[]> toCompress) throws IOException {
			Lz4SectionCompressionTask ct = new Lz4SectionCompressionTask(toCompress);
			cts.add(ct);
			execs.submit(ct::attemptCompress);
			if (cts.size() > MAX_QUEUED_COMPRESSION_TASKS) {
				Lz4SectionCompressionTask first = cts.pollFirst();
				CompressionTask.writeCompressionTask(first, wrapped);
			}
		}

		public void write(List<byte[]> lines) throws IOException {
			submitCompressionTask(lines);
		}

		@Override
		public void close() throws IOException {
			while (!cts.isEmpty()) {
				Lz4SectionCompressionTask first = cts.pollFirst();
				CompressionTask.writeCompressionTask(first, wrapped);
			}
			wrapped.close();
			execs.shutdown();
		}
	}

	public static SortedList<String> readinStrings(File target) throws FileNotFoundException, IOException {
		return readin(target, SortedListInSections::utf8bytesToString, SortedListInSections::stringToUtf8bytes,
				Comparators.forRawBytesOfDatatype(CoreDatatype.XSD.STRING.getIri()));
	}

	public static SortedList<Value> readinIris(File target) throws FileNotFoundException, IOException {
		IO io = RawIO.forOutput(Kind.IRI);
		return readin(target, io::read, io::getBytes, Comparators.forIRIBytes());
	}

	public static SortedList<Value> readinBNodes(File target) throws FileNotFoundException, IOException {
		IO io = RawIO.forOutput(Kind.BNODE);
		return readin(target, io::read, io::getBytes, Comparators.forBNodeBytes());
	}

	public static SortedList<Value> readinValues(File target, IRI datatype) throws FileNotFoundException, IOException {
		IO io = RawIO.forOutput(Kind.LITERAL, datatype, null);
		return readin(target, io::read, io::getBytes, Comparators.forRawBytesOfDatatype(datatype));
	}

	public static SortedList<Value> readinValues(File target, String lang) throws FileNotFoundException, IOException {
		IO io = RawIO.forOutput(Kind.LITERAL, null, lang);
		return readin(target, io::read, io::getBytes, Comparators.forLangStringBytes());
	}

	private static String utf8bytesToString(byte[] b) {
		return new String(b, StandardCharsets.UTF_8);
	}

	private static byte[] stringToUtf8bytes(String s) {
		return s.getBytes(StandardCharsets.UTF_8);
	}

	public static <T> SortedList<T> readin(File target, Function<byte[], T> reconstructor,
			Function<T, byte[]> deconstructor, Comparator<byte[]> comparator)
			throws FileNotFoundException, IOException {
		long size = target.length();
		List<Section<T>> sections = new ArrayList<>();
		ByteBuffer[] buffers = BufferUtils.openByteBuffer(target.toPath());
		try (FileInputStream fis = new FileInputStream(target);
				BufferedInputStream bis = new BufferedInputStream(fis);) {
			long at = 0;
			long sectionId = 0;
			Section<T> previous = null;
			while (at < size) {
				Section<T> section = LZ4Section.read(bis, sectionId, at, buffers, reconstructor, deconstructor,
						comparator);
				at += section.sizeOnDisk();
				sections.add(section);
				sectionId++;
				if (previous != null) {
					assert comparator.compare(previous.first(), section.first()) < 0;
				}
				previous = section;
			}
		}
		return new SortedListInSections<>(sections, reconstructor, deconstructor, comparator, target);
	}

	@Override
	public long positionOf(T element) throws IOException {
		int binarySearch = sectionIndexOf(element);
		// A positive means it is the first element of a section.
		if (binarySearch >= 0) {
			return sections.get(binarySearch).get(0).position();
		} else {
			// a negative value gives "where" it should have been if it was a first element
			// of a section.
			// translate it into a sectionId.
			int sectionId = -(binarySearch + 2);
			if (sectionId < 0 || sectionId > sections.size())
				return WriteOnce.NOT_FOUND;
			Section<T> presentIn = sections.get(sectionId);
			String start = new String(presentIn.first());
			assert start != null;
			TPosition<T> foundByBinarySearch = presentIn.findByBinarySearch(element);
			if (foundByBinarySearch == null)
				return WriteOnce.NOT_FOUND;
			else
				return foundByBinarySearch.position();
		}
	}

	private int sectionIndexOf(T element) {
		byte[] deconstructed = deconstructor.apply(element);
		Section<T> searchFor = new LZ4Section<>(0, deconstructed, null, WriteOnce.NOT_FOUND, reconstructor,
				deconstructor, comparator);
		int binarySearch = Collections.binarySearch(sections, searchFor, this::compareSectionAsRange);
		return binarySearch;
	}

	@Override
	public IterateSectionsInSortedOrder iterator() throws IOException {
		return new IterateSectionsInSortedOrder(sections.listIterator());
	}

	public Function<T, TPosition<T>> searchInOrder() throws IOException {
		SearchInSortedOrder findInSortedOrder = new SearchInSortedOrder();
		return findInSortedOrder::search;
	}

	private class SearchInSortedOrder {
		private TPosition<T> previous;
		private Section<T> previousSection;

		public SearchInSortedOrder() {
			super();
		}

		public TPosition<T> search(T t) {
			assert t != null;
			if (previous != null && previous.t().equals(t)) {
				return previous;
			} else if (previousSection != null) {
				long findPositionByBinarySearch = previousSection.findPositionByBinarySearch(t);
				if (findPositionByBinarySearch != WriteOnce.NOT_FOUND) {
					return new TPosition<>(t, findPositionByBinarySearch);
				}
			}
			long positionOf;
			try {
				positionOf = positionOf(t);
				if (positionOf == WriteOnce.NOT_FOUND) {
					assert false : "Could not find " + t + " in store";
					return null;
				} else {
					previousSection = sections.get(extractSectionFromId(positionOf));
					return new TPosition<>(t, positionOf);
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not find " + t + " in store", e);
			}

		}
	}

	public final class IterateSectionsInSortedOrder implements IterateInSortedOrder<T> {

		private final ListIterator<Section<T>> sectionIterator;
		private ListIterator<TPosition<T>> current;
		private TPosition<T> previous;

		public IterateSectionsInSortedOrder(ListIterator<Section<T>> sectionIterator) throws FileNotFoundException {
			super();
			this.sectionIterator = sectionIterator;
		}

		@Override
		public boolean hasNext() {
			if (current != null && current.hasNext())
				return true;
			while (sectionIterator.hasNext()) {
				current = sectionIterator.next().listIterator();
				if (current.hasNext())
					return true;
			}

			return false;
		}

		@Override
		public TPosition<T> next() {
			TPosition<T> next = current.next();
			previous = next;
			return next;
		}

		public void advanceNear(T t) {
			if (previous != null && previous.t().equals(t)) {
				return;
			} else {
				if (current == null && sectionIterator.hasNext()) {
					current = sectionIterator.next().listIterator();
				}
				int nowNextIndex = sectionIterator.nextIndex();
				if (sectionIterator.hasNext()) {
					byte[] b = deconstructor.apply(t);
					while (sectionIterator.hasNext()) {
						Section<T> next = sectionIterator.next();
						if (comparator.compare(next.first(), b) >= 0) {
							Section<T> rewound = sectionIterator.previous();
							if (nowNextIndex != sectionIterator.nextIndex()) {
								current = rewound.listIterator();
							}
							return;
						}
					}
				}
			}
		}
	}

	private int compareSectionAsRange(Section<T> incol, Section<T> other) {
		return comparator.compare(incol.first(), other.first());
	}

	@Override
	public T get(long id) {
		if (id == WriteOnce.NOT_FOUND) {
			return null;
		}
		int section = extractSectionFromId(id);
		return sections.get(section).get((int) (id % SECTION_SIZE)).t();
	}

	private int extractSectionFromId(long id) {
		int section = (int) (id / SECTION_SIZE);
		return section;
	}
}
