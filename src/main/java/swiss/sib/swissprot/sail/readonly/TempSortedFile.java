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
package swiss.sib.swissprot.sail.readonly;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO.IO;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.ReducingIterator;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.ThreadSafeSecondIterator;
import swiss.sib.swissprot.sail.readonly.sorting.Comparators;

/**
 * A collection of methods to turn an unsorted file into one which is sorted. Also merging multiple of these files into
 * one.
 *
 * @author <a href="mailto:jerven.bolleman@sib.swiss">Jerven Bolleman</a>
 *
 */
public class TempSortedFile {
	private final File sogFile;
	private final File objectFile;
	private final Compression compression;
	private final IO subjectIO;
	private final IO objectIO;
	private final Comparator<byte[]> objectComparator;
	private final Comparator<byte[]> subjectComparator;

	public TempSortedFile(File file, Kind subjectKind, Kind objectKind, IRI objectDatatype, String lang,
			Compression compression) {
		super();
		this.sogFile = file;
		this.compression = compression;
		this.objectFile = new File(sogFile.getParent(), sogFile.getName() + "-objects");
		this.objectComparator = Comparators.byteComparatorFor(objectKind, objectDatatype, lang);
		this.subjectComparator = Comparators.byteComparatorFor(subjectKind);
		this.subjectIO = RawIO.forOutput(subjectKind);
		this.objectIO = RawIO.forOutput(objectKind, objectDatatype, lang);
	}

	public void from(File tempFile) throws FileNotFoundException, IOException {
		writeSubjectObjectGraphs(tempFile);
		List<byte[]> objects = sortObjects();
		writeObjects(objects.iterator());

	}

	private final record SOGToByteArrayIterator(Iterator<SubjectObjectGraph> sogs) implements Iterator<byte[]> {

		@Override
		public boolean hasNext() {
			return sogs.hasNext();
		}

		@Override
		public byte[] next() {
			return sogs.next().subject();
		}
	}

	/**
	 * Exists only because byte[].hashCode is unique per array nor per contents.
	 *
	 * We use it just in the de duplicator logic.
	 */
	private record ByteArrayHolder(byte[] data) {

		@Override
		public int hashCode() {
			return Arrays.hashCode(data);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ByteArrayHolder bah)
				return Arrays.equals(data, bah.data);
			else
				return false;
		}
	}

	private List<byte[]> sortObjects() throws IOException, FileNotFoundException {
		List<byte[]> objects = new ArrayList<>();
		// We try to avoid building huge lists of identical byte[] with this set.
		LRUSet<ByteArrayHolder> deduplicator = new LRUSet<>(1024);
		try (DataInputStream dis = openSubjectObjectGraph()) {
			Iterator<SubjectObjectGraph> iterator = new ReducingIterator<>(iterator(dis), comparator());
			while (iterator.hasNext()) {
				SubjectObjectGraph sog = iterator.next();
				byte[] rawObject = sog.object();
				if (deduplicator.add(new ByteArrayHolder(rawObject))) {
					objects.add(rawObject);
				}
			}
		}

		objects.sort(objectComparator);
		return objects;
	}

	public DataInputStream openSubjectObjectGraph() throws FileNotFoundException, IOException {
		InputStream bis = compression.decompress(sogFile);
		DataInputStream dis = new DataInputStream(bis);
		return dis;
	}

	public DataInputStream openObjects() throws FileNotFoundException, IOException {
		InputStream bis = compression.decompress(objectFile);
		DataInputStream dis = new DataInputStream(bis);
		return dis;
	}

	private void writeSubjectObjectGraphs(File tempFile) throws IOException, FileNotFoundException {
		List<SubjectObjectGraph> temp = new ArrayList<>();		
		SogComparator comparator = comparator();
		try (InputStream bis = compression.decompress(tempFile); DataInputStream dis = new DataInputStream(bis)) {
			Iterator<SubjectObjectGraph> iter =  new ReducingIterator<>(iterator(dis), comparator);
			while (iter.hasNext()) {
				SubjectObjectGraph line = iter.next();
				temp.add(line);
			}
		}


		temp.sort(comparator);
		write(temp.iterator());
	}

	private void write(Iterator<SubjectObjectGraph> iterator) throws IOException, FileNotFoundException {

		try (OutputStream fos = compression.compress(sogFile); DataOutputStream dos = new DataOutputStream(fos)) {
			while (iterator.hasNext()) {
				SubjectObjectGraph sog = iterator.next();
				write(dos, sog);
			}
		}
	}

	private void writeObjects(Iterator<byte[]> iterator) throws IOException, FileNotFoundException {

		try (OutputStream fos = compression.compress(objectFile); DataOutputStream dos = new DataOutputStream(fos)) {
			while (iterator.hasNext()) {
				byte[] object = iterator.next();
				objectIO.write(dos, object);
			}
		}
	}

	private int write(DataOutputStream dos, SubjectObjectGraph sog) throws IOException {
		int subjectLength = subjectIO.write(dos, sog.subject());
		int objectLength = objectIO.write(dos, sog.object());
		dos.writeInt(sog.graphId);
		return (Integer.BYTES) + subjectLength + objectLength;
	}

	public int write(DataOutputStream dos, Resource subject, Value object, int graphId) throws IOException {
		int subjectLength = subjectIO.write(dos, subject);
		int objectLength = objectIO.write(dos, object);
		dos.writeInt(graphId);
		return (Integer.BYTES) + subjectLength + objectLength;
	}

	private SogComparator comparator() {
		return new SogComparator(subjectComparator, objectComparator);
	}

	private SubjectObjectGraph readSog(DataInputStream dis) throws IOException {
		byte[] subject = subjectIO.readRaw(dis);
		byte[] object = objectIO.readRaw(dis);
		int graphId = dis.readInt();
		SubjectObjectGraph line = new SubjectObjectGraph(subject, object, graphId);
		assert graphId == line.graphId();
		return line;
	}

	protected static abstract class FromFileIterator<T> implements Iterator<T> {
		private final DataInputStream dis;
		private boolean done = false;

		private T next;

		private FromFileIterator(DataInputStream dis) throws IOException {
			this.dis = dis;
		}

		private T readNext() throws IOException {
			while (!done) {
				try {
					return read(dis);
				} catch (EOFException eof) {
					done = true;
				}
			}
			return null;
		}

		@Override
		public boolean hasNext() {
			if (next != null)
				return true;
			if (!done) {
				try {
					next = readNext();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			return next != null;
		}

		@Override
		public T next() {
			try {
				return next;
			} finally {
				next = null;
			}
		}

		protected abstract T read(DataInputStream dis) throws IOException;
	}

	protected static final class SogIterator extends FromFileIterator<SubjectObjectGraph> {

		private final TempSortedFile temp;

		private SogIterator(DataInputStream dis, TempSortedFile temp) throws IOException {
			super(dis);
			this.temp = temp;
		}

		protected SubjectObjectGraph read(DataInputStream dis) throws IOException {
			return temp.readSog(dis);
		}
	}

	private final record SogComparator(Comparator<byte[]> subjectComparator, Comparator<byte[]> objectComparator)
			implements Comparator<SubjectObjectGraph> {

		@Override
		public int compare(SubjectObjectGraph a, SubjectObjectGraph b) {
			int subjComparison = subjectComparator.compare(a.subject(), b.subject());
			if (subjComparison == 0) {
				int objComparison = objectComparator.compare(a.object(), b.object());
				if (objComparison == 0) {
					return Integer.compare(a.graphId(), b.graphId());
				} else {
					return objComparison;
				}
			} else {
				return subjComparison;
			}
		}
	}

	public static record SubjectObjectGraph(byte[] subject, byte[] object, int graphId) {

	};

	public void merge(List<TempSortedFile> toMerge, ExecutorService exec) throws FileNotFoundException, IOException {
		mergeSogs(toMerge, exec);
		mergeByteComparableObjects(toMerge, exec);
	}

	private void mergeByteComparableObjects(List<TempSortedFile> toMerge, ExecutorService exec)
			throws FileNotFoundException, IOException {
		List<InputStream> openedToMerge = new ArrayList<>();
		List<Iterator<byte[]>> objectIterators = new ArrayList<>();
		for (TempSortedFile temp : toMerge) {
			if (temp.file().length() > 0) {
				DataInputStream dis = temp.openObjects();
				openedToMerge.add(dis);
				objectIterators.add(wrappedIterator(temp.rawObjectIterator(dis), exec));
			}
		}
		Iterator<byte[]> mergeObjectsSort = Iterators.<byte[]>mergeDistinctSorted(objectComparator, objectIterators);
		writeObjects(mergeObjectsSort);
		for (InputStream is : openedToMerge) {
			is.close();
		}
	}

	private void mergeSogs(List<TempSortedFile> toMerge, ExecutorService exec)
			throws FileNotFoundException, IOException {
		List<InputStream> openedToMerge = new ArrayList<>();
		List<Iterator<SubjectObjectGraph>> iterators = new ArrayList<>();
		for (TempSortedFile temp : toMerge) {
			if (temp.file().length() > 0) {
				DataInputStream dis = temp.openSubjectObjectGraph();
				openedToMerge.add(dis);
				iterators.add(wrappedIterator(temp.iterator(dis), exec));
			} else {
				// removing empty file
				if (!temp.file().delete()) {
					temp.file().deleteOnExit();
				}
			}
		}
		write(Iterators.<SubjectObjectGraph>mergeDistinctSorted(new SogComparator(subjectComparator, objectComparator),
				iterators));
		for (InputStream is : openedToMerge) {
			is.close();
		}
	}

	private static <T> Iterator<T> wrappedIterator(Iterator<T> iterator, ExecutorService exec) {
		if (exec == null) {
			return iterator;
		} else {
			ThreadSafeSecondIterator<T> tssi = new ThreadSafeSecondIterator<>();
			tssi.addToQueue(() -> iterator, exec);
			return tssi;
		}
	}

	public Iterator<Value> objectIterator(DataInputStream dis) throws IOException {
		Iterator<byte[]> raw = rawObjectIterator(dis);
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return raw.hasNext();
			}

			@Override
			public Value next() {
				return objectValue(raw.next());
			}

		};
	}
	
	private final record ByteArrayToObjectValueIterator(Iterator<byte[]> raw, IO objectio) implements Iterator<Value> {
		@Override
		public boolean hasNext() {
			return raw.hasNext();
		}

		@Override
		public Value next() {
			return objectio.read(raw.next());
		}		
	}

	public Iterator<byte[]> rawObjectIterator(DataInputStream dis) throws IOException {
		try {
			return new FromFileIterator<>(dis) {

				@Override
				protected byte[] read(DataInputStream dis) throws IOException {
					return objectIO.readRaw(dis);
				}
			};
		} catch (EOFException e) {
			return Collections.emptyIterator();
		}
	}

	public Iterator<SubjectObjectGraph> iterator(DataInputStream dis) throws IOException {
		try {
			return new SogIterator(dis, this);
		} catch (EOFException e) {
			return Collections.emptyIterator();
		}
	}

	public Iterator<Resource> subjectIterator(DataInputStream dis) throws IOException {

		Iterator<SubjectObjectGraph> sogs = iterator(dis);
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return sogs.hasNext();
			}

			@Override
			public Resource next() {
				SubjectObjectGraph next = sogs.next();
				return (Resource) subjectIO.read(next.subject());
			}
		};
	}

	public File file() {
		return sogFile;
	}

	public void delete() {
		sogFile.delete();
		objectFile.delete();
	}

	public Iterator<? extends Value> distinctSubjectIterator(DataInputStream dis) throws IOException {
		try {
			Iterator<byte[]> r = rawDistinctSubjectIterator(dis);
			return new Iterator<Value>() {

				@Override
				public boolean hasNext() {
					return r.hasNext();
				}

				@Override
				public Value next() {
					byte[] n = r.next();
					return subjectIO.read(n);
				}
			};
		} catch (EOFException e) {
			return Collections.emptyIterator();
		}
	}

	public Iterator<byte[]> rawDistinctSubjectIterator(DataInputStream dis) throws IOException {

		Iterator<SubjectObjectGraph> sogs = iterator(dis);
		Iterator<byte[]> rawSubjects = new SOGToByteArrayIterator(sogs);

		return new ReducingIterator<>(rawSubjects, subjectComparator);
	}

	public Value objectValue(byte[] object) {
		return objectIO.read(object);
	}

	public Resource subjectValue(byte[] subject) {
		return (Resource) subjectIO.read(subject);
	}
}
