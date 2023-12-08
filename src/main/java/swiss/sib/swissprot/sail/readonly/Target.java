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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.sail.readonly.WriteOnce.GraphIdIri;
import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.storing.TemporaryGraphIdMap;

final class Target implements AutoCloseable {
	private final Compression tempCompression;
	private static final int MERGE_X_FILES = 64;
	private final int maxTempFiles;
	private static final Logger logger = LoggerFactory.getLogger(Target.class);
	protected static final long SWITCH_TO_NEW_FILE = 128l * 1024l * 1024l;
	private final Kind subjectKind;
	private final Kind objectKind;
	private final IRI datatype;
	private final String lang;
	private final File targetTripleFile;
	private final TemporaryGraphIdMap tgid;
	private final ExecutorService exec;
	private final ExecutorService fileMergeExec = Executors.newVirtualThreadPerTaskExecutor();

	private final Map<Integer, List<TempSortedFile>> sortedTempFilesByMergeLevel = new ConcurrentHashMap<>();
	private final List<Future<IOException>> running = newThreadSafeList();
	private final FileAndWriter[] tripleWriters;
	private volatile int tempFileNamerCount = 0;
	private volatile int toWriteTo = 0;
	private final Lock mergeLock = new ReentrantLock();
	private final Semaphore sortPressureLimit;
	private volatile boolean closed = false;
	private final IRI predicate;

	private class FileAndWriter {

		private final int id;
		private volatile File tempFile;
		private volatile DataOutputStream writer;
		private final AtomicLong wrote = new AtomicLong(0);
		private final Lock lock = new ReentrantLock();
		private TempSortedFile toSort;

		public FileAndWriter(int id) {
			super();
			this.id = id;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof FileAndWriter faw) {
				return id == faw.id;
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return id;
		}

		public long write(Resource subjectS, Value objectS, int graphId) throws IOException {
			return wrote.addAndGet(toSort.write(writer, subjectS, objectS, graphId));
		}

		public void close() throws IOException {
			writer.close();
			wrote.set(0);
		}

		public void reopenTripleWriter() throws IOException {
			int id = tempFileNamerCount++;
			this.tempFile = new File(targetTripleFile.getParentFile(),
					Compression.removeExtension(targetTripleFile.getName()) + '-' + id + tempCompression.extension());
			this.writer = new DataOutputStream(tempCompression.compress(tempFile));
			this.toSort = new TempSortedFile(tempFile, subjectKind, objectKind, datatype, lang, tempCompression);

		}

		public void lock() {
			lock.lock();
		}

		public boolean tryLock() {
			return lock.tryLock();
		}

		public void unlock() {
			lock.unlock();
		}

	};

	Target(Statement template, File directory, TemporaryGraphIdMap tgid) throws IOException {
		this(template, directory, tgid, Executors.newSingleThreadExecutor(), 1, new Semaphore(1), Compression.LZ4);
	}

	Target(Statement template, File directory, TemporaryGraphIdMap tgid, ExecutorService exec, int maxTempFiles,
			Semaphore sortPressureLimit, Compression tempCompression) throws IOException {
		this.tgid = tgid;
		this.exec = exec;
		this.maxTempFiles = maxTempFiles;
		this.sortPressureLimit = sortPressureLimit;
		this.subjectKind = Kind.of(template.getSubject());
		this.objectKind = Kind.of(template.getObject());
		this.predicate = template.getPredicate();
		this.tempCompression = tempCompression;
		File subdirectory = new File(directory, subjectKind.label());

		if (!subdirectory.isDirectory() && !subdirectory.mkdir())
			throw new IOException("Can't make " + subdirectory.getAbsolutePath());
		if (objectKind != Kind.LITERAL) {
			lang = null;
			datatype = null;
			targetTripleFile = newFile(subdirectory, objectKind);
		} else {
			Literal lit = (Literal) template.getObject();
			lang = lit.getLanguage().orElse(null);
			datatype = lit.getDatatype();
			targetTripleFile = newFile(subdirectory, fileNameForTarget(lang, datatype, objectKind));
		}
		tripleWriters = new FileAndWriter[this.maxTempFiles];
		for (int i = 0; i < this.maxTempFiles; i++) {
			tripleWriters[i] = new FileAndWriter(i);
			tripleWriters[i].reopenTripleWriter();
		}
		sortedTempFilesByMergeLevel.put(0, newThreadSafeList());
	}

	private static <T> List<T> newThreadSafeList(List<T> list) {
		return Collections.synchronizedList(new ArrayList<>(list));
	}

	private static <T> List<T> newThreadSafeList() {
		return Collections.synchronizedList(new ArrayList<>());
	}

	private IOException sortTempFile(File tempFile) {
		try {
			File sortedTempFile = new File(tempFile.getParent(),
					Compression.removeExtension(tempFile.getName()) + ".sorted" + tempCompression.extension());
			TempSortedFile tempSortedFile = new TempSortedFile(sortedTempFile, subjectKind, objectKind, datatype, lang,
					tempCompression);
			tempSortedFile.from(tempFile);
			tempFile.delete();
			addToMergeLevelZero(tempSortedFile);
			mergeIfNeeded();
		} catch (IOException e) {
			logger.error("can't sort", e);
			return e;
		}
		return null;
	}

	private void addToMergeLevelZero(TempSortedFile sortedTempFile) {
		try {
			mergeLock.lock();
			sortedTempFilesByMergeLevel.get(0).add(sortedTempFile);
		} finally {
			mergeLock.unlock();
		}
	}

	private void mergeIfNeeded() throws IOException {
		List<Integer> keys = new ArrayList<>(sortedTempFilesByMergeLevel.keySet());
		for (int level : keys) {
			try {
				mergeLock.lock();
				List<TempSortedFile> list = sortedTempFilesByMergeLevel.get(level);
				if (list.size() > MERGE_X_FILES && !closed) {
					mergeFiles(level, newThreadSafeList(list));
					list.clear();
				}
			} finally {
				mergeLock.unlock();
			}
		}

	}

	private void mergeFiles(int level, List<TempSortedFile> toMerge) throws IOException {
		running.add(fileMergeExec.submit(new FileMerger(level, toMerge)));
	}

	private class FileMerger implements Callable<IOException> {
		private final int level;
		private final List<TempSortedFile> toMerge;

		public FileMerger(int level, List<TempSortedFile> toMerge) {
			super();
			this.level = level;
			this.toMerge = toMerge;
		}

		public IOException call() {
			try {
				int nextLevel = level + 1;
				File firstFile = toMerge.get(0).file();
				File sortedTempFile = new File(firstFile.getParent(), firstFile.getName() + "-" + nextLevel);
				TempSortedFile tempSortedFile = new TempSortedFile(sortedTempFile, subjectKind, objectKind, datatype,
						lang, tempCompression);
				tempSortedFile.merge(toMerge, null);

				for (TempSortedFile merged : toMerge) {
					merged.delete();
				}
				try {
					mergeLock.lock();
					if (!sortedTempFilesByMergeLevel.containsKey(nextLevel)) {
						sortedTempFilesByMergeLevel.put(nextLevel, newThreadSafeList());
					}
					sortedTempFilesByMergeLevel.get(nextLevel).add(tempSortedFile);
				} finally {
					mergeLock.unlock();
				}
			} catch (IOException e) {
				return e;
			}
			return null;
		}
	}

	private void sortTempFilesIntoTarget() throws IOException {
		try {
			mergeLock.lock();
			List<TempSortedFile> collect = sortedTempFilesByMergeLevel.values().stream().flatMap(List::stream)
					.collect(Collectors.toList());
			TempSortedFile tempSortedFile = new TempSortedFile(targetTripleFile, subjectKind, objectKind, datatype,
					lang, tempCompression);
			tempSortedFile.merge(collect, exec);
			for (TempSortedFile tsf : collect)
				tsf.delete();
		} finally {
			mergeLock.lock();
		}
	}

	private static String fileNameForTarget(String lang, IRI datatype, Kind objectKind) throws IOException {
		if (lang != null || datatype != null) {
			return ReadOnlyLiteralStore.fileNameForLiteral(datatype, lang);
		} else {
			return objectKind.label();
		}
	}

	private File newFile(File subdirectory, String string) throws IOException {

		File file = new File(subdirectory, string + tempCompression.extension());
		if (file.exists() && !file.delete())
			throw new IOException("Previous file exists and can't be removed:" + file);
		return file;
	}

	private File newFile(File subdirectory, Kind of) throws IOException {
		return newFile(subdirectory, of.label());
	}

	@Override
	public void close() throws IOException {
		closed = true;
		for (FileAndWriter faw : tripleWriters) {
			try {
				faw.lock();
				faw.close();
				if (faw.tempFile.length() > 0)
					running.add(exec.submit(() -> sortTempFile(faw.tempFile)));
			} finally {
				faw.unlock();
			}
		}
		ExternalProcessHelper.waitForFutures(running);
		sortTempFilesIntoTarget();
	}

	/**
	 * Test if the statement may be written by this target.
	 *
	 * @param statement
	 * @return true if it is accepted
	 */
	public boolean testForAcceptance(Statement statement) {
		if (predicate.equals(statement.getPredicate())) {
			if (Kind.of(statement.getSubject()) != this.subjectKind) {
				return false;
			} else if (Kind.of(statement.getObject()) != this.objectKind) {
				return false;
			} else if (this.objectKind == Kind.LITERAL) {
				Literal lit = (Literal) statement.getObject();
				String otherLang = lit.getLanguage().orElse(null);
				IRI otherDatatype = lit.getDatatype();
				boolean langEq = Objects.equals(this.lang, otherLang);
				boolean datatypeEq = Objects.equals(this.datatype, otherDatatype);
				return langEq && datatypeEq;
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Used for faster hashcoding and lookups.
	 *
	 * @param statement
	 * @return a new TargetKey that matches this statement
	 */
	public static TargetKey key(Statement statement) {
		Kind objectKind = Kind.of(statement.getObject());
		if (objectKind != Kind.LITERAL) {
			return new TargetKey(Kind.of(statement.getSubject()), objectKind, null, null);
		} else {
			Literal lit = (Literal) statement.getObject();
			String otherLang = lit.getLanguage().orElse(null);
			IRI otherDatatype = lit.getDatatype();
			return new TargetKey(Kind.of(statement.getSubject()), objectKind, otherLang, otherDatatype);
		}
	}

	public static record TargetKey(Kind subjectKind, Kind objectKind, String otherLang, IRI otherDatatype) {
	}

	public void write(Statement statement) throws IOException {
		Resource subjectS = statement.getSubject();
		Value objectS = statement.getObject();
		if (statement.getContext() != null) {
			int tempGraphId = getTemporaryGraphId(statement);
			write(subjectS, objectS, tempGraphId);
		} else {
			WriteOnce.Failures.NO_GRAPH.exit();
		}
	}

	private int getTemporaryGraphId(Statement statement) {
		int tempGraphId;
		Resource context = statement.getContext();
		if (context instanceof GraphIdIri t) {
			tempGraphId = t.id();
		} else {
			tempGraphId = tgid.tempGraphId(context);
		}
		return tempGraphId;
	}

	private void write(Resource subjectS, Value objectS, int tempGraphId) throws IOException, FileNotFoundException {

		if (!writeIntoFirstUnlocked(subjectS, objectS, tempGraphId)) {
			writeIntoRandomLocked(subjectS, objectS, tempGraphId);
		}
	}

	/*
	 * This waits if needed
	 */
	private void writeIntoRandomLocked(Resource subjectS, Value objectS, int tempGraphId) throws IOException {
		int l = Math.abs(toWriteTo++ % tripleWriters.length);
		FileAndWriter fileAndWriter = tripleWriters[l];
		try {
			fileAndWriter.lock();
			writeIntoLockedFile(subjectS, objectS, tempGraphId, fileAndWriter);
			return;
		} finally {
			fileAndWriter.unlock();
		}
	}

	/**
	 * Tries to write in the first file that is not locked.
	 *
	 * @param subjectS
	 * @param objectS
	 * @param tempGraphId
	 * @return
	 * @throws IOException
	 */
	private boolean writeIntoFirstUnlocked(Resource subjectS, Value objectS, int tempGraphId) throws IOException {
		for (int i = 0; i < tripleWriters.length; i++) {
			FileAndWriter fileAndWriter = tripleWriters[i];
			if (fileAndWriter.tryLock()) {
				try {
					writeIntoLockedFile(subjectS, objectS, tempGraphId, fileAndWriter);
					return true;
				} finally {
					fileAndWriter.unlock();
				}
			}
		}
		return false;
	}

	private void writeIntoLockedFile(Resource subjectS, Value objectS, int tempGraphId, FileAndWriter fileAndWriter)
			throws IOException {
		long wrote = fileAndWriter.write(subjectS, objectS, tempGraphId);

		if (wrote > SWITCH_TO_NEW_FILE && !closed) {
			fileAndWriter.close();
			File tempFile = fileAndWriter.tempFile;

			running.add(exec.submit(() -> {
				try {
					sortPressureLimit.acquireUninterruptibly();
					return sortTempFile(tempFile);
				} finally {
					sortPressureLimit.release();
				}
			}));

			fileAndWriter.reopenTripleWriter();
		}
	}

	public Kind subjectKind() {
		return subjectKind;
	}

	public Kind objectKind() {
		return objectKind;
	}

	public TempSortedFile getTripleFile() {
		return new TempSortedFile(targetTripleFile, subjectKind, objectKind, datatype, lang, tempCompression);
	}

	public File getTripleFinalFile() {
		String name = Compression.removeExtension(targetTripleFile.getName());
		return new File(targetTripleFile.getParentFile(), name);
	}

	public IRI getDatatype() {
		return datatype;
	}

	public String getLang() {
		return lang;
	}
}
