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

import static swiss.sib.swissprot.sail.readonly.ExternalProcessHelper.waitForProcessToBeDone;
import static swiss.sib.swissprot.sail.readonly.FileNames.IRIS_FILE_NAME;
import static swiss.sib.swissprot.sail.readonly.FileNames.PREDICATES_FILE_NAME;
import static swiss.sib.swissprot.sail.readonly.FileNames.PRED_DIR_PREFIX;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.XMLParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.sail.readonly.Target.TargetKey;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO.IO;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.ReducingIterator;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.ThreadSafeSecondIterator;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedListInSections;
import swiss.sib.swissprot.sail.readonly.sorting.Comparators;
import swiss.sib.swissprot.sail.readonly.storing.TemporaryGraphIdMap;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyBlankNode;

public class WriteOnce implements AutoCloseable {

	public static final Compression COMPRESSION = Compression.LZ4;

	private static final Logger logger = LoggerFactory.getLogger(WriteOnce.class);
	static final char fieldSep = '\t';

	private static SimpleValueFactory vf = SimpleValueFactory.getInstance();

	private final int step;
	private static final AtomicLong BNODE_ID_NORMALIZER = new AtomicLong();
	public static final long NOT_FOUND = -404;
	private final File directoryToWriteToo;
	private final File iriFile;
	private final File predFile;
	private final Set<IRI> predicatesInOrderOfSeen = Collections.synchronizedSet(new LinkedHashSet<>());
	private final Map<IRI, PredicateDirectoryWriter> predicatesDirectories = new ConcurrentHashMap<>();
	private volatile TemporaryGraphIdMap temporaryGraphIdMap = new TemporaryGraphIdMap();

	private final ExecutorService exec = Executors.newCachedThreadPool();
	/**
	 * Try to select a reasonable number of concurrent parse threads to actually
	 * run.
	 */
	private final Semaphore parsePresureLimit;
	/**
	 * The sort pressure limit is there to make sure the files being sorted fit in
	 * the java heap and avoids issues with OutOfMemory
	 */
	private final Semaphore sortPresureLimit;
	private final int concurrentTargetFiles;
	/**
	 * Lock to protect the maps predicateInOrderOfSeen and predcateDirectories.
	 */
	private final Lock predicateSeenLock = new ReentrantLock();

	private final Compression tempCompression;
	private static final Compression FINAL_COMPRESSION = Compression.LZ4;

	/**
	 * Error exit codes.
	 */
	public static enum Failures {
		UNKOWN_FORMAT(1), GENERIC_RDF_PARSE_IO_ERROR(2), CUT_SORT_UNIQ_IO(4), GENERIC_RDF_PARSE_ERROR(5),
		TO_LOAD_FILE_NOT_CORRECT(7), NOT_DONE_YET(8), NO_GRAPH(9), GRAPH_ID_NOT_IRI(10);

		private final int exitCode;

		Failures(int i) {
			this.exitCode = i;
		}

		public void exit() {
			System.exit(exitCode);
		}
	}

	public WriteOnce(File directoryToWriteToo, int step, Compression tempCompression) throws IOException {
		super();
		this.directoryToWriteToo = directoryToWriteToo;
		this.tempCompression = tempCompression;
		if (!directoryToWriteToo.exists()) {
			directoryToWriteToo.mkdirs();
		}
		this.step = step;
		this.iriFile = new File(directoryToWriteToo, IRIS_FILE_NAME);
		this.predFile = new File(directoryToWriteToo, PREDICATES_FILE_NAME);

		int procs = Runtime.getRuntime().availableProcessors();
		int estimateParsingProcessors = estimateParsingProcessors(procs);
		parsePresureLimit = new Semaphore(estimateParsingProcessors);
		int estimatedSorters = estimateMaxConcurrentSorters(Runtime.getRuntime().maxMemory(), procs);
		sortPresureLimit = new Semaphore(Math.max(1, Math.min(estimatedSorters, procs)));
		logger.info("Running " + estimateParsingProcessors + " loaders and  " + estimatedSorters + " sorters");
		concurrentTargetFiles = Math.max(1, estimateParsingProcessors / 4);
	}

	private int estimateParsingProcessors(int procs) {
		return Math.max(1, (procs / 4) * 3);
	}

	private int estimateMaxConcurrentSorters(long maxMemory, int procs) {
		long expectedWorstCaseMemoryPresure = Target.SWITCH_TO_NEW_FILE * 8;
		int b = (int) (maxMemory / expectedWorstCaseMemoryPresure);
		return Math.max(1, Math.min(b, procs / 4));
	}

	public static void main(String args[]) throws IOException {
		String fileDescribedToLoad = args[0];
		File directoryToWriteToo = new File(args[1]);
		Path path = Paths.get(fileDescribedToLoad);
		List<String> lines = Files.readAllLines(path);
		int step = Integer.parseInt(args[2]);
		Compression tempCompression = Compression.fromExtension(args[3]);
		try (WriteOnce wo = new WriteOnce(directoryToWriteToo, step, tempCompression)) {
			wo.parse(lines);
		} catch (IOException e) {
			logger.error("io", e);
		}
	}

	private void parseFilesIntoPerPredicateType(List<String> lines, List<Future<?>> toRun, CountDownLatch latch) {
		// We shuffle to increase the likelihood different kind of files are processed
		// at the same time.
		// files that are different often have different sets of predicates.
		Collections.shuffle(lines);

		for (String line : lines) {
			String[] fileGraph = line.split("\t");
			try {
				IRI graph = vf.createIRI(fileGraph[1]);
				String fileName = fileGraph[0];
				Optional<RDFFormat> parserFormatForFileName = Rio.getParserFormatForFileName(fileName);
				if (!parserFormatForFileName.isPresent()) {
					logger.error("Starting parsing of " + fileName + " failed because we can't guess format");
					Failures.UNKOWN_FORMAT.exit();
				} else {
					logger.info("Submitting parsing of " + fileName + " at " + Instant.now() + " with format "
							+ parserFormatForFileName.get());
					toRun.add(exec.submit(() -> parseInThread(latch, graph, fileName, parserFormatForFileName)));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				logger.error("Error in to load file at line : " + line);
				Failures.TO_LOAD_FILE_NOT_CORRECT.exit();
			}

		}
		WAIT: try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.interrupted();
			break WAIT;
		}
	}

	private void parseInThread(CountDownLatch latch, IRI graph, String fileName,
			Optional<RDFFormat> parserFormatForFileName) {
		try {
			parsePresureLimit.acquireUninterruptibly();
			parse(this, graph, fileName, parserFormatForFileName);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			Failures.GENERIC_RDF_PARSE_IO_ERROR.exit();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			Failures.GENERIC_RDF_PARSE_ERROR.exit();
		} finally {
			parsePresureLimit.release();
		}
		latch.countDown();
	}

	private static void parse(WriteOnce wo, IRI graph, String fileName, Optional<RDFFormat> parserFormatForFileName)
			throws IOException {
		
		RDFParser parser = Rio.createParser(parserFormatForFileName.get(), SimpleValueFactory.getInstance());
	
		ParserConfig pc = parser.getParserConfig();
		pc.set(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID, false);
		pc.set(XMLParserSettings.FAIL_ON_INVALID_QNAME, false);
		pc.set(XMLParserSettings.FAIL_ON_INVALID_NCNAME, false);
		pc.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
		// TODO support rdf-star.
		pc.set(BasicParserSettings.PROCESS_ENCODED_RDF_STAR, false);
		pc.setNonFatalErrors(Set.of(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID));
		
		try {
			Instant start = Instant.now();
			logger.info("Starting parsing of " + fileName + " at " + start);
			parser.setRDFHandler(wo.newHandler(graph));
			if (fileName.endsWith(".gz")) {
				Process cat = Compression.GZIP.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".bz2")) {
				Process cat = Compression.BZIP2.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".xz")) {
				Process cat = Compression.XZ.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".lz4")) {
				Process cat = Compression.LZ4.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else if (fileName.endsWith(".zstd") || fileName.endsWith(".zst")) {
				Process cat = Compression.ZSTD.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			} else {
				Process cat = Compression.NONE.decompressInExternalProcess(new File(fileName));
				parseWithInputViaCat(graph, parser, cat);
			}
			Instant end = Instant.now();
			logger.info("Finished parsing of " + fileName + " which took" + Duration.between(start, end) + "at " + end);
		} catch (RDF4JException e) {
			logger.error(e.getMessage() + " for " + fileName);
		} catch (RuntimeException e) {
			e.printStackTrace();
			logger.error(e.getMessage() + " for " + fileName);
		}
	}

	private static void parseWithInputViaCat(IRI graph, RDFParser parser, Process cat) throws IOException {

		try (InputStream gis = cat.getInputStream(); InputStream bis = new BufferedInputStream(gis, 128 * 1024)) {
			parser.parse(bis, graph.stringValue());
		}
		waitForProcessToBeDone(cat);
	}

	private RDFHandler newHandler(IRI graph) {
		return new Handler(new GraphIdIri(graph, temporaryGraphIdMap.tempGraphId(graph)), tempCompression);
	}

	/**
	 * This class reduces contention on the tempGraphId map. Or in practical terms
	 * saves a billion or so string hashCode operations etc.
	 */
	static class GraphIdIri implements IRI {

		private static final long serialVersionUID = 1L;
		private final IRI wrapped;
		private final int id;

		public GraphIdIri(IRI wrapped, int id) {
			super();
			this.wrapped = wrapped;
			this.id = id;
		}

		@Override
		public String stringValue() {
			return wrapped.stringValue();
		}

		@Override
		public String getNamespace() {
			return wrapped.getNamespace();
		}

		@Override
		public String getLocalName() {
			return wrapped.getLocalName();
		}

		@Override
		public int hashCode() {
			return wrapped.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (obj instanceof GraphIdIri)
				return id == ((GraphIdIri) obj).id;
			if (obj instanceof IRI other)
				return wrapped.equals(other);
			return false;
		}

		/**
		 * @return the temporary id associated with this IRI as a Graph
		 */
		public int id() {
			return id;
		}
	}

	private void cleanUpTempSortedFiles(Map<IRI, PredicateDirectoryWriter> pdws) {
		Instant start = Instant.now();
		for (PredicateDirectoryWriter pdw : pdws.values()) {
			for (Target t : pdw.getTargets()) {
				TempSortedFile tf = t.getTripleFile();
				tf.delete();
			}
		}
		logger.info("step 6 took " + Duration.between(start, Instant.now()));
	}

	private void makeUniqueSortedIriList(File directoryToWriteToo, Map<IRI, PredicateDirectoryWriter> pdws)
			throws IOException {
		List<InputStream> disses = new ArrayList<>();
		List<Iterator<byte[]>> iris = new ArrayList<>();

		for (PredicateDirectoryWriter pdw : pdws.values()) {
			for (Target t : pdw.getTargets()) {
				TempSortedFile tf = t.getTripleFile();
				if (t.subjectKind() == Kind.IRI) {
					DataInputStream dis = tf.openSubjectObjectGraph();
					disses.add(dis);
					ThreadSafeSecondIterator<byte[]> tssi = new ThreadSafeSecondIterator<>();
					tssi.addToQueue(() -> {
						try {
							return tf.rawDistinctSubjectIterator(dis);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}, exec);
					iris.add(tssi);
				}
				if (t.objectKind() == Kind.IRI) {
					DataInputStream dis = tf.openObjects();
					disses.add(dis);
					ThreadSafeSecondIterator<byte[]> tssi = new ThreadSafeSecondIterator<>();
					tssi.addToQueue(() -> {
						try {
							return tf.rawObjectIterator(dis);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}, exec);
					iris.add(tssi);
				}
			}
		}
		Set<IRI> predsAndGraphs = ensurePredicateAndGraphIrisAreIncluded();

		IO io = RawIO.forOutput(Kind.IRI);
		iris.add(predsAndGraphs.stream().map(io::getBytes).sorted(Comparators.forIRIBytes()).iterator());

		Instant start = Instant.now();
		Iterator<byte[]> mergeUniquePreSorted = Iterators.mergeDistinctSorted(Comparators.forIRIBytes(), iris);
		ToFinalDiskForm.makeSortedRawIRIList(mergeUniquePreSorted, iriFile);
		logger.info("Finished merge sorting all unique iris: " + Duration.between(start, Instant.now()));

		for (InputStream is : disses)
			is.close();
	}

	private Set<IRI> ensurePredicateAndGraphIrisAreIncluded() {
		Set<IRI> predsAndGraphs = new TreeSet<>(Comparators.forIRI());
		predsAndGraphs.addAll(temporaryGraphIdMap.graphs());
		predsAndGraphs.addAll(predicatesInOrderOfSeen);
		return predsAndGraphs;
	}

	public void parse(List<String> lines) throws IOException {

		if (step <= 0) {
			stepOne(lines);
		} else {
			reloadStepOne();
		}
		if (step <= 2) {
			stepTwo();
		}
		ReadOnlyLiteralStore literalStore;
		if (step <= 3) {
			literalStore = new ReadOnlyLiteralStore(directoryToWriteToo);
		} else {
			literalStore = new ReadOnlyLiteralStore(directoryToWriteToo);
			temporaryGraphIdMap = TemporaryGraphIdMap.fromDisk(directoryToWriteToo);
		}
		if (step <= 4) {
			stepFour();
		} else {

		}
		if (step <= 5) {
			stepFive(literalStore);
		}
		if (step <= 6) {
			cleanUpTempSortedFiles(predicatesDirectories);
		}
	}

	private void reloadStepOne() throws IOException, FileNotFoundException {
		List<String> ris = Files.readAllLines(predFile.toPath());
		SimpleValueFactory vf = SimpleValueFactory.getInstance();
		for (String ri : ris) {
			predicatesInOrderOfSeen.add(vf.createIRI(ri));
		}
		ReadOnlyStore.findExistingPredicateDirectories(directoryToWriteToo, ris, new HashMap<>(), vf);
		// TODO: restart after step 0;
		Failures.NOT_DONE_YET.exit();
	}

	private void stepOne(List<String> lines) throws IOException {
		Instant start = Instant.now();
		logger.info("Starting step 1 parsing files into temporary sorted ones");
		List<Future<IOException>> closers = new ArrayList<>();
		List<Future<?>> toRun = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(lines.size());
		parseFilesIntoPerPredicateType(lines, toRun, latch);
		writeOutPredicates(closers);
		temporaryGraphIdMap.toDisk(directoryToWriteToo);
		logger.info("step 1 took " + Duration.between(start, Instant.now()));
	}

	private void stepTwo() throws IOException {
		Instant start = Instant.now();
		logger.info("Starting step 2 sorting into unique datatype lists");
		List<Future<IOException>> toWaitFor = makeUniqueSortedDatatypeLists(directoryToWriteToo, predicatesDirectories);
		ExternalProcessHelper.waitForFutures(toWaitFor);
		logger.info("step 2 took " + Duration.between(start, Instant.now()));
	}

	private void stepFour() throws IOException {
		Instant start = Instant.now();
		logger.info("Starting step 4 sorting into unique iri list");
		makeUniqueSortedIriList(directoryToWriteToo, predicatesDirectories);
		logger.info("step 4 took " + Duration.between(start, Instant.now()));
	}

	private void stepFive(ReadOnlyLiteralStore literalStore) throws FileNotFoundException, IOException {
		Instant start = Instant.now();
		logger.info("Starting step 5 read the sorted iris");
		SortedList<Value> ssl = SortedListInSections.readinIris(iriFile);
		assert ssl != null;
		ToFinalDiskForm.compressTripleMaps(predicatesDirectories, ssl, exec, literalStore, temporaryGraphIdMap);
		logger.info("step 5 took " + Duration.between(start, Instant.now()));
	}

	private void writeOutPredicates(List<Future<IOException>> closers) throws IOException {
		try (OutputStream predFileWriter = newWriter(predFile)) {
			for (IRI predicateI : predicatesInOrderOfSeen) {

				String predicateS = predicateI.stringValue();
				byte[] predicate = bytes(predicateS);
				predFileWriter.write(predicate);
				predFileWriter.write('\n');
			}
		}
		for (IRI predicateI : predicatesInOrderOfSeen) {
			closers.add(exec.submit(() -> closeSingle(predicateI)));
		}

		ExternalProcessHelper.waitForFutures(closers);
	}

	private IOException closeSingle(IRI predicate) {
		try {
			parsePresureLimit.acquireUninterruptibly();
			predicatesDirectories.get(predicate).close();
		} catch (IOException e) {
			return e;
		} finally {
			parsePresureLimit.release();
		}
		return null;
	}

	private List<Future<IOException>> makeUniqueSortedDatatypeLists(File directoryToWriteToo,
			Map<IRI, PredicateDirectoryWriter> pdws) throws IOException {
		Map<String, Target> encodingDatatype = new HashMap<>();
		Map<String, List<TempSortedFile>> mergeSets = new HashMap<>();
		gatherTemporaryDatatypeFiles(pdws, encodingDatatype, mergeSets);
		List<Future<IOException>> futures = new ArrayList<>();
		for (String tn : mergeSets.keySet()) {
			Target target = encodingDatatype.get(tn);
			List<TempSortedFile> list = mergeSets.get(tn);
			futures.add(exec.submit(() -> mergeSortIntoSortedListPerDatatype(directoryToWriteToo, target, list)));
		}
		return futures;
	}

	private IOException mergeSortIntoSortedListPerDatatype(File directoryToWriteToo, Target t,
			List<TempSortedFile> toMergeIntoOne) {
		try {
			List<InputStream> toClose = new ArrayList<>();
			List<Iterator<byte[]>> iters = new ArrayList<>();
			for (TempSortedFile tsf : toMergeIntoOne) {
				DataInputStream openObjects = tsf.openObjects();
				toClose.add(openObjects);
				iters.add(tsf.rawObjectIterator(openObjects));
			}
			Instant start = Instant.now();
			Comparator<byte[]> vc = Comparators.byteComparatorFor(t.objectKind(), t.getDatatype(), t.getLang());
			Iterator<byte[]> sortedInput = Iterators.mergeDistinctSorted(vc, iters);

			File target = new File(directoryToWriteToo,
					ReadOnlyLiteralStore.fileNameForLiteral(t.getDatatype(), t.getLang()));
			if (t.getLang() != null) {
				ToFinalDiskForm.makeSortedRawLangStringList(new ReducingIterator<>(sortedInput, vc), target,
						t.getLang());
			} else {
				ToFinalDiskForm.makeSortedRawDatatypedLiteralList(new ReducingIterator<>(sortedInput, vc), target,
						t.getDatatype());
			}
			assert target.length() > 0;
			for (InputStream is : toClose) {
				is.close();
			}
			if (t.getDatatype() != null)
				logger.info("Finished merge sorting all unique " + t.getDatatype().getLocalName() + "s: "
						+ Duration.between(start, Instant.now()));
			else
				logger.info("Finished merge sorting all unique " + t.getLang() + "s: "
						+ Duration.between(start, Instant.now()));
		} catch (IOException e) {
			return e;
		}
		return null;
	}

	private void gatherTemporaryDatatypeFiles(Map<IRI, PredicateDirectoryWriter> pdws,
			Map<String, Target> encodingDatatype, Map<String, List<TempSortedFile>> mergeSets) {
		for (PredicateDirectoryWriter pdw : pdws.values()) {
			for (Target t : pdw.getTargets()) {
				if (t.objectKind() == Kind.LITERAL) {
					String tn = t.getTripleFile().file().getName();
					List<TempSortedFile> toMerge = mergeSets.get(tn);
					if (toMerge == null) {
						toMerge = new ArrayList<>();
						mergeSets.put(tn, toMerge);
						encodingDatatype.put(tn, t);
					}
					toMerge.add(t.getTripleFile());
				}
			}
		}
	}

	private Target writeStatement(File directoryToWriteToo, Set<IRI> predicatesInOrderOfSeen,
			Map<IRI, PredicateDirectoryWriter> predicateDirectories, Statement next, Target previous,
			Compression tempCompression) throws IOException {
		PredicateDirectoryWriter predicateDirectoryWriter;
		if (previous != null && previous.testForAcceptance(next)) {
			previous.write(next);
			return previous;
		} else {
			IRI predicate = next.getPredicate();
			predicateDirectoryWriter = predicateDirectories.get(predicate);
			if (predicateDirectoryWriter == null) {
				predicateDirectoryWriter = addNewPredicateWriter(directoryToWriteToo, predicatesInOrderOfSeen,
						predicateDirectories, predicate, tempCompression);
			}
			return predicateDirectoryWriter.write(next);
		}
	}

	private PredicateDirectoryWriter addNewPredicateWriter(File directoryToWriteToo, Set<IRI> predicatesInOrderOfSeen,
			Map<IRI, PredicateDirectoryWriter> predicateDirectories, IRI predicate, Compression tempCompression)
			throws IOException {
		PredicateDirectoryWriter predicateDirectoryWriter;
		try {
			predicateSeenLock.lock();

			if (!predicatesInOrderOfSeen.contains(predicate)) {
				predicatesInOrderOfSeen.add(predicate);
				predicateDirectoryWriter = createPredicateDirectoryWriter(directoryToWriteToo, predicateDirectories,
						predicate, temporaryGraphIdMap, tempCompression);
				predicateDirectories.put(predicate, predicateDirectoryWriter);
			} else {
				predicateDirectoryWriter = predicateDirectories.get(predicate);
			}

		} finally {
			predicateSeenLock.unlock();
		}
		return predicateDirectoryWriter;
	}

	private static final byte[] bytes(String string) {
		return string.getBytes(StandardCharsets.UTF_8);
	}

	private PredicateDirectoryWriter createPredicateDirectoryWriter(File directoryToWriteToo,
			Map<IRI, PredicateDirectoryWriter> predicatesInOrderOfSeen, IRI predicate,
			TemporaryGraphIdMap temporaryGraphIdMap2, Compression tempCompression) throws IOException {
		File pred_dir = new File(directoryToWriteToo, PRED_DIR_PREFIX + predicatesInOrderOfSeen.size());
		if (!pred_dir.isDirectory() && !pred_dir.mkdir())
			throw new RuntimeException("can't make directory" + pred_dir.getAbsolutePath());
		PredicateDirectoryWriter predicateDirectoryWriter = new PredicateDirectoryWriter(pred_dir, temporaryGraphIdMap2,
				exec, concurrentTargetFiles, sortPresureLimit, predicate, tempCompression);
		predicatesInOrderOfSeen.put(predicate, predicateDirectoryWriter);
		return predicateDirectoryWriter;
	}

	public enum Kind {
		BNODE(0), IRI(1), LITERAL(2), TRIPLE(3);

		private final int sortOrder;

		Kind(int i) {
			this.sortOrder = i;
		}

		public static Kind of(Value val) {
			if (val.isIRI())
				return IRI;
			else if (val.isBNode())
				return BNODE;
			else if (val.isTriple())
				return TRIPLE;
			else
				return LITERAL;
		}

		public String label() {
			switch (this) {
			case TRIPLE:
				return "triple";
			case IRI:
				return "iri";
			case BNODE:
				return "bnode";
			case LITERAL:
				return "literal";
			default:
				throw new IllegalStateException();
			}
		}
	}

	static class PredicateDirectoryWriter implements AutoCloseable {
		private final Map<Target.TargetKey, Target> targets = new ConcurrentHashMap<>();

		public Collection<Target> getTargets() {
			return targets.values();
		}

		private final File directory;
		private final TemporaryGraphIdMap temporaryGraphIdMap;
		private final ExecutorService exec;
		private final int concurrentTargetFiles;
		private final Semaphore sortPressureLimit;
		private final Lock lock = new ReentrantLock();
		private final IRI predicate;
		private final Compression tempCompression;

		private PredicateDirectoryWriter(File directory, TemporaryGraphIdMap temporaryGraphIdMap, ExecutorService exec,
				int concurrentTargetFiles, Semaphore sortPressureLimit, IRI predicate, Compression tempCompression)
				throws IOException {
			this.directory = directory;
			this.temporaryGraphIdMap = temporaryGraphIdMap;
			this.exec = exec;
			this.concurrentTargetFiles = concurrentTargetFiles;
			this.sortPressureLimit = sortPressureLimit;
			this.predicate = predicate;
			this.tempCompression = tempCompression;
		}

		/**
		 * Warning accessed from multiple threads.
		 *
		 * @param statement to write
		 * @throws IOException
		 */
		private Target write(Statement statement) throws IOException {
			TargetKey key = Target.key(statement);
			Target findAny = targets.get(key);
			if (findAny != null) {
				findAny.write(statement);
			} else {
				try {
					lock.lock();
					findAny = targets.get(key);
					if (findAny != null)
						findAny.write(statement);
					else {
						findAny = new Target(statement, directory, temporaryGraphIdMap, exec, concurrentTargetFiles,
								sortPressureLimit, tempCompression);
						targets.put(key, findAny);
						findAny.write(statement);
					}
				} finally {
					lock.unlock();
				}
			}
			return findAny;
		}

		@Override
		public void close() throws IOException {
			for (Target writer : targets.values()) {
				writer.close();
			}
		}

		public IRI getPredicate() {
			return predicate;
		}
	}

	public OutputStream newWriter(File file) throws IOException {
		OutputStream out = new BufferedOutputStream(new FileOutputStream(file), 128 * 1024);
		return out;
	}

	private class Handler implements RDFHandler {
		private final Map<String, Long> bnodeMap = new HashMap<>();
		private final IRI graph;
		private Target previous = null;
		private final Compression tempCompression;

		public Handler(IRI graph, Compression tempCompression) {
			super();
			this.graph = graph;
			this.tempCompression = tempCompression;
		}

		@Override
		public void startRDF() throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void endRDF() throws RDFHandlerException {
		}

		@Override
		public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleStatement(Statement next) throws RDFHandlerException {
			if (next.getContext() == null) {
				next = vf.createStatement(next.getSubject(), next.getPredicate(), next.getObject(), graph);
			}
			if (next.getObject() instanceof BNode) {
				BNode bo = bnodeToReadOnlyBnode((BNode) next.getObject());
				next = vf.createStatement(next.getSubject(), next.getPredicate(), bo, next.getContext());
			}
			if (next.getSubject() instanceof BNode) {
				BNode bo = bnodeToReadOnlyBnode((BNode) next.getSubject());
				next = vf.createStatement(bo, next.getPredicate(), next.getObject(), next.getContext());
			}
			try {
				previous = writeStatement(directoryToWriteToo, predicatesInOrderOfSeen, predicatesDirectories, next,
						previous, tempCompression);
			} catch (IOException e) {
				logger.error("IO:", e);
				throw new RDFHandlerException("Failure passing data on", e);
			}

		}

		private BNode bnodeToReadOnlyBnode(BNode bo) {
			if (bnodeMap.containsKey(bo.getID())) {
				bo = new ReadOnlyBlankNode(bnodeMap.get(bo.getID()));
			} else {
				long bnodeId = BNODE_ID_NORMALIZER.incrementAndGet();
				bnodeMap.put(bo.getID(), bnodeId);
				bo = new ReadOnlyBlankNode(bnodeId);
			}
			return bo;
		}

		@Override
		public void handleComment(String comment) throws RDFHandlerException {
			// TODO Auto-generated method stub

		}
	}

	@Override
	public void close() {
		exec.shutdown();
	}
}
