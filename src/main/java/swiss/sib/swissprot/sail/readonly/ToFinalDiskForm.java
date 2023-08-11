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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.WriteOnce.PredicateDirectoryWriter;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMapViaLongBuffers;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongViaBitSetsMap;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO.IO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaBitSetsIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaLongBuffersIO;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.datastructures.list.FitsInLongSortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.FitsInLongSortedList.FitingDatatypes;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedListInSections;
import swiss.sib.swissprot.sail.readonly.datastructures.roaringbitmap.Roaring64BitmapAdder;
import swiss.sib.swissprot.sail.readonly.storing.TemporaryGraphIdMap;

public class ToFinalDiskForm {
	private static final Logger logger = LoggerFactory.getLogger(ToFinalDiskForm.class);

	private static final class ObjectValueToLong implements ToLongFunction<Value> {
		private final SortedList<Value> values;
		private final Map<Value, Long> objectPositionBuffer = new LRUMap<>(Short.MAX_VALUE);

		private ObjectValueToLong(SortedList<Value> values) {
			assert values != null;
			this.values = values;
		}

		@Override
		public long applyAsLong(Value s) {
			Long position = objectPositionBuffer.get(s);
			if (position != null) {
				return position;
			} else {
				long pos = ToFinalDiskForm.findIdOfValue(values, s);
				objectPositionBuffer.put(s, pos);
				return pos;
			}
		}
	}

	public static SortedList<? extends Value> makeSortedValueList(Iterator<Value> sortedInput, File iriFile,
			IRI datatype)
			throws IOException, FileNotFoundException {
		logger.debug("Reading " + iriFile.getName() + " into memory: " + Instant.now());
		IO io = RawIO.forOutput(Kind.LITERAL, datatype, null);
		FitingDatatypes forDatatype = FitsInLongSortedList.FitingDatatypes.forDatatype(datatype);
		SortedList<? extends Value> ssl;
		if (forDatatype == null) {
			SortedListInSections.rewriteValues(sortedInput, iriFile, io::getBytes);
			ssl = SortedListInSections.readinValues(iriFile, datatype);
		} else {
			FitsInLongSortedList.rewriteValues(sortedInput, iriFile, forDatatype);
			ssl = FitsInLongSortedList.readInValues(iriFile, forDatatype);
		}
		assert ssl != null;
		return ssl;
	}

	public static SortedList<Value> makeSortedValueList(Iterator<Value> sortedInput, File iriFile, String lang)
			throws IOException, FileNotFoundException {
		logger.debug("Reading " + iriFile.getName() + " into memory: " + Instant.now());
		IO io = RawIO.forOutput(Kind.LITERAL, null, lang);
		SortedListInSections.rewriteValues(sortedInput, iriFile, io::getBytes);
		SortedList<Value> ssl = SortedListInSections.readinValues(iriFile, lang);
		assert ssl != null;
		return ssl;
	}

	public static SortedList<Value> makeSortedIRIList(Iterator<IRI> sortedInput, File iriFile)
			throws IOException, FileNotFoundException {
		logger.debug("Reading " + iriFile.getName() + " into memory: " + Instant.now());
		IO io = RawIO.forOutput(Kind.IRI);
		SortedListInSections.rewriteIRIs(sortedInput, iriFile, io::getBytes);
		SortedList<Value> ssl = SortedListInSections.readinIris(iriFile);
		assert ssl != null;
		return ssl;
	}

	public static SortedList<Value> makeSortedRawIRIList(Iterator<byte[]> sortedInput, File iriFile)
			throws IOException, FileNotFoundException {
		logger.debug("Reading " + iriFile.getName() + " into memory: " + Instant.now());
		SortedListInSections.rewrite(sortedInput, iriFile);
		SortedList<Value> ssl = SortedListInSections.readinIris(iriFile);
		assert ssl != null;
		return ssl;
	}

	public static SortedList<? extends Value> makeSortedRawDatatypedLiteralList(Iterator<byte[]> sortedInput,
			File iriFile,
			IRI datatype) throws IOException, FileNotFoundException {
		logger.debug("Reading " + iriFile.getName() + " into memory: " + Instant.now(
		));
		FitingDatatypes forDatatype = FitsInLongSortedList.FitingDatatypes.forDatatype(datatype);
		SortedList<? extends Value> ssl;
		if (forDatatype == null) {
			SortedListInSections.rewrite(sortedInput, iriFile);
			ssl = SortedListInSections.readinValues(iriFile, datatype);
		} else {
			IO io = RawIO.forOutput(Kind.LITERAL, datatype, null);
			FitsInLongSortedList.rewriteValues(Iterators.map(sortedInput, io::read), iriFile, forDatatype);
			ssl = FitsInLongSortedList.readInValues(iriFile, forDatatype);
		}
		assert ssl != null;
		return ssl;
	}

	public static SortedList<Value> makeSortedRawLangStringList(Iterator<byte[]> sortedInput, File iriFile, String lang)
			throws IOException, FileNotFoundException {
		logger.debug("Reading " + iriFile.getName() + " into memory: " + Instant.now());
		SortedListInSections.rewrite(sortedInput, iriFile);
		SortedList<Value> ssl = SortedListInSections.readinValues(iriFile, lang);
		assert ssl != null;
		return ssl;
	}

	private static final class SubjectValueToLong implements ToLongFunction<Value> {
		private final Function<Value, TPosition<Value>> subjects;
		private TPosition<Value> prev = null;

		private SubjectValueToLong(Function<Value, TPosition<Value>> subjects) {
			this.subjects = subjects;
		}

		@Override
		public long applyAsLong(Value toConvert) {
			if (prev == null) {
				prev = subjects.apply(toConvert);
			} else if (prev.t().equals(toConvert)) {
				return prev.position();
			} else {
				prev = subjects.apply(toConvert);
			}
			return prev.position();
		}
	}

	public static void compressTripleMaps(Map<IRI, PredicateDirectoryWriter> pdws, SortedList<Value> iris,
			ExecutorService exec, ReadOnlyLiteralStore rols, TemporaryGraphIdMap temporaryGraphIdMap)
			throws FileNotFoundException, IOException {
		int procs = Runtime.getRuntime().availableProcessors();
		Semaphore compressionPresureLimit = new Semaphore(Math.max(1, procs / 2));
		List<Future<Exception>> toRun = new ArrayList<>();
		List<Target> targetList = pdws.values()
				.stream()
				.flatMap(p -> p.getTargets().stream())
				.collect(Collectors.toList());
		CountDownLatch latch = new CountDownLatch(targetList.size());
		targetList.sort((a, b) -> {
			return Long.compare(a.getTripleFile().file().length(), b.getTripleFile().file().length());
		});
		for (Target t : targetList) {

			TempSortedFile uncompressed = t.getTripleFile();
			String compname = t.getTripleFinalFile().getName();

			if (t.subjectKind() == Kind.TRIPLE) {
				latch.countDown();
			} else {
				assert iris != null;
				ToLongFunction<Value> objectToLong = findObjectToLongFunction(iris, rols, t);
				if (objectToLong != null) {
					submitRewriter(iris, exec, toRun, latch, objectToLong, uncompressed, compname, t.subjectKind(),
							temporaryGraphIdMap, compressionPresureLimit);
				} else
					latch.countDown();
			}
		}
		logger.debug(latch.getCount() + " latches " + toRun.size());
		waitForLatchesAndFutures(toRun, latch, exec);
	}

	private static void waitForLatchesAndFutures(List<Future<Exception>> toRun, CountDownLatch latch,
			ExecutorService exec) {
		WAIT: try {
			while (latch.getCount() > 0) {
				latch.await(1, TimeUnit.SECONDS);
				logger.debug(latch.getCount() + " latches " + toRun.size());
				for (Iterator<Future<Exception>> iterator = toRun.iterator(); iterator.hasNext();) {
					Future<Exception> element = iterator.next();
					if (element.isDone()) {
						Exception exception = element.get();
						if (exception != null) {
							throw new RuntimeException(exception);
						}
						iterator.remove();
					} else if (element.isCancelled()) {
						iterator.remove();
					}
				}
			}
			logger.debug("done converting triple maps");
		} catch (InterruptedException e) {
			Thread.interrupted();
			break WAIT;
		} catch (ExecutionException e) {
			exec.shutdownNow();
			throw new RuntimeException(e);
		}
	}

	static void submitRewriter(SortedList<Value> iris, ExecutorService exec, List<Future<Exception>> toRun,
			CountDownLatch latch, ToLongFunction<Value> objectToLong, TempSortedFile uncompressed, String compname,
			Kind subjectKind, TemporaryGraphIdMap temporaryGraphIdMap, Semaphore compressionPresureLimit) {

		toRun.add(exec.submit(() -> runRewrite(iris, latch, objectToLong, uncompressed, compname, subjectKind,
				temporaryGraphIdMap, compressionPresureLimit)));
	}

	private static Exception runRewrite(SortedList<Value> iris, CountDownLatch latch,
			ToLongFunction<Value> objectToLong, TempSortedFile uncompressed, String compname, Kind subjectKind,
			TemporaryGraphIdMap temporaryGraphIdMap, Semaphore compressionPresureLimit) {
		try {
			compressionPresureLimit.acquireUninterruptibly();
			logger.info(uncompressed.file() + " is is going to be rewriten");
			File rewriten = rewrite(iris, subjectKind, objectToLong, uncompressed, compname, temporaryGraphIdMap);
			logger.info(uncompressed.file().getAbsolutePath() + " is now stored as " + rewriten.getAbsolutePath());
			uncompressed.delete();
			return null;
		} catch (IOException e) {
			logger.error(e.getMessage());
			return e;
		} finally {
			compressionPresureLimit.release();
			latch.countDown();
		}
	}

	private static ToLongFunction<Value> findObjectToLongFunction(SortedList<Value> iris, ReadOnlyLiteralStore rols,
			Target t) {
		ToLongFunction<Value> objectToLong = null;
		if (t.objectKind() == Kind.IRI) {
			objectToLong = new ObjectValueToLong(iris);
		} else if (t.objectKind() == Kind.BNODE) {
			objectToLong = ToFinalDiskForm::fromBNode;
		} else if (t.objectKind() == Kind.LITERAL) {
			if (t.getLang() != null) {
				SortedList<Value> sl = rols.getSortedListFor(t.getLang());
				assert sl != null : t.getLang().toString();
				objectToLong = new ObjectValueToLong(sl);
			} else if (t.getDatatype() != null) {
				SortedList<Value> sl = rols.getSortedListFor(t.getDatatype());
				assert sl != null : t.getDatatype().toString();
				objectToLong = new ObjectValueToLong(sl);
			} else {
				SortedList<Value> sl = rols.getSortedListForStrings();
				assert sl != null;
				objectToLong = new ObjectValueToLong(sl);
			}
		}
		return objectToLong;
	}

	private static long fromBNode(Value s) {
		return Long.parseLong(((BNode) s).getID());
	}

	private static File rewrite(SortedList<Value> iris, Kind subjectKind, ToLongFunction<Value> objectToLong,
			TempSortedFile uncompressed, String compname, TemporaryGraphIdMap temporaryGraphIdMap)
			throws IOException, FileNotFoundException {
		Map<Integer, Roaring64BitmapAdder> graphBitMapsAdders = new HashMap<>();
		Instant start = Instant.now();
		logger.info("Starting to transform " + uncompressed.file().getAbsolutePath() + ": " + start);
		File compbs = new File(uncompressed.file().getParentFile(), compname + SortedLongLongViaBitSetsMap.POSTFIX);
		try (AddToGraphBitset forGraphs = new AddToGraphBitset(graphBitMapsAdders)) {

			boolean bitsetCompr = tryRewritingIntoBitsets(iris, subjectKind, objectToLong, uncompressed, forGraphs,
					compbs);
			if (!bitsetCompr) {
				compbs = rewriteIntoLongLongMap(iris, subjectKind, objectToLong, uncompressed, compname, forGraphs,
						compbs);
			}
		}
		Map<Integer, LongBitmapDataProvider> graphBitMaps = new HashMap<>();
		for (Map.Entry<Integer, Roaring64BitmapAdder> en : graphBitMapsAdders.entrySet()) {
			graphBitMaps.put(en.getKey(), en.getValue().build());
		}
		writeOutGraphBitSets(iris, uncompressed, compname, temporaryGraphIdMap, graphBitMaps);

		long triples = graphBitMaps.values().stream().mapToLong(LongBitmapDataProvider::getLongCardinality).sum();
		logger.info("Finished transforming " + triples + " from " + uncompressed.file().getAbsolutePath() + ": "
				+ Duration.between(start, Instant.now()));
		return compbs;
	}

	private static void writeOutGraphBitSets(SortedList<Value> iris, TempSortedFile uncompressed, String compname,
			TemporaryGraphIdMap temporaryGraphIdMap, Map<Integer, LongBitmapDataProvider> graphBitMaps)
			throws IOException, FileNotFoundException {
		for (Map.Entry<Integer, LongBitmapDataProvider> en : graphBitMaps.entrySet()) {
			IRI graph = temporaryGraphIdMap.iriFromTempGraphId(en.getKey());
			long graphPos = iris.positionOf(graph);
			LongBitmapDataProvider value = en.getValue();
			File file = new File(uncompressed.file().getAbsoluteFile().getParentFile(),
					"graph-" + compname + "-" + graphPos);

			try (FileOutputStream fos = new FileOutputStream(file);
					BufferedOutputStream bos = new BufferedOutputStream(fos);
					DataOutputStream out = new DataOutputStream(bos)) {
				value.serialize(out);
			}
		}
	}

	private static File rewriteIntoLongLongMap(SortedList<Value> iris, Kind subjectKind,
			ToLongFunction<Value> objectToLong, TempSortedFile uncompressed, String compname,
			ObjIntConsumer<Long> forGraphs, File compbs) throws IOException {
		if (subjectKind == Kind.IRI) {
			Function<Value, TPosition<Value>> iterator = iris.searchInOrder();
			compbs = new File(uncompressed.file().getParentFile(), compname + SortedLongLongMapViaLongBuffers.POSTFIX);
			ToLongFunction<Value> subjectStringToLong = new SubjectValueToLong(iterator);
			SortedLongLongMapViaLongBuffersIO.rewrite(uncompressed, compbs, subjectStringToLong, objectToLong,
					forGraphs);

		} else if (subjectKind == Kind.BNODE) {
			SortedLongLongMapViaLongBuffersIO.rewrite(uncompressed, compbs, ToFinalDiskForm::fromBNode, objectToLong,
					forGraphs);
		} else if (subjectKind == Kind.TRIPLE) {
			logger.error("Asking to index triples which we don't support yet");
		}
		return compbs;
	}

	private static boolean tryRewritingIntoBitsets(SortedList<Value> iris, Kind subjectKind,
			ToLongFunction<Value> objectToLong, TempSortedFile uncompressed, ObjIntConsumer<Long> forGraphs,
			File compbs) throws IOException {

		if (subjectKind == Kind.IRI) {
			Function<Value, TPosition<Value>> iterator = iris.searchInOrder();
			SubjectValueToLong subjectToLong = new SubjectValueToLong(iterator);
			if (!SortedLongLongMapViaBitSetsIO.rewrite(uncompressed, compbs, subjectToLong, objectToLong, forGraphs)) {
				return false;
			}

		} else if (subjectKind == Kind.BNODE) {
			if (!SortedLongLongMapViaBitSetsIO.rewrite(uncompressed, compbs, ToFinalDiskForm::fromBNode, objectToLong,
					forGraphs)) {
				return false;
			}
		} else if (subjectKind == Kind.TRIPLE) {
			logger.error("Asking to index triples which we don't support yet");
		}
		return true;
	}

	private static class AddToGraphBitset implements ObjIntConsumer<Long>, AutoCloseable {
		private final Map<Integer, Roaring64BitmapAdder> graphBitMaps;
		private Roaring64BitmapAdder lastBitMap;
		private int lastGraphId = (int) WriteOnce.NOT_FOUND;

		public AddToGraphBitset(Map<Integer, Roaring64BitmapAdder> graphBitMaps) {
			super();
			this.graphBitMaps = graphBitMaps;
		}

		@Override
		public void accept(Long l, int s) {
			Roaring64BitmapAdder bm;
			if (lastGraphId == s) {
				bm = lastBitMap;
			} else {
				bm = graphBitMaps.get(s);
				if (bm == null) {
					bm = new Roaring64BitmapAdder();
					graphBitMaps.put(s, bm);
				}
				lastBitMap = bm;
				lastGraphId = s;
			}
			bm.add(l);
		}

		@Override
		public void close() {

		}
	}

	private static long findIdOfValue(SortedList<Value> iris, Value o) {
		try {
			return iris.positionOf(o);
		} catch (IOException e) {
			throw new RuntimeException("Can't get object position in iri list ", e);
		}
	}
}
