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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSail;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.shaded.com.google.common.io.Files;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMap;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMapViaLongBuffers;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongViaBitSetsMap;
import swiss.sib.swissprot.sail.readonly.datastructures.Triples;
import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaBitSetsIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaLongBuffersIO;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedListInSections;
import swiss.sib.swissprot.sail.readonly.datastructures.roaringbitmap.Roaring64BitmapAdder;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyBlankNode;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyIRI;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyValueFactory;

public class ReadOnlyStore extends AbstractSail {
	private final class BNodeToLong implements ToLongFunction<Value> {
		@Override
		public long applyAsLong(Value l) {
			if (l instanceof ReadOnlyBlankNode) {
				return ((ReadOnlyBlankNode) l).id();
			} else {
				assert false : l.stringValue();
				return WriteOnce.NOT_FOUND;
			}
		}
	}

	private final class IriToLong implements ToLongFunction<Value> {
		@Override
		public long applyAsLong(Value l) {
			if (l instanceof ReadOnlyIRI) {
				return ((ReadOnlyIRI) l).id();
			} else if (l instanceof IRI) {
				try {
					return iris.positionOf(l);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				assert false : l.stringValue();
				return WriteOnce.NOT_FOUND;
			}
		}
	}

	private final class ValueToLongFromSortedList implements ToLongFunction<Value> {
		private final IRI datatype;
		private final SortedList<Value> sortedListFor;

		private ValueToLongFromSortedList(IRI datatype, SortedList<Value> sortedListFor) {
			this.datatype = datatype;
			this.sortedListFor = sortedListFor;
		}

		@Override
		public long applyAsLong(Value v) {
			if (!v.isLiteral()) {
				assert false : v.stringValue();
				return WriteOnce.NOT_FOUND;
			}
			Literal l = (Literal) v;
			if (!l.getDatatype().equals(datatype)) {
				return WriteOnce.NOT_FOUND;
			}
			try {
				return sortedListFor.positionOf(v);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private final class LangStringToLongFromSortedList implements ToLongFunction<Value> {
		private final String lang;
		private final SortedList<Value> sortedListFor;

		private LangStringToLongFromSortedList(String lang, SortedList<Value> sortedListFor) {
			this.lang = lang;
			this.sortedListFor = sortedListFor;
		}

		@Override
		public long applyAsLong(Value v) {
			if (!v.isLiteral()) {
				return WriteOnce.NOT_FOUND;
			}
			Literal l = (Literal) v;
			Optional<String> language = l.getLanguage();
			if (language.isEmpty())
				return WriteOnce.NOT_FOUND;
			String string = language.get();
			if (lang.equals(string))
				try {
					return sortedListFor.positionOf(v);

				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			else {
				return WriteOnce.NOT_FOUND;
			}
		}
	}

	private final class ResourceToLong implements ToLongFunction<Resource> {
		@Override
		public long applyAsLong(Resource l) {
			if (l instanceof ReadOnlyIRI)
				return ((ReadOnlyIRI) l).id();
			else if (l instanceof IRI)
				try {
					return iris.positionOf(l);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			else {
				return WriteOnce.NOT_FOUND;
			}
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ReadOnlyStore.class);
	private final ReadOnlyValueFactory vf;
	private final SortedList<Value> iris;
	private Map<IRI, File> predicateDirectories = new ConcurrentHashMap<>();
	private Map<IRI, List<Triples>> triplesPerPredicate = new ConcurrentHashMap<>();

	public ReadOnlyStore(File rootDir) throws FileNotFoundException, IOException {
		super();
		this.setDataDir(rootDir);
		this.iris = SortedListInSections.readinIris(new File(rootDir, FileNames.IRIS_FILE_NAME));
		List<String> predicates = Files.readLines(new File(rootDir, FileNames.PREDICATES_FILE_NAME),
				StandardCharsets.UTF_8);

		ReadOnlyLiteralStore rols = new ReadOnlyLiteralStore(rootDir);
		this.vf = new ReadOnlyValueFactory(iris, rols);
		findExistingPredicateDirectories(rootDir, predicates, predicateDirectories, vf);
		openPredicateDirectories(rootDir, rols);
		triplesPerPredicate.forEach((k, l) -> {
			l.sort(Triples::compareTo);
		});
	}

	private void openPredicateDirectories(File rootDir, ReadOnlyLiteralStore rols)
			throws FileNotFoundException, IOException {
		for (Map.Entry<IRI, File> en : predicateDirectories.entrySet()) {
			IRI pred = en.getKey();
			File predDir = en.getValue();
			mapSubjectDirectories(predDir, pred, rols);
		}
	}

	public static void findExistingPredicateDirectories(File rootDir, List<String> predicates,
			Map<IRI, File> predicateDirectories, ValueFactory vf) throws FileNotFoundException, IOException {

		for (int j = 0; j < predicates.size(); j++) {
			String predicate = predicates.get(j);
			File predDir = new File(rootDir, FileNames.PRED_DIR_PREFIX + j);
			IRI pred = vf.createIRI(predicate);
			predicateDirectories.put(pred, predDir);
		}
	}

	private void mapSubjectDirectories(File predDir, IRI pred, ReadOnlyLiteralStore rols)
			throws FileNotFoundException, IOException {
		for (File subjectDir : predDir.listFiles()) {
			String name = subjectDir.getName();
			for (Kind sk : Kind.values()) {
				if (name.equals(sk.label())) {
					mapObjectFiles(pred, subjectDir, sk, rols);
				}
			}
		}
	}

	private void mapObjectFiles(IRI pred, File subjectDir, Kind sk, ReadOnlyLiteralStore rols)
			throws FileNotFoundException, IOException {
		for (File objectFiles : subjectDir.listFiles()) {
			String name = objectFiles.getName();
			for (Kind ok : Kind.values()) {
				if (name.startsWith(ok.label())) {
					mapAnObjectFile(pred, sk, objectFiles, ok, rols);
				}
			}
			if (ReadOnlyLiteralStore.isLiteralFile(name)) {
				logger.info("opening " + objectFiles.getAbsolutePath() + " for " + pred.toString());
				mapAnObjectFile(pred, sk, objectFiles, Kind.LITERAL, rols);
			}
		}
	}

	private void mapAnObjectFile(IRI pred, Kind sk, File objectFiles, Kind ok, ReadOnlyLiteralStore rols)
			throws FileNotFoundException, IOException {
		SortedLongLongMap so = null;
		String objectFileMinusPostFix = "";
		if (objectFiles.getName().endsWith(SortedLongLongViaBitSetsMap.POSTFIX)) {
			so = SortedLongLongMapViaBitSetsIO.readin(objectFiles);
			objectFileMinusPostFix = objectFiles.getName()
					.substring(0,
							objectFiles.getName().length() - SortedLongLongViaBitSetsMap.POSTFIX.length());
		} else if (objectFiles.getName().endsWith(SortedLongLongMapViaLongBuffers.POSTFIX)) {
			so = SortedLongLongMapViaLongBuffersIO.readin(objectFiles);
			objectFileMinusPostFix = objectFiles.getName()
					.substring(0,
							objectFiles.getName().length() - SortedLongLongMapViaLongBuffers.POSTFIX.length());
		}
		if (so == null)
			return;
		LongFunction<Resource> longToIri = l -> new ReadOnlyIRI(l, iris);

		ToLongFunction<Resource> iriToLong = new ResourceToLong();

		ToLongFunction<Value> valueToLong = valueToLong(objectFiles, ok, rols);
		LongFunction<Value> longToValue = longToValue(iris, ok, rols, objectFiles);
		triplesPerPredicate.computeIfAbsent(pred, (p) -> new ArrayList<>());
		Map<IRI, Roaring64Bitmap> graphs = new HashMap<>();
		for (File graphFile : objectFiles.getParentFile().listFiles()) {
			if (graphFile.getName().startsWith("graph-" + objectFileMinusPostFix + "-")) {
				IRI graphIri = new ReadOnlyIRI(Long.parseLong(graphFile.getName().split("-")[2]), iris);
				try (InputStream is = new FileInputStream(graphFile);
						BufferedInputStream bis = new BufferedInputStream(is);
						ObjectInputStream dis = new ObjectInputStream(bis)) {
					final LongBitmapDataProvider readLongBitmapDataProvider = Roaring64BitmapAdder.readLongBitmapDataProvider(dis);
					if (readLongBitmapDataProvider instanceof Roaring64Bitmap rb) {
						graphs.put(graphIri, rb);
					} else {
						throw new IllegalStateException("Unkown type of RoaringBitmap");
					}
				}
			}
		}

		Triples triples = new Triples(this, pred, sk, ok, so, longToIri, iriToLong, longToIri, valueToLong, longToValue,
				graphs);
		triplesPerPredicate.get(pred).add(triples);
	}

	private LongFunction<Value> longToValue(SortedList<Value> iris2, Kind ok, ReadOnlyLiteralStore rols,
			File objectFile) {
		if (ok == Kind.IRI) {
			return l -> new ReadOnlyIRI(l, iris);
		} else if (ok == Kind.BNODE) {
			return l -> new ReadOnlyBlankNode(l);
		} else if (ok == Kind.LITERAL) {
			Optional<IRI> dt = ReadOnlyLiteralStore.dataTypeInFile(objectFile);
			if (dt.isPresent()) {
				
				final IRI dti = dt.get();
				final CoreDatatype dtc = CoreDatatype.from(dti);
				if (dtc != null) {
					return rols.getLongToValue(dtc);
				} else {
					return rols.getLongToValue(dti);
				}
			}
		}
		return null;
	}

	ToLongFunction<Value> valueToLong(File objectFiles, Kind ok, ReadOnlyLiteralStore rols) {
		ToLongFunction<Value> valueToLong;
		switch (ok) {
		case IRI:
			valueToLong = iriToLong();
			break;
		case LITERAL:
			valueToLong = literalToLong(objectFiles, rols);
			break;
		case BNODE:
			valueToLong = bnodeToLong();
			break;
		case TRIPLE:
			throw new UnsupportedOperationException();
		default:
			throw new UnsupportedOperationException();
		}
		return valueToLong;
	}

	private ToLongFunction<Value> literalToLong(File objectFiles, ReadOnlyLiteralStore rols) {
		String fn = objectFiles.getName();
		if (fn.equals(Kind.LITERAL.label())) {
			return sortedStringListToPosition(rols.getSortedListForStrings(), XSD.STRING);
		} else if (fn.startsWith(ReadOnlyLiteralStore.LANG)) {
			// lang string
			String lang = fn.substring(ReadOnlyLiteralStore.LANG.length());
			return new LangStringToLongFromSortedList(lang, rols.getSortedListFor(lang));
		} else {
			Optional<IRI> dt = ReadOnlyLiteralStore.dataTypeInFile(objectFiles);
			if (dt.isEmpty())
				throw new IllegalStateException("Empty datatype");
			else {
				IRI datatype = dt.get();
				SortedList<Value> sortedListFor = rols.getSortedListFor(datatype);
				return sortedStringListToPosition(sortedListFor, datatype);
				// datatype
			}
		}

	}

	private ToLongFunction<Value> sortedStringListToPosition(SortedList<Value> sortedListFor, IRI datatype) {
		return new ValueToLongFromSortedList(datatype, sortedListFor);
	}

	ToLongFunction<Value> iriToLong() {
		return new IriToLong();
	}

	ToLongFunction<Value> bnodeToLong() {
		return new BNodeToLong();
	}

	@Override
	public boolean isWritable() throws SailException {
		return false;
	}

	@Override
	protected SailConnection getConnectionInternal() throws SailException {
		return new ReadonlyStoreConnection(this);
	}

	@Override
	public ReadOnlyValueFactory getValueFactory() {
		return vf;
	}

	public File getDirectory(IRI predicate, Kind subjectKind) {
		File predDir = predicateDirectories.get(predicate);
		return new File(predDir, subjectKind.label());
	}

	public List<Triples> getTriples(IRI predicate) {
		if (predicate == null) {
			return triplesPerPredicate.values().stream().flatMap(List::stream).collect(Collectors.toList());
		}
		List<Triples> list = triplesPerPredicate.get(predicate);
		return list;
	}

	@Override
	protected void shutDownInternal() throws SailException {

	}

	public List<Triples> getAllTriples() {
		return triplesPerPredicate.values().stream().flatMap(List::stream).collect(Collectors.toList());
	}

}
