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
package swiss.sib.swissprot.sail.readonly.datastructures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.AbstractStatement;
import org.eclipse.rdf4j.sail.extensiblestore.valuefactory.ExtensibleStatement;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import swiss.sib.swissprot.sail.readonly.ReadOnlyStore;
import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMap.KeyValue;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.FilteredKeyValueOrderIterator;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

public class Triples {

	private final Kind subjectKind;
	private final Kind objectKind;
	private final SortedLongLongMap so;
	private final Map<IRI, Roaring64Bitmap> graphs;
	private final LongFunction<Resource> longToSubject;
	private final LongFunction<Value> longToObject;
	private final ToLongFunction<? super Resource> subjectToLong;
	private final ToLongFunction<? super Value> objectToLong;
	private final IRI predicate;

	public Triples(ReadOnlyStore store, IRI predicate, Kind subjectKind, Kind objectKind, SortedLongLongMap so,
			LongFunction<Resource> longToIri, ToLongFunction<Resource> iriToLong,
			LongFunction<Resource> longToSubject, ToLongFunction<Value> valueToLong,
			LongFunction<Value> longToObject, Map<IRI, Roaring64Bitmap> graphs) {
		super();
		this.predicate = predicate;
		this.subjectKind = subjectKind;
		this.objectKind = objectKind;
		this.so = so;
		this.subjectToLong = iriToLong;
		this.longToSubject = longToSubject;
		this.objectToLong = valueToLong;
		this.longToObject = longToObject;

		this.graphs = graphs;
		store.getDirectory(predicate, subjectKind);

	}

	public Iterator<Statement> iterateStatements(Resource subject, Value object, Resource[] contexts) {
		Iterator<KeyValue> raw = plainIterate(subject, object, contexts);
		return new KeyValueToLongLongStatement(raw);
	}

	private Iterator<KeyValue> plainIterate(Resource subject, Value object, Resource[] contexts) {
		List<IRI> graphsInUse = new ArrayList<>();
		boolean couldReturnResults = extractGraphsTestIfCouldReturnResults(subject, object, contexts, graphsInUse);
		if (!couldReturnResults)
			return Collections.emptyIterator();
		Iterator<KeyValue> base = baseIterator(subject, object);
		if (graphsInUse.isEmpty()) {
			return base;
		} else if (graphsInUse.size() == 1) {
			return FilteredKeyValueOrderIterator.supply(base, graphs.get(graphsInUse.get(0)).iterator());
		} else {
			Roaring64Bitmap or = new Roaring64Bitmap();
			graphsInUse.stream().forEach(g -> or.or(graphs.get(g)));
			return FilteredKeyValueOrderIterator.supply(base, or.iterator());
		}
	}

	public CloseableIteration<? extends Statement> iterate(Resource subject, Value object,
			Resource[] contexts) {
		return new LongLongToStatementIteration(plainIterate(subject, object, contexts));
	}

	private Iterator<KeyValue> baseIterator(Resource subject, Value object) {
		Iterator<KeyValue> base;
		if (subject != null && object != null) {
			long subjectId = subjectToLong.applyAsLong(subject);
			long objectId = objectToLong.applyAsLong(object);
			if (subjectId == WriteOnce.NOT_FOUND || objectId == WriteOnce.NOT_FOUND)
				return Collections.emptyIterator();
			return so.iteratorForKeyValue(subjectId, objectId);
		} else if (subject != null) {
			long subjectId = subjectToLong.applyAsLong(subject);
			if (subjectId == WriteOnce.NOT_FOUND)
				return Collections.emptyIterator();
			base = so.iteratorForKey(subjectId);
		} else if (object != null) {
			long objectId = objectToLong.applyAsLong(object);
			if (objectId == WriteOnce.NOT_FOUND)
				return Collections.emptyIterator();
			base = so.iteratorForValue(objectId);
		} else {
			base = so.iterator();
		}
		return base;
	}

	CloseableIteration<? extends Statement> knownSubjectObjectNoGraph(Resource subject,
			Value object) {
		long subjectId = subjectToLong.applyAsLong(subject);
		long objectId = objectToLong.applyAsLong(object);
		Iterator<KeyValue> iterateWithKey = so.iteratorForKey(subjectId);
		Iterator<KeyValue> filter = Iterators.filter(iterateWithKey, lp -> lp.value() == objectId);
		return new LongLongToStatementIteration(filter);
	}

	private boolean extractGraphsTestIfCouldReturnResults(Resource subject, Value object, Resource[] contexts,
			List<IRI> graphsInUse2) {
		List<IRI> graphsInUse;
		if (subject != null && Kind.of(subject) != subjectKind)
			return false;
		if (object != null && Kind.of(object) != objectKind)
			return false;
		boolean nullcontext = false;
		if (contexts != null && contexts.length > 0) {
			graphsInUse = new ArrayList<>();
			for (Resource context : contexts) {
				if (context == null) {
					nullcontext = true;
				} else if (graphs.containsKey(context)) {
					graphsInUse.add((IRI) context);
				}
			}
		} else {
			graphsInUse = new ArrayList<>(graphs.keySet());
		}
		return !graphsInUse.isEmpty() || nullcontext;
	}

	private final class KeyValueToLongLongStatement implements Iterator<Statement> {
		private final Iterator<KeyValue> raw;

		private KeyValueToLongLongStatement(Iterator<KeyValue> raw) {
			this.raw = raw;
		}

		@Override
		public boolean hasNext() {
			return raw.hasNext();
		}

		@Override
		public Statement next() {
			KeyValue n = raw.next();
			return new LongLongStatement(n);
		}
	}

	private final class LongLongStatement extends AbstractStatement implements ExtensibleStatement {
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private long key;
		private long value;

		private LongLongStatement(KeyValue next) {
			key = next.key();
			value = next.value();
		}

		@Override
		public Resource getSubject() {
			return longToSubject.apply(key);
		}

		@Override
		public IRI getPredicate() {
			return predicate;
		}

		@Override
		public Value getObject() {
			return longToObject.apply(value);
		}

		@Override
		public Resource getContext() {
			return null;
		}

		@Override
		public boolean isInferred() {
			return false;
		}
	}

	private final class LongLongToStatementIteration
			implements CloseableIteration<Statement> {

		private final Iterator<KeyValue> kvs;

		@Override
		public boolean hasNext() {
			return kvs.hasNext();
		}

		@Override
		public Statement next() {
			KeyValue next = kvs.next();

			return new LongLongStatement(next);
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub

		}

		public LongLongToStatementIteration(Iterator<KeyValue> kvs) {
			super();
			this.kvs = kvs;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub

		}
	}

	public int compareTo(Triples b) {
		Function<Triples, Kind> bySubject = (t) -> t.subjectKind;
		Function<Triples, Kind> byObject = (t) -> t.objectKind;
		return Comparator.comparing(bySubject).thenComparing(byObject).compare(this, b);
	}

	public long size() {
		return so.size();
	}

	public long sizeOfContext(IRI context) {
		Roaring64Bitmap roaring64Bitmap = graphs.get(context);
		if (roaring64Bitmap != null) {
			return roaring64Bitmap.getLongCardinality();
		}
		return 0;
	}
}
