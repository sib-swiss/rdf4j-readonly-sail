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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;

import swiss.sib.swissprot.sail.readonly.datastructures.Triples;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyValueComparator;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyValueFactory;

public class ReadOnlyDataTripleSource implements TripleSource {
	private final ReadOnlyValueFactory vf;
	private final ReadOnlyStore store;

	public ReadOnlyDataTripleSource(ReadOnlyValueFactory vf, ReadOnlyStore store) {
		super();
		this.vf = vf;
		this.store = store;
	}

	@Override
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subject,
			IRI predicate, Value object, Resource... contexts) {

		List<Triples> triples = store.getTriples(predicate);
		if (triples == null || triples.isEmpty()) {
			return new EmptyIteration<>();
		} else if (triples.size() == 1) {
			Iterator<Triples> iterator = triples.iterator();
			Triples next = iterator.next();
			return next.iterate(subject, object, contexts);
		} else {
			Iterator<Iterator<Statement>> collect = triples.stream()
					.map(t -> t.iterateStatements(subject, object, contexts))
					.iterator();

			Iterator<Statement> multiOrderedIterator = Iterators.concat(collect);
			return new CloseableIteration<Statement, QueryEvaluationException>() {

				@Override
				public boolean hasNext() throws QueryEvaluationException {
					return multiOrderedIterator.hasNext();
				}

				@Override
				public Statement next() throws QueryEvaluationException {
					return multiOrderedIterator.next();
				}

				@Override
				public void remove() throws QueryEvaluationException {
					// TODO Auto-generated method stub

				}

				@Override
				public void close() throws QueryEvaluationException {
					// TODO Auto-generated method stub

				}

			};
		}
	}

	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatementsInOrder(Resource subject,
			IRI predicate, Value object, Resource... contexts) {

		List<Triples> triples = store.getTriples(predicate);
		if (triples == null || triples.isEmpty()) {
			return new EmptyIteration<>();
		} else if (triples.size() == 1) {
			Iterator<Triples> iterator = triples.iterator();
			Triples next = iterator.next();
			return next.iterate(subject, object, contexts);
		} else {
			List<Iterator<Statement>> collect = triples.stream()
					.map(t -> t.iterateStatements(subject, object, contexts))
					.collect(Collectors.toList());
			Iterator<Statement> multiOrderedIterator = Iterators.mergeSorted(ReadOnlyDataTripleSource::compareStatement,
					collect);
			return new CloseableIteration<Statement, QueryEvaluationException>() {

				@Override
				public boolean hasNext() throws QueryEvaluationException {
					return multiOrderedIterator.hasNext();
				}

				@Override
				public Statement next() throws QueryEvaluationException {
					return multiOrderedIterator.next();
				}

				@Override
				public void remove() throws QueryEvaluationException {
					// TODO Auto-generated method stub

				}

				@Override
				public void close() throws QueryEvaluationException {
					// TODO Auto-generated method stub

				}

			};
		}
	}

	private static int compareStatement(Statement a, Statement b) {

		Resource bp = b.getPredicate();
		Resource ap = a.getPredicate();
		int subComp = compare(bp, ap);
		if (subComp != 0) {
			return subComp;
		}
		Resource bs = b.getSubject();
		Resource as = a.getSubject();
		subComp = compare(bs, as);
		if (subComp != 0) {
			return subComp;
		}
		return compare(a.getObject(), b.getObject());
	}

	private static int compare(Value bs, Value as) {
		return new ReadOnlyValueComparator().compare(bs, as);
	}

	@Override
	public ReadOnlyValueFactory getValueFactory() {
		return vf;
	}

}
