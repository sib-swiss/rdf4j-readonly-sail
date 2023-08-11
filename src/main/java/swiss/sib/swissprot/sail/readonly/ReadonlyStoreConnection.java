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

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.repository.sparql.federation.SPARQLServiceResolver;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSailConnection;

public class ReadonlyStoreConnection extends AbstractSailConnection {

	private ReadOnlyStore sail;
	private final SPARQLServiceResolver fd;

	protected ReadonlyStoreConnection(ReadOnlyStore sail) {
		super(sail);
		this.sail = sail;
		this.fd = new SPARQLServiceResolver();
	}

	@Override
	protected void closeInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected CloseableIteration<? extends BindingSet> evaluateInternal(TupleExpr tupleExpr,
			Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {

		try {
			ReadOnlyDataTripleSource tripleSource = new ReadOnlyDataTripleSource(sail.getValueFactory(), sail);
			StrictEvaluationStrategy strategy = new ReadOnlyQueryStrictEvaluationStrategy(tripleSource, dataset, fd);
			tupleExpr = optimize(tripleSource, strategy, tupleExpr, bindings);
			return strategy.precompile(tupleExpr).evaluate(bindings);
		} catch (QueryEvaluationException e) {
			throw new SailException(e);
		}
	}

	private TupleExpr optimize(ReadOnlyDataTripleSource tripleSource, StrictEvaluationStrategy strategy,
			TupleExpr tupleExpr, BindingSet bindings) {
		ReadOnlyEvaluationStatistics evStats = new ReadOnlyEvaluationStatistics(sail);
		ReadOnlyQueryOptimizerPipeline queryOptimizer = new ReadOnlyQueryOptimizerPipeline(strategy, tripleSource,
				evStats);
		strategy.setOptimizerPipeline(queryOptimizer);
		TupleExpr optimizedTupleExpr = strategy.optimize(tupleExpr, evStats, bindings);
		return optimizedTupleExpr;
	}

	@Override
	protected CloseableIteration<? extends Resource> getContextIDsInternal() throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIteration<? extends Statement> getStatementsInternal(Resource subj, IRI pred,
			Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		ReadOnlyDataTripleSource tripleSource = new ReadOnlyDataTripleSource(sail.getValueFactory(), sail);
		CloseableIteration<? extends Statement> statements = tripleSource.getStatements(subj,
				pred, obj, contexts);
		return new CloseableIteration<Statement>() {

			@Override
			public boolean hasNext() throws SailException {
				return statements.hasNext();
			}

			@Override
			public Statement next() throws SailException {
				return statements.next();
			}

			@Override
			public void remove() throws SailException {
				statements.remove();
			}

			@Override
			public void close() throws SailException {
				statements.close();
			}
		};
	}

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {

		return 0;
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void commitInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void rollbackInternal() throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void addStatementInternal(Resource subj, IRI pred, Value obj, Resource... contexts) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void removeStatementsInternal(Resource subj, IRI pred, Value obj, Resource... contexts)
			throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void clearInternal(Resource... contexts) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected CloseableIteration<? extends Namespace> getNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getNamespaceInternal(String prefix) throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void setNamespaceInternal(String prefix, String name) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void removeNamespaceInternal(String prefix) throws SailException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub

	}
}
