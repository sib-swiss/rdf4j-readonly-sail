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

import java.util.Arrays;

import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizerPipeline;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.RegexAsStringFunctionOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;

public class ReadOnlyQueryOptimizerPipeline implements QueryOptimizerPipeline {

	private final TripleSource ts;
	private final StrictEvaluationStrategy strategy;
	private final EvaluationStatistics ev;

	public ReadOnlyQueryOptimizerPipeline(StrictEvaluationStrategy strategy, TripleSource ts, EvaluationStatistics ev) {
		this.ts = ts;
		this.strategy = strategy;
		this.ev = ev;
	}

	@Override
	public Iterable<QueryOptimizer> getOptimizers() {
		return Arrays.asList(new BindingAssigner(), new ConstantOptimizer(strategy),
				new RegexAsStringFunctionOptimizer(ts.getValueFactory()), new CompareOptimizer(),
				new ConjunctiveConstraintSplitter(), new DisjunctiveConstraintOptimizer(),
				new SameTermFilterOptimizer(), new QueryModelNormalizer(), new QueryJoinOptimizer(ev),
				new IterativeEvaluationOptimizer(), new FilterOptimizer());
	}

}
