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

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DefaultEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.evaluationsteps.JoinQueryEvaluationStep;

import swiss.sib.swissprot.sail.readonly.evaluation.ReadOnlyMergeJoinQueryEvaluationStep;

public class ReadOnlyQueryStrictEvaluationStrategy extends DefaultEvaluationStrategy {

	public ReadOnlyQueryStrictEvaluationStrategy(ReadOnlyDataTripleSource tripleSource,
			FederatedServiceResolver serviceResolver) {
		super(tripleSource, serviceResolver);
	}

	public ReadOnlyQueryStrictEvaluationStrategy(ReadOnlyDataTripleSource tripleSource, Dataset dataset,
			FederatedServiceResolver serviceResolver) {
		super(tripleSource, dataset, serviceResolver);
	}

	protected QueryEvaluationStep prepare(Join node, QueryEvaluationContext context) throws QueryEvaluationException {
		if (ReadOnlyMergeJoinQueryEvaluationStep.isApplicableTo(node)) {
			StatementPattern left = (StatementPattern) node.getLeftArg();
			StatementPattern right = (StatementPattern) node.getRightArg();
//			return new ReadOnlyMergeJoinQueryEvaluationStep(left, right, tripleSource, context);
		}
		return new JoinQueryEvaluationStep(this, node, context);
	}
}
