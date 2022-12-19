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

import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

import swiss.sib.swissprot.sail.readonly.datastructures.Triples;

public class ReadOnlyEvaluationStatistics extends EvaluationStatistics {

	private ReadOnlyStore tripleSource;

	public ReadOnlyEvaluationStatistics(ReadOnlyStore tripleSource) {
		this.tripleSource = tripleSource;
	}

	protected CardinalityCalculator createCardinalityCalculator() {
		return new CardinalityCalculator() {

			@Override
			protected double getPredicateCardinality(Var var) {
				if (var != null && var.isConstant() && var.getValue() instanceof IRI) {
					List<Triples> triples = tripleSource.getTriples((IRI) var.getValue());
					if (triples == null)
						return 0;
					double estimate = 0;
					for (Triples t : triples) {
						estimate += t.size();
					}
					return estimate;
				}
				return super.getPredicateCardinality(var);
			}

			@Override
			protected double getContextCardinality(Var var) {
				if (var != null && var.isConstant() && var.getValue() instanceof IRI i) {
					List<Triples> triples = tripleSource.getAllTriples();
					if (triples == null)
						return 0;
					double estimate = 0;
					for (Triples t : triples) {
						estimate += t.sizeOfContext(i);
					}
					return estimate;
				}
				return super.getContextCardinality(var);
			}

		};
	}
}
