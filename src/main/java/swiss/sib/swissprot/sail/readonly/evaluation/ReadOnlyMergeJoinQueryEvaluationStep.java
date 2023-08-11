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
package swiss.sib.swissprot.sail.readonly.evaluation;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.common.iteration.FilterIteration;
import org.eclipse.rdf4j.common.iteration.LookAheadIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryEvaluationStep;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryEvaluationContext;

import swiss.sib.swissprot.sail.readonly.ReadOnlyDataTripleSource;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyValue;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyValueComparator;

public class ReadOnlyMergeJoinQueryEvaluationStep implements QueryEvaluationStep {

	private final QueryEvaluationContext context;
	private final TripleSource tripleSource;
	private final StatementPattern left;
	private final StatementPattern right;
	private final BiPredicate<Statement, Statement> svf;
	private final Predicate<Statement> filterLeftForSameVariables;
	private final Predicate<Statement> filterRightForSameVariables;
	private final BiFunction<BindingSet, List<Statement>, BindingSet> bindingMaker;
	private final Predicate<BindingSet> unboundTest;
	private final boolean doubleResults;

	public ReadOnlyMergeJoinQueryEvaluationStep(StatementPattern left, StatementPattern right,
			TripleSource tripleSource, QueryEvaluationContext context) {
		this.left = left;
		this.right = right;
		this.tripleSource = tripleSource;
		this.context = context;
		this.filterLeftForSameVariables = makeSameVariableFilter(left);
		this.filterRightForSameVariables = makeSameVariableFilter(right);
		this.svf = makeSameVariableFilter(left, right);
		this.bindingMaker = makeBindingMaker(left, right);
		Predicate<BindingSet> anyLeftBound = isAnyBound(left, context);
		Predicate<BindingSet> anyRightBound = isAnyBound(left, context);
		Predicate<BindingSet> isNotEmpty = Predicate.not(BindingSet::isEmpty);
		doubleResults = sameVarPredicate(left, right);
		unboundTest = isNotEmpty.and(anyLeftBound.or(anyRightBound));
	}

	private boolean sameVarPredicate(StatementPattern left, StatementPattern right) {
		if (left.getPredicateVar() != null && right.getPredicateVar() != null) {
			if (left.getPredicateVar().equals(right.getPredicateVar())) {
				if (left.getObjectVar() == null && right.getObjectVar() == null) {
					return false;
				} else if (left.getObjectVar() != null && right.getObjectVar() == null) {
					return true;
				} else if (left.getObjectVar() == null && right.getObjectVar() != null) {
					return true;
				} else if (!left.getObjectVar().isConstant() && !right.getObjectVar().isConstant()) {
					return !left.getObjectVar().equals(right.getObjectVar());
				} else
					return true;
			}
		}
		return false;
	}

	private Predicate<BindingSet> isAnyBound(StatementPattern statementPattern, QueryEvaluationContext context) {
		final Var subjVar = statementPattern.getSubjectVar();
		final Var predVar = statementPattern.getPredicateVar();
		final Var objVar = statementPattern.getObjectVar();
		final Var conVar = statementPattern.getContextVar();
		Predicate<BindingSet> isSubjBound = unbound(subjVar, context);
		Predicate<BindingSet> isPredBound = unbound(predVar, context);
		Predicate<BindingSet> isObjBound = unbound(objVar, context);
		Predicate<BindingSet> isConBound = unbound(conVar, context);
		Predicate<BindingSet> anyBound = isSubjBound.or(isPredBound).or(isObjBound).or(isConBound);
		return anyBound;
	}

	private static Predicate<BindingSet> unbound(final Var var, QueryEvaluationContext context) {
		if (var == null) {
			return (bindings) -> false;
		} else {
			Predicate<BindingSet> hasBinding = context.hasBinding(var.getName());
			Function<BindingSet, Value> getValue = context.getValue(var.getName());
			Predicate<BindingSet> getBindingIsNull = (binding) -> getValue.apply(binding) == null;
			return hasBinding.and(getBindingIsNull);
		}
	}

	@Override
	public CloseableIteration<BindingSet> evaluate(BindingSet bindings) {
		if (unboundTest.test(bindings)) {
//		// the variable must remain unbound for this solution see
//		// https://www.w3.org/TR/sparql11-query/#assignment
			return new EmptyIteration<>();
		}
		CloseableIteration<? extends Statement> leftIter = statementIterator(left,
				filterLeftForSameVariables);
		CloseableIteration<? extends Statement> rightIter = statementIterator(right,
				filterRightForSameVariables);
//
		if (!leftIter.hasNext() || !rightIter.hasNext()) {
			return new EmptyIteration<>();
		}

		PreSortedMergingIterationImplementation next = new PreSortedMergingIterationImplementation(rightIter, leftIter,
				bindingMaker, svf, bindings);
		if (doubleResults) {
			return new LookAheadIteration<>() {
				private BindingSet current;

				@Override
				protected BindingSet getNextElement() {
					if (current != null) {
						BindingSet ne = current;
						current = null;
						return ne;
					} else if (next.hasNext()) {
						BindingSet ne = next.next();
						current = ne;
						return ne;
					} else {
						return null;
					}

				}
			};
		}
		return next;
	}

	private BiPredicate<Statement, Statement> makeSameVariableFilter(StatementPattern left, StatementPattern right) {
		// subject, subject is implicit already joined on the same value.
		BiPredicate<Statement, Statement> fil = null;
		if (left.getSubjectVar() != null) {
			if (left.getSubjectVar().equals(right.getPredicateVar())) {
				fil = addThen(fil, (l, r) -> l.getSubject().equals(r.getPredicate()));
			}
			if (left.getSubjectVar().equals(right.getObjectVar())) {
				fil = addThen(fil, (l, r) -> l.getSubject().equals(r.getObject()));
			}
			if (left.getSubjectVar().equals(right.getContextVar())) {
				fil = addThen(fil, (l, r) -> l.getSubject().equals(r.getContext()));
			}
		}
		if (left.getPredicateVar() != null) {
			if (left.getPredicateVar().equals(right.getPredicateVar())) {
				fil = addThen(fil, (l, r) -> l.getPredicate().equals(r.getPredicate()));
			}

			if (left.getPredicateVar().equals(right.getObjectVar())) {
				fil = addThen(fil, (l, r) -> l.getPredicate().equals(r.getObject()));
			}
			if (left.getPredicateVar().equals(right.getContextVar())) {
				fil = addThen(fil, (l, r) -> l.getPredicate().equals(r.getContext()));
			}
		}
		if (left.getObjectVar() != null) {
			if (left.getObjectVar().equals(right.getPredicateVar())) {
				fil = addThen(fil, (l, r) -> l.getObject().equals(r.getPredicate()));
			}

			if (left.getObjectVar().equals(right.getObjectVar())) {
				fil = addThen(fil, (l, r) -> l.getObject().equals(r.getObject()));
			}
			if (left.getObjectVar().equals(right.getContextVar())) {
				fil = addThen(fil, (l, r) -> l.getObject().equals(r.getContext()));
			}
		}
		if (left.getContextVar() != null) {
			if (left.getContextVar().equals(right.getPredicateVar())) {
				fil = addThen(fil, (l, r) -> l.getContext().equals(r.getPredicate()));
			}

			if (left.getContextVar().equals(right.getObjectVar())) {
				fil = addThen(fil, (l, r) -> l.getContext().equals(r.getObject()));
			}
			if (left.getContextVar().equals(right.getContextVar())) {
				fil = addThen(fil, (l, r) -> l.getContext().equals(r.getContext()));
			}
		}
		return fil;
	}

	private CloseableIteration<? extends Statement> statementIterator(StatementPattern sp,
			Predicate<Statement> filter) {
		CloseableIteration<? extends Statement> raw = getStatements(
				(ReadOnlyDataTripleSource) tripleSource, sp.getSubjectVar(), sp.getPredicateVar(), sp.getObjectVar(),
				sp.getContextVar());
		if (filter == null)
			return raw;
		else
			return new FilterByPredicateIteration(raw, filter);
	}

	private Predicate<Statement> makeSameVariableFilter(StatementPattern sp) {
		Predicate<Statement> fil = null;
		if (sp.getSubjectVar() != null) {
			if (sp.getSubjectVar().equals(sp.getPredicateVar())) {
				fil = addThen(fil, (s) -> s.getSubject().equals(s.getPredicate()));
			}
			if (sp.getSubjectVar().equals(sp.getObjectVar())) {
				fil = addThen(fil, (s) -> s.getSubject().equals(s.getObject()));
			}
			if (sp.getSubjectVar().equals(sp.getContextVar())) {
				fil = addThen(fil, (s) -> s.getSubject().equals(s.getContext()));
			}
		}
		if (sp.getPredicateVar() != null) {
			if (sp.getPredicateVar().equals(sp.getObjectVar())) {
				fil = addThen(fil, (s) -> s.getPredicate().equals(s.getObject()));
			}
			if (sp.getPredicateVar().equals(sp.getContextVar())) {
				fil = addThen(fil, (s) -> s.getPredicate().equals(s.getContext()));
			}
		}
		if (sp.getObjectVar() != null) {
			if (sp.getObjectVar().equals(sp.getContextVar())) {
				fil = addThen(fil, (s) -> s.getObject().equals(s.getContext()));
			}
		}
		return fil;
	}

	private Predicate<Statement> addThen(Predicate<Statement> fil, Predicate<Statement> s) {
		if (fil == null)
			fil = s;
		return fil.and(s);
	}

	private BiPredicate<Statement, Statement> addThen(BiPredicate<Statement, Statement> fil,
			BiPredicate<Statement, Statement> s) {
		if (fil == null)
			fil = s;
		return fil.and(s);
	}

	private BiFunction<BindingSet, List<Statement>, BindingSet> makeBindingMaker(StatementPattern left,
			StatementPattern right) {

		return (b, l) -> {
			MutableBindingSet result = context.createBindingSet(b);

			valuesFromStatementIntoBindingSet(result, left, l.get(0));
			valuesFromStatementIntoBindingSet(result, right, l.get(1));

			return result;
		};
	}

	private static final class PreSortedMergingIterationImplementation
			implements CloseableIteration<BindingSet> {
		private final CloseableIteration<? extends Statement> rightIter;
		private final CloseableIteration<? extends Statement> leftIter;
		private Statement left;
		private Statement right;
		private static final Comparator<Value> comp = new ReadOnlyValueComparator();
		private final BiFunction<BindingSet, List<Statement>, BindingSet> bindingMaker;
		private final BiPredicate<Statement, Statement> filterSameVariables;
		private final BindingSet bindings;

		private PreSortedMergingIterationImplementation(

				CloseableIteration<? extends Statement> leftIter,
				CloseableIteration<? extends Statement> rightIter,
				BiFunction<BindingSet, List<Statement>, BindingSet> bindingMaker,
				BiPredicate<Statement, Statement> filterSameVariables, BindingSet bindings) {
			this.rightIter = rightIter;
			this.leftIter = leftIter;
			this.bindingMaker = bindingMaker;
			this.bindings = bindings;
			if (filterSameVariables == null)
				this.filterSameVariables = filterSameVariables;
			else
				this.filterSameVariables = (l, r) -> true;
			this.left = leftIter.next();
			this.right = rightIter.next();
		}

		private void advance() {
			if (rightIter.hasNext())
				this.right = rightIter.next();
			else
				this.right = null;
			while (left != null && right != null && !left.getSubject().equals(right.getSubject())
					&& !filterSameVariables.test(left, right)) {
				int compare = comp.compare(left.getSubject(), right.getSubject());
				if (compare == -1) {
					if (leftIter.hasNext()) {
						this.left = leftIter.next();
					} else {
						this.left = null;
					}
				} else if (compare == 1) {
					if (rightIter.hasNext())
						this.right = rightIter.next();
					else {
						this.right = null;
					}
				}
			}
		}

		@Override
		public boolean hasNext() throws QueryEvaluationException {
			return left != null && right != null;
		}

		@Override
		public BindingSet next() throws QueryEvaluationException {
			try {
				return bindingMaker.apply(bindings, List.of(left, right));
			} finally {
				advance();
			}
		}

		@Override
		public void remove() throws QueryEvaluationException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws QueryEvaluationException {
			leftIter.close();
			rightIter.close();
		}
	}

	private CloseableIteration<? extends Statement> getStatements(
			ReadOnlyDataTripleSource tripleSource, Var subjectVar, Var predicateVar, Var objectVar, Var contextVar) {
		Resource subject = null;
		if (subjectVar != null && subjectVar.isConstant() && subjectVar instanceof IRI) {
			subject = tripleSource.getValueFactory().tryToConvertIri((IRI) subjectVar.getValue());
			if (!(subject instanceof ReadOnlyValue))
				return new EmptyIteration<>();
		}
		IRI predicate = null;
		if (predicateVar != null && predicateVar.isConstant()) {
			predicate = tripleSource.getValueFactory().tryToConvertIri((IRI) predicateVar.getValue());
			if (!(predicate instanceof ReadOnlyValue))
				return new EmptyIteration<>();
		}
		Value object = null;
		if (objectVar != null && objectVar.isConstant()) {
			object = tripleSource.getValueFactory().tryToConvertValue(objectVar.getValue());
			if (!(object instanceof ReadOnlyValue))
				return new EmptyIteration<>();
		}
		Resource context = null;
		if (contextVar != null && contextVar.isConstant())
			context = (Resource) contextVar.getValue();
		return tripleSource.getStatements(subject, predicate, object, context);
	}

	private final class FilterByPredicateIteration extends FilterIteration<Statement> {
		private final Predicate<Statement> filterLeftForSameVariables;

		private FilterByPredicateIteration(CloseableIteration<? extends Statement> iter,
				Predicate<Statement> filterLeftForSameVariables) {
			super(iter);
			this.filterLeftForSameVariables = filterLeftForSameVariables;
		}

		@Override
		protected boolean accept(Statement object) throws QueryEvaluationException {
			return filterLeftForSameVariables.test(object);
		}
	}

	private void valuesFromStatementIntoBindingSet(MutableBindingSet result, StatementPattern sp, Statement st) {
		Var subjVar = sp.getSubjectVar();
		Var predVar = sp.getPredicateVar();
		Var objVar = sp.getObjectVar();
		Var conVar = sp.getContextVar();
		if (subjVar != null && !subjVar.isConstant() && !result.hasBinding(subjVar.getName())) {
			result.addBinding(subjVar.getName(), st.getSubject());
		}
		if (predVar != null && !predVar.isConstant() && !result.hasBinding(predVar.getName())) {
			result.addBinding(predVar.getName(), st.getPredicate());
		}
		if (objVar != null && !objVar.isConstant() && !result.hasBinding(objVar.getName())) {
			result.addBinding(objVar.getName(), st.getObject());
		}
		if (conVar != null && !conVar.isConstant() && !result.hasBinding(conVar.getName()) && st.getContext() != null) {
			result.addBinding(conVar.getName(), st.getContext());
		}
	}

	public static boolean isApplicableTo(Join node) {
		TupleExpr leftArg = node.getLeftArg();
		TupleExpr rightArg = node.getRightArg();
		if (leftArg instanceof StatementPattern && rightArg instanceof StatementPattern) {
			StatementPattern left = (StatementPattern) leftArg;
			StatementPattern right = (StatementPattern) rightArg;
			return (left.getSubjectVar() != null && right.getSubjectVar() != null
					&& left.getSubjectVar().equals(right.getSubjectVar()));
		}
		return false;
	}

}
