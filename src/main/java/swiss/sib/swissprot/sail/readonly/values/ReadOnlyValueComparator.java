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
package swiss.sib.swissprot.sail.readonly.values;

import java.util.Optional;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryEvaluationUtil;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;

/**
 * A comparator that compares values according the SPARQL value ordering as specified in
 * <A href="http://www.w3.org/TR/rdf-sparql-query/#modOrderBy">SPARQL Query Language for RDF</a>.
 *
 * Overrides a few methods to use the sorted in order behaviour of the ReadOnlyStore
 *
 * @author <a href="mailto:jerven.bolleman@sib.swiss">Jerven Bolleman</a>
 * @author james
 * @author Arjohn Kampman
 */
public class ReadOnlyValueComparator extends ValueComparator {

	private boolean strict = true;

	@Override
	public int compare(Value o1, Value o2) {
		// check equality
		if (o1 == o2) {
			return 0;
		}

		// 1. (Lowest) no value assigned to the variable
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}

		// 2. Blank nodes
		boolean b1 = o1.isBNode();
		boolean b2 = o2.isBNode();
		if (b1 && b2) {
			return compareBNodes((BNode) o1, (BNode) o2);
		}
		if (b1) {
			return -1;
		}
		if (b2) {
			return 1;
		}

		// 3. IRIs
		boolean iri1 = o1.isIRI();
		boolean iri2 = o2.isIRI();
		if (iri1 && iri2) {
			return compareURIs((IRI) o1, (IRI) o2);
		}
		if (iri1) {
			return -1;
		}
		if (iri2) {
			return 1;
		}

		// 4. Literals
		boolean l1 = o1.isLiteral();
		boolean l2 = o2.isLiteral();
		if (l1 && l2) {
			return compareLiterals((Literal) o1, (Literal) o2);
		}
		if (l1) {
			return -1;
		}
		if (l2) {
			return 1;
		}

		// 5. RDF-star triples
		return compareTriples((Triple) o1, (Triple) o2);
	}

	public void setStrict(boolean flag) {
		this.strict = flag;
	}

	public boolean isStrict() {
		return this.strict;
	}

	private int compareBNodes(BNode leftBNode, BNode rightBNode) {
		if (leftBNode instanceof ReadOnlyBlankNode && rightBNode instanceof ReadOnlyBlankNode)
			return Long.compare(((ReadOnlyBlankNode) leftBNode).id(), ((ReadOnlyBlankNode) rightBNode).id());
		else
			return leftBNode.getID().compareTo(rightBNode.getID());
	}

	private int compareURIs(IRI leftURI, IRI rightURI) {
		if (leftURI instanceof ReadOnlyIRI && rightURI instanceof ReadOnlyIRI)
			return Long.compare(((ReadOnlyIRI) leftURI).id(), ((ReadOnlyIRI) rightURI).id());
		else
			return leftURI.stringValue().compareTo(rightURI.stringValue());
	}

	private int compareLiterals(Literal leftLit, Literal rightLit) {
		// Additional constraint for ORDER BY: "A plain literal is lower
		// than an RDF literal with type xsd:string of the same lexical
		// form."

		if (!(QueryEvaluationUtil.isPlainLiteral(leftLit) || QueryEvaluationUtil.isPlainLiteral(rightLit))) {
			try {
				boolean isSmaller = QueryEvaluationUtil.compareLiterals(leftLit, rightLit, CompareOp.LT, strict);
				if (isSmaller) {
					return -1;
				} else {
					boolean isEquivalent = QueryEvaluationUtil.compareLiterals(leftLit, rightLit, CompareOp.EQ, strict);
					if (isEquivalent) {
						return 0;
					}
					return 1;
				}
			} catch (ValueExprEvaluationException e) {
				// literals cannot be compared using the '<' operator, continue
				// below
			}
		}

		int result = 0;

		// FIXME: Confirm these rules work with RDF-1.1
		// Sort by datatype first, plain literals come before datatyped literals
		IRI leftDatatype = leftLit.getDatatype();
		IRI rightDatatype = rightLit.getDatatype();

		if (leftDatatype != null) {
			if (rightDatatype != null) {
				// Both literals have datatypes
				CoreDatatype leftXmlDatatype = leftLit.getCoreDatatype();
				CoreDatatype rightXmlDatatype = rightLit.getCoreDatatype();

				if (leftXmlDatatype !=null && rightXmlDatatype !=null) {
					result = compareDatatypes(leftXmlDatatype, rightXmlDatatype);
				} else {
					result = compareDatatypes(leftDatatype, rightDatatype);
				}

			} else {
				result = 1;
			}
		} else if (rightDatatype != null) {
			result = -1;
		}

		if (result == 0) {
			// datatypes are equal or both literals are untyped; sort by language
			// tags, simple literals come before literals with language tags
			Optional<String> leftLanguage = leftLit.getLanguage();
			Optional<String> rightLanguage = rightLit.getLanguage();

			if (leftLanguage.isPresent()) {
				if (rightLanguage.isPresent()) {
					result = leftLanguage.get().compareTo(rightLanguage.get());
				} else {
					result = 1;
				}
			} else if (rightLanguage.isPresent()) {
				result = -1;
			}
		}

		if (result == 0) {
			// Literals are equal as fas as their datatypes and language tags are
			// concerned, compare their labels
			result = leftLit.getLabel().compareTo(rightLit.getLabel());
		}

		return result;
	}

	/**
	 * Compares two literal datatypes and indicates if one should be ordered after the other. This algorithm ensures
	 * that compatible ordered datatypes (numeric and date/time) are grouped together so that
	 * {@link QueryEvaluationUtil#compareLiterals(Literal, Literal, CompareOp)} is used in consecutive ordering steps.
	 */
	private int compareDatatypes(IRI leftDatatype, IRI rightDatatype) {
		if (XMLDatatypeUtil.isNumericDatatype(leftDatatype)) {
			if (XMLDatatypeUtil.isNumericDatatype(rightDatatype)) {
				// both are numeric datatypes
				return compareURIs(leftDatatype, rightDatatype);
			} else {
				return -1;
			}
		} else if (XMLDatatypeUtil.isNumericDatatype(rightDatatype)) {
			return 1;
		} else if (XMLDatatypeUtil.isCalendarDatatype(leftDatatype)) {
			if (XMLDatatypeUtil.isCalendarDatatype(rightDatatype)) {
				// both are calendar datatypes
				return compareURIs(leftDatatype, rightDatatype);
			} else {
				return -1;
			}
		} else if (XMLDatatypeUtil.isCalendarDatatype(rightDatatype)) {
			return 1;
		} else {
			// incompatible or unordered datatypes
			return compareURIs(leftDatatype, rightDatatype);
		}
	}

	private int compareDatatypes(CoreDatatype leftDatatype, CoreDatatype rightDatatype) {
		if (leftDatatype.isXSDDatatype() && rightDatatype.isXSDDatatype()) {
			CoreDatatype.XSD left = (CoreDatatype.XSD) leftDatatype;
			CoreDatatype.XSD right = (CoreDatatype.XSD) rightDatatype;
			if (left.isNumericDatatype()) {
				if (right.isNumericDatatype()) {
					// both are numeric datatypes
					return compareURIs(leftDatatype.getIri(), rightDatatype.getIri());
				} else {
					return -1;
				}
			} else if (right.isNumericDatatype()) {
				return 1;
			} else if (left.isCalendarDatatype()) {
				if (right.isCalendarDatatype()) {
					// both are calendar datatype
					return compareURIs(leftDatatype.getIri(), rightDatatype.getIri());
				} else {
					return -1;
				}
			} else if (right.isCalendarDatatype()) {
				return 1;
			} else {
				// incompatible or unordered datatype
				return compareURIs(leftDatatype.getIri(), rightDatatype.getIri());
			}
		} else {
			return compareURIs(leftDatatype.getIri(), rightDatatype.getIri());
		}
	}

	private int compareTriples(Triple leftTriple, Triple rightTriple) {
		int c = compare(leftTriple.getSubject(), rightTriple.getSubject());
		if (c == 0) {
			c = compare(leftTriple.getPredicate(), rightTriple.getPredicate());
			if (c == 0) {
				c = compare(leftTriple.getObject(), rightTriple.getObject());
			}
		}
		return c;
	}

}
