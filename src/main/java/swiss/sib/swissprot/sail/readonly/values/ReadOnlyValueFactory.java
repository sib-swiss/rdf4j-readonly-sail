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

import java.io.IOException;
import java.util.Objects;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.AbstractValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.repository.RepositoryException;

import swiss.sib.swissprot.sail.readonly.ReadOnlyLiteralStore;
import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;

public class ReadOnlyValueFactory extends AbstractValueFactory {
	private final SortedList<Value> iris;
	private final ReadOnlyLiteralStore rols;
	private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

	public ReadOnlyValueFactory(SortedList<Value> iris, ReadOnlyLiteralStore rols) {
		super();
		this.iris = iris;
		this.rols = rols;
	}

	@Override
	public IRI createIRI(String iri) {
		long positionOf;
		try {
			positionOf = iris.positionOf(VF.createIRI(iri));
			if (positionOf == WriteOnce.NOT_FOUND)
				return super.createIRI(iri);
			else
				return new ReadOnlyIRI(positionOf, iris);
		} catch (IOException e) {
			throw new RepositoryException(e);
		}

	}

	@Override
	public IRI createIRI(String namespace, String localName) {
		return createIRI(namespace + localName);
	}

	@Override
	public Literal createLiteral(String label, IRI datatype) {

		Objects.requireNonNull(label, "null label");

		if (RDF.LANGSTRING.equals(datatype)) {
			throw new IllegalArgumentException("reserved datatype <" + datatype + ">");
		} else if (XSD.BOOLEAN.equals(datatype)) {
			switch (label) {
			case "true":
			case "1":
				return ReadOnlyBooleanLiteral.TRUE;
			default:
				return ReadOnlyBooleanLiteral.FALSE;
			}
		} else if (XSD.STRING.equals(datatype)) {
			return createLiteral(label);
		} else if (XSD.INT.equals(datatype)) {
			int value = Integer.parseInt(label);
			return new ReadOnlyInt(value);
		} else if (XSD.LONG.equals(datatype)) {
			long value = Long.parseLong(label);
			return new ReadOnlyLong(value);
		} else if (XSD.FLOAT.equals(datatype)) {
			float value = Float.parseFloat(label);
			return new ReadOnlyFloat(value);
		} else if (XSD.DOUBLE.equals(datatype)) {
			double value = Double.parseDouble(label);
			return new ReadOnlyDouble(value);
		}

		return super.createLiteral(label, datatype);
	}

	@Override
	public Literal createLiteral(String label) {
		if (ShortString.encodable(label)) {
			return new ReadOnlyString(new ShortString(label).value, rols.getSortedListForStrings());
		} else {
			long positionOf;
			try {
				positionOf = rols.getSortedListForStrings().positionOf(VF.createLiteral(label));
				if (positionOf != WriteOnce.NOT_FOUND)
					return new ReadOnlyString(positionOf, rols.getSortedListForStrings());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return super.createLiteral(label);
	}

	public Value tryToConvertValue(Value val) {
		if (val instanceof IRI)
			return tryToConvertIri((IRI) val);
		if (val instanceof Literal)
			return tryToConvertLiteral((Literal) val);
		else
			return val;
	}

	public IRI tryToConvertIri(IRI iri) {
		if (iri instanceof ReadOnlyIRI)
			return (ReadOnlyIRI) iri;
		else
			return createIRI(iri.stringValue());
	}

	public Literal tryToConvertLiteral(Literal lit) {
		if (lit instanceof ReadOnlyValue)
			return lit;
		else
			return createLiteral(lit.getLabel(), lit.getDatatype());
	}
}
