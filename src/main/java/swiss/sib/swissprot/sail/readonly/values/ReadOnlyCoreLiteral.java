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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;

import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;

public record ReadOnlyCoreLiteral(long id, SortedList<? extends Value> backingstore, CoreDatatype datatype)  implements Literal, ReadOnlyValue {

	private static final long serialVersionUID = 1L;
	
	@Override
	public String stringValue() {
		return backingstore.get(id).stringValue();
	}

	@Override
	public String getLabel() {
		return stringValue();
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public IRI getDatatype() {
		return datatype.getIri();
	}

	@Override
	public boolean booleanValue() {
		return false;
	}

	@Override
	public byte byteValue() {

		return ((Literal) backingstore.get(id)).byteValue();
	}

	@Override
	public short shortValue() {
		return ((Literal) backingstore.get(id)).shortValue();
	}

	@Override
	public int intValue() {
		return ((Literal) backingstore.get(id)).intValue();
	}

	@Override
	public long longValue() {
		return ((Literal) backingstore.get(id)).longValue();
	}

	@Override
	public BigInteger integerValue() {
		return ((Literal) backingstore.get(id)).integerValue();
	}

	@Override
	public BigDecimal decimalValue() {
		return ((Literal) backingstore.get(id)).decimalValue();
	}

	@Override
	public float floatValue() {
		return ((Literal) backingstore.get(id)).floatValue();
	}

	@Override
	public double doubleValue() {
		return ((Literal) backingstore.get(id)).doubleValue();
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return ((Literal) backingstore.get(id)).calendarValue();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof ReadOnlyCoreLiteral) {
			ReadOnlyCoreLiteral os = (ReadOnlyCoreLiteral) o;
			return id == os.id && backingstore == os.backingstore && this.datatype.equals(os.datatype);
		}

		return LiteralFunctions.standardLiteralEquals(this, o);
	}

	// overrides Object.hashCode(), implements Literal.hashCode()
	@Override
	public int hashCode() {
		return getLabel().hashCode();
	}

	@Override
	public String toString() {
		return stringValue();
	}

	@Override
	public long id() {
		return id;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return datatype;
	}
}
