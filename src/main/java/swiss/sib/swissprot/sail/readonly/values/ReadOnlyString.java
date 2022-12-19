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
import org.eclipse.rdf4j.model.vocabulary.XSD;

import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;

public class ReadOnlyString implements Literal, ReadOnlyValue {

	private static final long serialVersionUID = 1L;
	private final long id;
	private final SortedList<? extends Value> backingstore;
	static final long FIRST_BIT_SET = 0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000l;

	ReadOnlyString(long id, SortedList<? extends Value> backingstore) {
		super();
		this.id = id;
		this.backingstore = backingstore;
	}

	@Override
	public String stringValue() {
		if ((id & FIRST_BIT_SET) == FIRST_BIT_SET)
			return new ShortString(id).toString();
		else
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
		return XSD.STRING;
	}

	@Override
	public boolean booleanValue() {
		return false;
	}

	@Override
	public byte byteValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short shortValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int intValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long longValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public BigInteger integerValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal decimalValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float floatValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double doubleValue() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof ReadOnlyString) {
			ReadOnlyString os = (ReadOnlyString) o;
			return id == os.id && backingstore == os.backingstore;
		}

		return LiteralFunctions.standardLiteralEquals(this, o);
	}

	// overrides Object.hashCode(), implements Literal.hashCode()
	@Override
	public int hashCode() {
		return getLabel().hashCode();
	}

	@Override
	public long id() {
		return id;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.XSD.STRING;
	}
}
