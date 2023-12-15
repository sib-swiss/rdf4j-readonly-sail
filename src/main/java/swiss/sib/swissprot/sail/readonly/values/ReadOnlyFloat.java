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
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public record ReadOnlyFloat(float value) implements Literal, ReadOnlyValue {
	private static final long serialVersionUID = 1L;

	public static ReadOnlyFloat fromLong(long l) {
		return new ReadOnlyFloat(Float.intBitsToFloat((int) l));
	}

	public static long toLong(Literal rogy) {
		return Float.floatToIntBits(rogy.floatValue());
	}

	@Override
	public String stringValue() {
		return Double.toString(value);
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
		return XSD.FLOAT;
	}

	@Override
	public boolean booleanValue() {
		return value != 0;
	}

	@Override
	public byte byteValue() {
		return (byte) value;
	}

	@Override
	public short shortValue() {
		return (short) value;
	}

	@Override
	public int intValue() {
		return (int) value;
	}

	@Override
	public long longValue() {
		return (long) value;
	}

	@Override
	public BigInteger integerValue() {
		return BigInteger.valueOf((long) value);
	}

	@Override
	public BigDecimal decimalValue() {
		return BigDecimal.valueOf(value);
	}

	@Override
	public float floatValue() {
		return (float) value;
	}

	@Override
	public double doubleValue() {
		return (double) value;
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		throw new IllegalArgumentException("float is not a calendar");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o instanceof ReadOnlyFloat) {
			return value == ((ReadOnlyFloat) o).value;
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
		return Float.floatToIntBits(value);
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.XSD.FLOAT;
	}
}
