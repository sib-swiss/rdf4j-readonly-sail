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

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public record ReadOnlyGYear(int year) implements Literal, ReadOnlyValue {
	private static final long serialVersionUID = 1L;
	private static final DatatypeFactory dtFactory;

	public static ReadOnlyGYear fromLong(long l) {
		return new ReadOnlyGYear((int) l);
	}

	public static long toLong(Literal rogy) {
		if (rogy instanceof ReadOnlyGYear) {
			return ((ReadOnlyGYear) rogy).year;
		} else {
			if (rogy.getCoreDatatype() == CoreDatatype.XSD.GYEAR && "0".equals(rogy.getLabel())) {
				return 0L;
			} else {
				return rogy.calendarValue().getYear();
			}
		}
	}

	static {
		try {
			dtFactory = DatatypeFactory.newInstance();
		} catch (DatatypeConfigurationException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * gYear should be in format YYYY
	 */
	@Override
	public String stringValue() {
		if (year == 0) {
			return "0000";
		} else if (year < 10) {
			return "000" + year;
		} else if (year < 100) {
			return "00" + year;
		} else if (year < 1000) {
			return "0" + year;
		}
		return Integer.toString(year);
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
		return XSD.GYEAR;
	}

	@Override
	public boolean booleanValue() {
		return year != 0;
	}

	@Override
	public byte byteValue() {
		return (byte) year;
	}

	@Override
	public short shortValue() {
		return (short) year;
	}

	@Override
	public int intValue() {
		return year;
	}

	@Override
	public long longValue() {
		return year;
	}

	@Override
	public BigInteger integerValue() {
		return BigInteger.valueOf(year);
	}

	@Override
	public BigDecimal decimalValue() {
		return BigDecimal.valueOf(year);
	}

	@Override
	public float floatValue() {
		return (float) year;
	}

	@Override
	public double doubleValue() {
		return (double) year;
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return dtFactory.newXMLGregorianCalendar(year, 0, 0, 0, 0, 0, 0, 0);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof ReadOnlyGYear) {
			return year == ((ReadOnlyGYear) o).year;
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
		return year;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.XSD.GYEAR;
	}
}
