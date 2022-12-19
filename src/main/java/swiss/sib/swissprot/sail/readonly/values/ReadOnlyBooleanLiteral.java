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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.model.vocabulary.XSD;

public class ReadOnlyBooleanLiteral extends SimpleLiteral implements Literal, ReadOnlyValue {

	private static final long serialVersionUID = 1L;
	private final boolean value;
	public static final ReadOnlyBooleanLiteral TRUE = new ReadOnlyBooleanLiteral(true);
	public static final ReadOnlyBooleanLiteral FALSE = new ReadOnlyBooleanLiteral(false);

	public static ReadOnlyBooleanLiteral fromLong(long l) {
		if (l == 1) {
			return TRUE;
		} else {
			return FALSE;
		}
	}

	public static long toLong(Literal rogy) {
		if (rogy.booleanValue()) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public String stringValue() {
		return Boolean.toString(value);
	}

	private ReadOnlyBooleanLiteral(boolean value) {
		super();
		this.value = value;
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
		return XSD.BOOLEAN;
	}

	@Override
	public boolean booleanValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof ReadOnlyBooleanLiteral) {
			return value == ((ReadOnlyBooleanLiteral) o).value;
		}

		return LiteralFunctions.standardLiteralEquals(this, o);
	}

	@Override
	public long id() {
		if (value)
			return 1;
		else
			return 0;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return CoreDatatype.XSD.BOOLEAN;
	}
}
