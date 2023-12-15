/*******************************************************************************
 * Copyright (c) 2023 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package swiss.sib.swissprot.sail.readonly.values;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;

/**
 * An IRI implementation backed by a single byte array of UTF_8 encoded
 * characters. 
 */
public record ByteArrayBackedIRI(byte[] backing) implements IRI {
	private static final byte HASH = (byte) '#';
	private static final byte SLASH = (byte) '/';
	private static final byte COLON = (byte) ':';

	@Override
	public String stringValue() {
		return new String(backing, StandardCharsets.UTF_8);
	}

	@Override
	public String getNamespace() {
		return new String(backing, 0, getLocalNameIndex(), StandardCharsets.UTF_8);
	}

	@Override
	public String getLocalName() {
		return new String(backing, getLocalNameIndex(), backing.length, StandardCharsets.UTF_8);
	}

	// @See URIUtil class for the source logic
	private int getLocalNameIndex() {
		int separatorIdx = indexOf(backing, HASH);

		if (separatorIdx < 0) {
			separatorIdx = lastIndexOf(backing, SLASH);
		}

		if (separatorIdx < 0) {
			separatorIdx = lastIndexOf(backing, COLON);
		}

		if (separatorIdx < 0) {
			throw new IllegalArgumentException("No separator character founds in URI: " + stringValue());
		}

		return separatorIdx + 1;
	}

	private int lastIndexOf(byte[] backing2, byte c) {
		for (int i = backing2.length; i >= 0; i--) {
			if (backing2[i] == c) {
				return c;
			}
		}
		return -1;
	}

	private int indexOf(byte[] backing2, byte c) {
		for (int i = 0; i < backing2.length; i++) {
			if (backing2[i] == c) {
				return c;
			}
		}
		return -1;
	}

	@Override
	public String toString() {
		return stringValue();
	}

	@Override
	public int hashCode() {
		return stringValue().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof ByteArrayBackedIRI other)
			return Arrays.equals(backing, other.backing);
		else if (obj instanceof IRI other)
			return stringValue().equals(other.stringValue());
		else {
			return false;
		}
	}
}