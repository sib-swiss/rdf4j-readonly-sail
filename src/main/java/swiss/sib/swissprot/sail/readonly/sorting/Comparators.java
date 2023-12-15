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
package swiss.sib.swissprot.sail.readonly.sorting;

import static java.nio.ByteBuffer.wrap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.algebra.evaluation.util.ValueComparator;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO.IO;

/**
 * A set of comparator functions that are SPARQL value comparisons equivalent but
 * work on a lower level byte[] representations.
 */
public class Comparators {

	private static final IO IRIIO = RawIO.forOutput(Kind.IRI);
	private static final ValueComparator VC = new ValueComparator();
	private static final Comparator<byte[]> IRI_COMPARATOR_BYTES = forIRIBytes();
	private static final Comparator<IRI> IRI_COMPARATOR = (a, b) -> {
		byte[] ab = getBytes(a);
		byte[] bb = getBytes(b);
		return IRI_COMPARATOR_BYTES.compare(ab, bb);
	};

	private static byte[] getBytes(IRI a) {
		return IRIIO.getBytes(a);
	}

	private Comparators() {

	}

	/**
	 * Retrieve a comparator for the byte[] representation of for a datatype used in the store. 
	 * 
	 * @param datatype for the values that will be compared.
	 * @return a Comparator
	 */
	public static Comparator<byte[]> forRawBytesOfDatatype(IRI datatype) {
		CoreDatatype from = CoreDatatype.from(datatype);
		if (from != null && from.isXSDDatatype()) {
			switch (from.asXSDDatatype().get()) {
			case INT:
				return Comparators::compareIntByteArrays;
			case LONG:
				return (a, b) -> Long.compare(wrap(a).getLong(0), wrap(b).getLong(0));
			case FLOAT:
				return (a, b) -> Float.compare(wrap(a).getFloat(0), wrap(b).getFloat(0));
			case DOUBLE:
				return (a, b) -> Double.compare(wrap(a).getDouble(0), wrap(b).getDouble(0));
			case STRING:
				return Arrays::compare;
			default:
				return comparatorFor(datatype);
			}
		}
		return comparatorFor(datatype);
	}

	private static int compareIntByteArrays(byte[] a, byte[] b) {
		return Integer.compare(wrap(a).getInt(0), wrap(b).getInt(0));
	}

	public static Comparator<byte[]> forLangStringBytes() {
		return Arrays::compare;
	}

	public static Comparator<byte[]> forIRIBytes() {
		return Arrays::compare;
	}

	public static Comparator<IRI> forIRI() {
		return IRI_COMPARATOR;
	}

	public static Comparator<byte[]> forBNodeBytes() {
		return (a, b) -> Long.compare(ByteBuffer.wrap(a).getLong(0), ByteBuffer.wrap(b).getLong(0));
	}

	public static Comparator<BNode> forBNode() {
		return (a, b) -> Long.compare(Long.parseLong(a.getID()), Long.parseLong(b.getID()));
	}

	public static Comparator<Literal> forLangString() {
		return (a, b) -> {
			Literal al = (Literal) a;
			Literal bl = (Literal) b;
			return Arrays.compare(al.stringValue().getBytes(StandardCharsets.UTF_8),
					bl.stringValue().getBytes(StandardCharsets.UTF_8));
		};
	}

	public static Comparator<Value> forValueOfDatatype(IRI datatype) {
		CoreDatatype from = CoreDatatype.from(datatype);
		if (from != null && from.isXSDDatatype()) {
			switch (from.asXSDDatatype().get()) {
			case INT:
				return (a, b) -> {
					Literal al = (Literal) a;
					Literal bl = (Literal) b;
					return Integer.compare(al.intValue(), bl.intValue());
				};
			case LONG:
				return (a, b) -> {
					Literal al = (Literal) a;
					Literal bl = (Literal) b;
					return Long.compare(al.longValue(), bl.longValue());
				};
			case FLOAT:
				return (a, b) -> {
					Literal al = (Literal) a;
					Literal bl = (Literal) b;
					return Float.compare(al.floatValue(), bl.floatValue());
				};
			case DOUBLE:
				return (a, b) -> {
					Literal al = (Literal) a;
					Literal bl = (Literal) b;
					return Double.compare(al.doubleValue(), bl.doubleValue());
				};
			case STRING:
				return (a, b) -> {
					Literal al = (Literal) a;
					Literal bl = (Literal) b;
					return Arrays.compare(al.stringValue().getBytes(StandardCharsets.UTF_8),
							bl.stringValue().getBytes(StandardCharsets.UTF_8));
				};
			default:
				return VC;
			}
		}
		return VC;
	}

	private static Comparator<byte[]> comparatorFor(IRI datatype) {
		IO io = RawIO.forOutput(Kind.LITERAL, datatype, null);
		return (a, b) -> {
			Value av = io.read(a);
			Value bv = io.read(b);
			return VC.compare(av, bv);
		};
	}

	/**
	 * For a given kind of value and if a literal the datatype or long give a valid comparator.
	 * @param kind of value
	 * @param datatype if a literal
	 * @param lang else a lang if a literal (exclusive with datatype)
	 * @return the Comparator
	 */
	public static Comparator<byte[]> byteComparatorFor(Kind kind, IRI datatype, String lang) {
		switch (kind) {
		case IRI:
			return forIRIBytes();
		case BNODE:
			return forBNodeBytes();
		case LITERAL: {
			if (lang != null) {
				return forLangStringBytes();
			} else {
				return forRawBytesOfDatatype(datatype);
			}
		}
		case TRIPLE:
		default:
			throw new UnsupportedOperationException("No RDF-star support yet");
		}
	}

	/**
	 * For a given kind of value
	 * @param kind of value
	 * @return the Comparator
	 */
	public static Comparator<byte[]> byteComparatorFor(Kind kind) {
		switch (kind) {
		case IRI:
			return forIRIBytes();
		case BNODE:
			return forBNodeBytes();
		case LITERAL:
			throw new IllegalArgumentException(
					"No generalized RDF support! Can't have a literal in the object position");
		case TRIPLE:
		default:
			throw new UnsupportedOperationException("No RDF-star support yet");
		}
	}

}
