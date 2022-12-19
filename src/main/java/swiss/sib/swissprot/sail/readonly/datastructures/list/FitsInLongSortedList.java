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
package swiss.sib.swissprot.sail.readonly.datastructures.list;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.base.CoreDatatype.XSD;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;
import swiss.sib.swissprot.sail.readonly.datastructures.roaringbitmap.Roaring64BitmapAdder;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyBooleanLiteral;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyDouble;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyGYear;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyInt;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyLong;

/**
 * If the complete range of a well formed literal can fit in 64 bits. We can store it in a sorted list backed by a
 * Roaring64Bitmap to test for presence.
 *
 * @param <T>
 */
public record FitsInLongSortedList(LongFunction<Literal> reconstructor, ToLongFunction<Literal> deconstructor,
		LongBitmapDataProvider present) implements SortedList<Value> {
	public enum FitingDatatypes {
		LONG(ReadOnlyLong::fromLong, ReadOnlyLong::toLong, CoreDatatype.XSD.LONG),
		INT(ReadOnlyInt::fromLong, ReadOnlyInt::toLong, CoreDatatype.XSD.INT),
		BOOLEAN(ReadOnlyBooleanLiteral::fromLong, ReadOnlyBooleanLiteral::toLong, CoreDatatype.XSD.BOOLEAN),
		DOUBLE(ReadOnlyDouble::fromLong, ReadOnlyDouble::toLong, CoreDatatype.XSD.DOUBLE),
		GYEAR(ReadOnlyGYear::fromLong, ReadOnlyGYear::toLong, CoreDatatype.XSD.GYEAR);

		private final LongFunction<Literal> reconstructor;
		private final ToLongFunction<Literal> deconstructor;
		private final XSD coreDatatype;

		FitingDatatypes(LongFunction<Literal> reconstructor, ToLongFunction<Literal> deconstructor, XSD coreDatatype) {
			this.reconstructor = reconstructor;
			this.deconstructor = deconstructor;
			this.coreDatatype = coreDatatype;
		}

		public FitsInLongSortedList of(Roaring64Bitmap present) {
			return new FitsInLongSortedList(reconstructor, deconstructor, present);
		}

		public static FitingDatatypes forDatatype(IRI datatype) {
			for (FitingDatatypes ftd : values()) {
				if (ftd.coreDatatype.getIri().equals(datatype)) {
					return ftd;
				}
			}
			return null;
		}
	}

	public long positionOf(Value element) throws IOException {
		if (element instanceof Literal) {
			long asLong = deconstructor.applyAsLong((Literal) element);
			if (present.contains(asLong)) {
				return present.rankLong(asLong);
			}
		}
		return WriteOnce.NOT_FOUND;

	}

	public IterateInSortedOrder<Value> iterator() throws IOException {
		// This is in order by default so we use it
		final LongIterator longIterator = present.getLongIterator();
		return new IterateInSortedOrder<>() {
			long rank = 0;

			@Override
			public boolean hasNext() {
				return longIterator.hasNext();
			}

			@Override
			public TPosition<Value> next() {
				long next = longIterator.next();
				return new TPosition<>(reconstructor.apply(next), rank++);
			}

			@Override
			public void advanceNear(Value t) {

			}
		};
	}

	public Literal get(long rank) {
		if (rank == WriteOnce.NOT_FOUND) {
			return null;
		}
		long value = present.select(rank);
		return reconstructor.apply(value);
	}

	@Override
	public Function<Value, TPosition<Value>> searchInOrder() throws IOException {
		return new Function<>() {

			@Override
			public TPosition<Value> apply(Value tosearchfor) {
				long positionOf;
				try {
					positionOf = positionOf(tosearchfor);
					if (positionOf == WriteOnce.NOT_FOUND) {
						return null;
					} else {
						return new TPosition<>(tosearchfor, positionOf);
					}
				} catch (IOException e) {
					throw new RuntimeException("Could not find " + tosearchfor + " in store", e);
				}
			}

		};
	}

	public static SortedList<Value> readInValues(File file, FitingDatatypes datatype)
			throws FileNotFoundException, IOException {
		try (FileInputStream fis = new FileInputStream(file); ObjectInputStream bis = new ObjectInputStream(fis)) {

			LongBitmapDataProvider rb = Roaring64BitmapAdder.readLongBitmapDataProvider(bis);
			return new FitsInLongSortedList(datatype.reconstructor, datatype.deconstructor, rb);
		}
	}

	public static void rewriteValues(Iterator<Value> sortedInput, File valueFile, FitingDatatypes forDatatype)
			throws FileNotFoundException, IOException {
		Roaring64BitmapAdder collector = new Roaring64BitmapAdder();
		while (sortedInput.hasNext()) {
			long asLong = forDatatype.deconstructor.applyAsLong((Literal) sortedInput.next());
			collector.add(asLong);
		}
		try (FileOutputStream fos = new FileOutputStream(valueFile);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				ObjectOutputStream dos = new ObjectOutputStream(bos)) {
			LongBitmapDataProvider values = collector.build();
			Roaring64BitmapAdder.writeLongBitmapDataProvider(dos, values);
		}
	}
}
