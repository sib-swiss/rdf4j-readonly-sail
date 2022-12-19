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
package swiss.sib.swissprot.sail.readonly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO;
import swiss.sib.swissprot.sail.readonly.datastructures.io.RawIO.IO;
import swiss.sib.swissprot.sail.readonly.datastructures.list.IterateInSortedOrder;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedListInSections;

public class SortedFloatListTest {
	private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void simpleTest() throws IOException {

		List<Float> strings = new ArrayList<>();
		for (int c = 0; c < 100; c++) {
			strings.add((float) c);
		}
		testFloats(strings);

	}

	@Test
	public void biggerTest() throws IOException {
		Random random = new Random();
		List<Float> strings = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			float length = random.nextFloat();
			strings.add(length);
		}

		List<Float> sorted = strings.stream().distinct().sorted((a, b) -> a.compareTo(b)).collect(Collectors.toList());
		testFloats(sorted);
	}

	@Test
	public void biggestTest() throws IOException {

		Random random = new Random();
		List<Float> strings = new ArrayList<>();
		for (int i = 0; i < 100_000; i++) {
			float length = random.nextFloat();
			strings.add(length);
		}

		List<Float> sorted = strings.stream().distinct().sorted((a, b) -> a.compareTo(b)).collect(Collectors.toList());
		testFloats(sorted);
	}

	private void testFloats(List<Float> strings) throws IOException, FileNotFoundException {
//		File newFile = temp.newFile();
		File newFile2 = temp.newFile();
		List<byte[]> rawFloats = new ArrayList<>(strings.size());
		IO fw = RawIO.forOutput(Kind.LITERAL, XSD.FLOAT, null);
		for (Float f : strings) {
			rawFloats.add(fw.getBytes(VF.createLiteral(f)));
		}

		SortedListInSections.rewrite(rawFloats.iterator(), newFile2);

		SortedList<Value> mappedStringBuffers = SortedListInSections.readinValues(newFile2, XSD.FLOAT);
		int i = 0;
		for (Float string : strings) {
			long pos = mappedStringBuffers.positionOf(VF.createLiteral(string));
			assertEquals(i + ":" + string, i, pos);
			i++;
		}

		testSimpleIterator(strings, mappedStringBuffers);
		testAdvancingIterator(strings, mappedStringBuffers);

		long positionOf = mappedStringBuffers.positionOf(VF.createLiteral(-404f));
		assertEquals(WriteOnce.NOT_FOUND, positionOf);
	}

	private void testAdvancingIterator(List<Float> strings, SortedList<Value> mappedStringBuffers) throws IOException {
		long pos = 0;
		int i = 0;
		IterateInSortedOrder<Value> iter2 = mappedStringBuffers.iterator();
		for (Float string : strings) {
			Value fv = VF.createLiteral(string);
			iter2.advanceNear(fv);
			assertTrue(i + ":", iter2.hasNext());
			TPosition<Value> next = iter2.next();
			pos = next.position();
			assertEquals(i + ":" + string, fv, next.t());
			assertEquals(i + ":" + string, i, pos);
			i++;
		}
	}

	private void testSimpleIterator(List<Float> strings, SortedList<Value> mappedStringBuffers) throws IOException {
		int i = 0;
		long pos = 0;
		Iterator<TPosition<Value>> iter = mappedStringBuffers.iterator();
		for (Float string : strings) {
			assertTrue(i + ":", iter.hasNext());
			Value fv = VF.createLiteral(string);
			TPosition<Value> next = iter.next();
			pos = next.position();
			assertEquals(i + ":" + string, fv, next.t());
			assertEquals(i + ":" + string, i, pos);
			i++;
		}
	}

}
