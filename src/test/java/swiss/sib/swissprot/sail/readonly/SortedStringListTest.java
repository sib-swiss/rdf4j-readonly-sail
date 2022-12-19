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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;
import swiss.sib.swissprot.sail.readonly.datastructures.list.IterateInSortedOrder;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedListInSections;

public class SortedStringListTest {
	private static final int TEST_STRING_LENGTH = 1024;
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void simpleTest() throws IOException {

		List<String> strings = new ArrayList<>();
		for (int c = 'a'; c <= 'z'; c++) {
			char[] chars = new char[TEST_STRING_LENGTH];
			Arrays.fill(chars, (char) c);
			strings.add(new String(chars));
		}
		testStrings(strings);

	}

	@Test
	public void biggerTest() throws IOException {
		int length = 10;
		Random random = new Random();
		List<String> strings = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			String generatedString = random.ints('a', 'z' + 1)
					.limit(length)
					.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
					.toString();
			strings.add(generatedString);
		}

		List<String> sorted = strings.stream().distinct().sorted((a, b) -> a.compareTo(b)).collect(Collectors.toList());
		testStrings(sorted);
	}

	@Test
	public void biggestTest() throws IOException {

		Random random = new Random();
		List<String> strings = new ArrayList<>();
		for (int i = 0; i < 100_000; i++) {
			int length = random.nextInt(2, 2000);
			String generatedString = random.ints('a', 'z' + 1)
					.limit(length)
					.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
					.toString();
			strings.add(generatedString);
		}

		List<String> sorted = strings.stream().distinct().sorted((a, b) -> a.compareTo(b)).collect(Collectors.toList());
		testStrings(sorted);
	}

	private void testStrings(List<String> strings) throws IOException, FileNotFoundException {
		File newFile2 = temp.newFile();

		SortedListInSections.rewrite(strings.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).iterator(),
				newFile2);

		SortedList<String> mappedStringBuffers = SortedListInSections.readinStrings(newFile2);
		int i = 0;
		for (String string : strings) {
			long pos = mappedStringBuffers.positionOf(string);
			assertEquals(i + ":" + string, i, pos);
			i++;
		}

		testSimpleIterator(strings, mappedStringBuffers);
		testAdvancingIterator(strings, mappedStringBuffers);

		long positionOf = mappedStringBuffers.positionOf("NOT_PRESENT");
		assertEquals(WriteOnce.NOT_FOUND, positionOf);
		positionOf = mappedStringBuffers.positionOf("azzzzzz");
		assertEquals(WriteOnce.NOT_FOUND, positionOf);

	}

	private void testAdvancingIterator(List<String> strings, SortedList<String> mappedStringBuffers)
			throws IOException {
		long pos = 0;
		int i = 0;
		IterateInSortedOrder<String> iter2 = mappedStringBuffers.iterator();
		for (String string : strings) {
			iter2.advanceNear(string);
			assertTrue(i + ":", iter2.hasNext());
			TPosition<String> next = iter2.next();
			pos = next.position();
			assertEquals(i + ":" + string.charAt(0), string, next.t());
			assertEquals(i + ":" + string.charAt(0), i, pos);
			i++;
		}
	}

	private void testSimpleIterator(List<String> strings, SortedList<String> mappedStringBuffers) throws IOException {
		int i = 0;
		long pos = 0;
		Iterator<TPosition<String>> iter = mappedStringBuffers.iterator();
		for (String string : strings) {
			assertTrue(i + ":", iter.hasNext());
			TPosition<String> next = iter.next();
			pos = next.position();
			assertEquals(i + ":" + string.charAt(0), string, next.t());
			assertEquals(i + ":" + string.charAt(0), i, pos);
			i++;
		}
	}

}
