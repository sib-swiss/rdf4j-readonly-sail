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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MappedStringBufferTest {
	private static final int TEST_STRING_LENGTH = 1024;
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void simpleTest() throws IOException {
		File newFile = temp.newFile();
		try (FileWriter writer = new FileWriter(newFile, StandardCharsets.UTF_8)) {
			for (int c = 'a'; c <= 'z'; c++) {
				char[] chars = new char[TEST_STRING_LENGTH];
				Arrays.fill(chars, (char) c);
				writer.write(new String(chars));
				writer.write('\n');
			}
		}

		try (MappedStringBuffers mappedStringBuffers = new MappedStringBuffers(newFile)) {
			int expectedPos = 0;
			for (int c = 'a'; c <= 'z'; c++) {
				char[] chars = new char[TEST_STRING_LENGTH];
				Arrays.fill(chars, (char) c);
				long pos = mappedStringBuffers.positionOf(new String(chars));
				assertEquals(expectedPos, pos);
				expectedPos += TEST_STRING_LENGTH + 1;
			}

			long pos = 0;
			expectedPos = 0;
			for (int c = 'a'; c <= 'z'; c++) {
				char[] chars = new char[TEST_STRING_LENGTH];
				Arrays.fill(chars, (char) c);
				pos = mappedStringBuffers.positionOfStringFrom(new String(chars), pos);
				assertEquals(":" + (char) c, expectedPos, pos);
				expectedPos += TEST_STRING_LENGTH + 1;
				pos = expectedPos;
			}

			pos = 0;
			expectedPos = 0;
			for (int c = 'a'; c <= 'z'; c += 2) {
				char[] chars = new char[TEST_STRING_LENGTH];
				Arrays.fill(chars, (char) c);
				pos = mappedStringBuffers.positionOfStringFrom(new String(chars), pos);
				assertEquals(":" + (char) c, expectedPos, pos);
				expectedPos += TEST_STRING_LENGTH + 1;
				expectedPos += TEST_STRING_LENGTH + 1;
				pos = expectedPos;
			}
		}
	}

	@Test
	public void biggerTest() throws IOException {
		File newFile = temp.newFile();
		int length = 1014;
		Random random = new Random();
		List<String> strings = new ArrayList<>();
		for (int i = 0; i < 10_000; i++) {
			String generatedString = random.ints('a', 'z' + 1)
					.limit(length)
					.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
					.toString();
			strings.add(generatedString);
		}
		strings.sort((a, b) -> a.compareTo(b));
		try (FileWriter writer = new FileWriter(newFile, StandardCharsets.UTF_8)) {
			for (String string : strings) {
				writer.write(string);
				writer.write('\n');
			}
		}

		try (MappedStringBuffers mappedStringBuffers = new MappedStringBuffers(newFile)) {
			int expectedPos = 0;
			for (String string : strings) {
				long pos = mappedStringBuffers.positionOf(string);
				assertEquals(expectedPos, pos);
				expectedPos += length + 1;
			}

			long pos = 0;
			expectedPos = 0;
			for (String string : strings) {
				pos = mappedStringBuffers.positionOfStringFrom(string, pos);
				assertEquals(expectedPos, pos);
				expectedPos += length + 1;
				pos = expectedPos;
			}

			pos = 0;
			expectedPos = 0;
			for (Iterator<String> iterator = strings.iterator(); iterator.hasNext();) {
				String string = iterator.next();
				pos = mappedStringBuffers.positionOfStringFrom(new String(string), pos);
				assertEquals(expectedPos, pos);
				expectedPos += length + 1;
				expectedPos += length + 1;
				pos = expectedPos;
				iterator.next();
			}
		}
	}
}
