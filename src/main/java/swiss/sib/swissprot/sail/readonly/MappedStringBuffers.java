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

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MappedStringBuffers implements AutoCloseable {
	private static final int MAX_STRING_SIZE = 128 * 1024;

	private static final long PAGE_SIZE = 2 * 1024; // Lines/elements

	private final List<Section> mappedBuffers;

	private final RandomAccessFile raf;

	private File fileWithNewLineSeppartedStrings;

	public MappedStringBuffers(File fileWithNewLineSeppartedStrings) throws IOException {
		raf = new RandomAccessFile(fileWithNewLineSeppartedStrings, "r");

		FileChannel fc = raf.getChannel();
		List<Section> tempMappedBuffers = new ArrayList<>();

		makeMemoryMappedSections(fileWithNewLineSeppartedStrings, fc, tempMappedBuffers);
		mappedBuffers = Collections.unmodifiableList(tempMappedBuffers);
	}

	private void makeMemoryMappedSections(File fileWithNewLineSeppartedStrings, FileChannel fc,
			List<Section> tempMappedBuffers) throws IOException, FileNotFoundException {
		this.fileWithNewLineSeppartedStrings = fileWithNewLineSeppartedStrings;
		String first = null;
		long fppointer = 0;
		int bpointer = 0;
		long lineCount = 0;
		byte[] buffer = new byte[MAX_STRING_SIZE];
		try (FileInputStream fis = new FileInputStream(fileWithNewLineSeppartedStrings);
				BufferedInputStream bis = new BufferedInputStream(fis)) {
			int read = -1;
			long firstPos = 0;
			long startPos = 0;
			while ((read = bis.read()) != -1) {
				if (read == '\n') {
					if (lineCount % PAGE_SIZE == 0) {
						if (first != null) {
							tempMappedBuffers.add(new Section(firstPos, startPos, first,
									fc.map(READ_ONLY, startPos, fppointer - startPos)));
						}
						first = new String(buffer, 0, bpointer, StandardCharsets.UTF_8);
						firstPos = fppointer - bpointer;
						startPos = fppointer;
					}
					bpointer = 0;
					lineCount++;
				} else {
					buffer[bpointer++] = (byte) read;
				}
				fppointer++;
			}
			if (first != null) {
				tempMappedBuffers
						.add(new Section(firstPos, startPos, first, fc.map(READ_ONLY, startPos, fppointer - startPos)));
			}
		}
	}

	public long positionOf(String element) throws IOException {
		Section searchFor = new Section(WriteOnce.NOT_FOUND, WriteOnce.NOT_FOUND, element, null);
		int binarySearch = Collections.binarySearch(mappedBuffers, searchFor, this::compareSectionAsRange);
		if (binarySearch >= 0)
			return mappedBuffers.get(binarySearch).firstPos;
		else {
			int sectionId = -(binarySearch + 2);
			Section presentIn = mappedBuffers.get(sectionId);
			return findByScan(element, presentIn);
		}
	}

	// TODO replace by a smart binary search.
	private long findByScan(String element, Section presentIn) {
		byte[] elAsBytes = element.getBytes(StandardCharsets.UTF_8);
		byte[] buffer = new byte[elAsBytes.length];
		for (int i = 0; i < presentIn.rest.limit(); i = i++) {
			byte char1 = presentIn.rest.get(i);
			if (char1 == '\n') {
				i++;
			}
			presentIn.rest.get(i, buffer);
			if (Arrays.equals(buffer, elAsBytes)) {
				return presentIn.startPos + i;
			}
			i = fastForwardToNextLine(presentIn.rest, i);
		}
		return -404;
	}

	private int fastForwardToNextLine(ByteBuffer rest, int i) {
		for (int a = i; a < rest.limit(); a++) {
			if (rest.get(a) == '\n') {
				return a;
			}
		}
		return rest.limit();
	}

	public static final class StringPositionIterator implements AutoCloseable {
		private final long size;
		private final FileInputStream fis;
		private final BufferedInputStream bis;
		private long at = 0;

		public StringPositionIterator(File fileWithNewLineSeppartedStrings) throws IOException {
			super();
			this.size = Files.size(fileWithNewLineSeppartedStrings.toPath());
			this.fis = new FileInputStream(fileWithNewLineSeppartedStrings);
			this.bis = new BufferedInputStream(fis);
		}

		public boolean hasNext() {
			return at <= size;
		}

		public StringPosition next() throws IOException {
			byte[] buffer = new byte[MAX_STRING_SIZE];
			int read = -1;
			int bpointer = 0;
			while ((read = bis.read()) != -1) {
				if (read == '\n') {
					return new StringPosition(at++ - bpointer, new String(buffer, 0, bpointer, StandardCharsets.UTF_8));
				} else {
					buffer[bpointer++] = (byte) read;
					at++;
				}
			}
			return null;
		}

		@Override
		public void close() throws IOException {
			bis.close();
			fis.close();
		}
	}

	private class Section {
		private final long firstPos;
		private final long startPos;
		private final String first;
		private final ByteBuffer rest;

		private Section(long firstPos, long startPos, String first, ByteBuffer rest) {
			this.firstPos = firstPos;
			this.startPos = startPos;
			this.first = first;
			this.rest = rest;
		}
	}

	private int compareSectionAsRange(Section incol, Section other) {
		return incol.first.compareTo(other.first);
	}

	@Override
	public void close() throws IOException {
		raf.close();
	}

	public long positionOfStringFrom(String toFind, long searchFrom) throws IOException {
		for (Section sec : mappedBuffers) {
			long sectionEnd = sec.startPos + sec.rest.limit();
			if (sec.firstPos <= searchFrom && sectionEnd > searchFrom && sec.first.equals(toFind))
				return sec.firstPos;
			if (sec.startPos <= searchFrom && sectionEnd > searchFrom) {
				long potential = findByScan(toFind, sec);
				if (potential != -404)
					return potential;
				else
					// move to the nextSection
					searchFrom = sectionEnd + 1;
			}
		}
		return -404;
	};

	public StringPositionIterator iterator() throws IOException {
		return new StringPositionIterator(fileWithNewLineSeppartedStrings);
	}

	public final static class StringPosition {
		private final long position;
		private final String string;

		public StringPosition(long position, String string) {
			super();
			this.position = position;
			this.string = string;
		}

		public long getPosition() {
			return position;
		}

		public String getString() {
			return string;
		}
	}
}
