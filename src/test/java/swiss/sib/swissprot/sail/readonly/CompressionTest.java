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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompressionTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void compressLz4() throws IOException {
		for (int i = 1; i < 10; i++) {
			byte[] in = new byte[i * 1024 * 1024];
			for (int j = 0; j < in.length; j++) {
				in[j] = (byte) j;
			}
			actualTest(Compression.LZ4, in, 99);
			actualTest(Compression.LZ4, in, 1024);
		}

		for (int i = 1; i < 5 * 1024 * 1024; i += 11111) {
			byte[] in = new byte[i];
			actualTest(Compression.LZ4, in, 1024);
		}
	}

	@Test
	public void compressZstd() throws IOException {
		byte[] in = new byte[8 * 1024 * 1024];
		actualTest(Compression.ZSTD, in, 100);
		actualTest(Compression.ZSTD, in, 1024);
	}

	private void actualTest(Compression c, byte[] in, int segment) throws IOException {

		File f = temp.newFile("test" + c.extension());
		compressInProcess(c, in, segment, f);
		testDecompress(c, in, f);
		assertTrue(f.delete());
		compressOutOfProcess(c, in, segment, f);
		testDecompress(c, in, f);
		assertTrue(f.delete());
	}

	private void compressOutOfProcess(Compression c, byte[] in, int segment, File f)
			throws FileNotFoundException, IOException {
		ProcessBuilder epb = c.compressInExternalProcessBuilder(f);
		epb.redirectInput(Redirect.PIPE);
		Process start = epb.start();
		try (OutputStream compress = start.getOutputStream()) {
			int i = 0;

			for (; i < in.length - segment; i += segment) {
				compress.write(in, i, segment);
			}
			if (i != in.length) {
				compress.write(in, i, in.length - i);
			}
		}
		try {
			assertEquals(0, start.waitFor());
		} catch (InterruptedException e) {
			fail();
		}
	}

	private void testDecompress(Compression c, byte[] in, File f) throws IOException, FileNotFoundException {
		byte[] out;
		try (InputStream is = c.decompress(f)) {
			out = is.readAllBytes();
		}
		assertTrue(Arrays.equals(in, out));
		Process p = c.decompressInExternalProcess(f);
		try (InputStream is = p.getInputStream()) {
			out = is.readAllBytes();
		}
		try {
			assertEquals(0, p.waitFor());
		} catch (InterruptedException e) {
			fail();
		}
		assertTrue(Arrays.equals(in, out));
	}

	private void compressInProcess(Compression c, byte[] in, int segment, File f)
			throws IOException, FileNotFoundException {
		try (OutputStream compress = c.compress(f)) {
			int i = 0;

			for (; i < in.length - segment; i += segment) {
				compress.write(in, i, segment);
			}
			if (i != in.length) {
				compress.write(in, i, in.length - i);
			}
		}
	}
}
