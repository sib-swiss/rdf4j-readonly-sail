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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.Test;

public class BasicSectionTest {

	@Test
	public void simpleWrite() throws IOException {
		List<byte[]> list = makeList();
		actualTest(list);
	}

	@Test
	public void biggerSection() throws IOException {
		List<byte[]> list = new ArrayList<>();
		for (int i = 'a'; i < 'z'; i++) {
			byte[] bs = new byte[5];
			Arrays.fill(bs, (byte) i);
			list.add(bs);
		}
		actualTest(list);
	}

	@Test
	public void muchBiggerSection() throws IOException {
		List<byte[]> list = new ArrayList<>();
		Random r = new Random();
		for (int i = 0; i < 4000; i++) {
			byte[] bs = new byte[1000];
			for (int j = 0; j < 1000; j++) {
				bs[j] = (byte) (r.nextInt('z' - 'a') + 'a');
			}
			list.add(bs);
		}
		list.sort(Arrays::compare);
		actualTest(list);
	}

	private void actualTest(List<byte[]> list) throws IOException {
		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		try (DataOutputStream dos = new DataOutputStream(boas)) {
			BasicSection.write(list, dos);
		}
		boas.close();
		ByteBuffer[] buffers = new ByteBuffer[] { ByteBuffer.wrap(boas.toByteArray()) };

		BasicSection<byte[]> section = new BasicSection<>(0l, list.get(0), buffers, 0l, Function.identity(),
				Function.identity(), Arrays::compare);
		for (int i = 0; i < list.size(); i++) {
			assertArrayEquals(list.get(i), section.get(i).t());
		}
		try (ByteArrayInputStream in = new ByteArrayInputStream(boas.toByteArray())) {
			BasicSection.<byte[]>read(in, 0, boas.size(), buffers, Function.identity(), Function.identity(),
					Arrays::compare);
		}
		for (int i = 0; i < list.size(); i++) {
			assertEquals(section.findPositionByBinarySearch(list.get(i)), i);
		}
	}

	private List<byte[]> makeList() {
		return List.of(new byte[] { (byte) 'a' });
	}
}
