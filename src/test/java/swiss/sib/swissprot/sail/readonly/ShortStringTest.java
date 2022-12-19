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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;

import org.junit.Test;

import swiss.sib.swissprot.sail.readonly.values.ShortString;

public class ShortStringTest {
	@Test
	public void testAAA() {
		for (int i = 1; i < 9; i++) {
			char[] aaas = new char[i];
			Arrays.fill(aaas, 'a');
			String aaa = new String(aaas);
			final ShortString shortString = new ShortString(aaa.getBytes(StandardCharsets.UTF_8));
			assertEquals(aaa.length(), shortString.length());
			for (int j = 0; j < aaa.length(); j++)
				assertEquals('a', shortString.charAt(j));
		}
	}

	@Test
	public void testCant() {
		assertFalse(ShortString.encodable("ABCDEFGHIJ"));
		assertFalse(ShortString.encodable("ABCDEFGHIJK"));
		assertFalse(ShortString.encodable("杭州市"));
		assertTrue(ShortString.encodable("ABC"));
		assertEquals('A', new ShortString("ABC").charAt(0));
		assertEquals('B', new ShortString("ABC").charAt(1));
		assertEquals('C', new ShortString("ABC").charAt(2));
	}

	@Test
	public void testDate() {
		final LocalDate of = LocalDate.of(1999, 4, 28);
		final LocalDateAsInteger dateAsLong = new LocalDateAsInteger(of);
		assertEquals(of, dateAsLong.get());
		assertEquals(1999, dateAsLong.get().getYear());
	}

	private static class LocalDateAsInteger {
		private final int value;

		public LocalDateAsInteger(LocalDate dt) {
			byte[] v = new byte[Integer.BYTES];
			short year = (short) dt.getYear();
			byte month = (byte) dt.getMonthValue();
			byte day = (byte) dt.getDayOfMonth();
			ByteBuffer buf = ByteBuffer.wrap(v);
			buf.putShort(year);
			buf.put(month);
			buf.put(day);
			this.value = buf.getInt(0);
		}

		public LocalDate get() {
			final ByteBuffer wrap = ByteBuffer.wrap(new byte[Integer.BYTES]);
			wrap.putInt(value);
			short year = wrap.getShort(0);
			byte month = wrap.get(Short.BYTES);
			byte day = wrap.get(Short.BYTES + 1);
			return LocalDate.of(year, month, day);
		}
	}
}
