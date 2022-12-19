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
package swiss.sib.swissprot.sail.readonly.values;

import java.nio.charset.StandardCharsets;

public class ShortString implements CharSequence {
	final long value;
	private static final int BITS_PER_CHAR = 7;
	private static final long ALL_BUT_FIRST_BIT_SET = ~ReadOnlyString.FIRST_BIT_SET;
	private static final int CHARS = Long.bitCount(ALL_BUT_FIRST_BIT_SET) / BITS_PER_CHAR;

	public static final boolean encodable(String string) {
		if (string.length() > CHARS) {
			return false;
		} else {
			final byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
			if (bytes.length != string.length())
				return false;
			for (byte b : bytes) {
				if (b < 0)
					return false;
			}
		}
		return true;
	}

	public ShortString(long value) {
		super();
		this.value = value;
	}

	public ShortString(String s) {
		this(s.getBytes(StandardCharsets.UTF_8));
	}

	public ShortString(byte[] value) {
		super();
		long temp = ReadOnlyString.FIRST_BIT_SET;
		for (int i = 0; i < value.length; i++) {
			long val = value[i];
			assert val >>> 8 != 1;
			int pos = 8 - i;
			final long temp2 = val << (pos * BITS_PER_CHAR);
			temp = temp | temp2;
		}
		this.value = temp;
	}

	@Override
	public int length() {
		long temp = this.value & ALL_BUT_FIRST_BIT_SET;
		for (int i = 8; i >= 0; i--) {
			final long temp2 = temp >>> (i * BITS_PER_CHAR);
			final long temp3 = temp2 & 0b01111111l;
			if (temp3 == 0)
				return 8 - i;
		}
		return CHARS;
	}

	@Override
	public char charAt(int index) {
		long temp = this.value & ALL_BUT_FIRST_BIT_SET;
		final int length = length();
		if (index < 0 || index > length) {
			throw new StringIndexOutOfBoundsException("index" + index + ", length " + length);
		}
		final long val = (temp >>> ((8 - index) * BITS_PER_CHAR)) & 0b01111111;
		return (char) val;
	}

	@Override
	public CharSequence subSequence(int begin, int end) {
		final int length = length();
		if (begin < 0 || begin > end || end > length) {
			throw new StringIndexOutOfBoundsException("begin " + begin + ", end " + end + ", length " + length);
		}

		int subLen = end - begin;
		if (begin == 0 && end == length) {
			return this;
		}
		char[] val = new char[subLen];
		for (int i = 0; begin + end < i; i++)
			val[i] = this.charAt(begin + 1);
		return new String(val);
	}

}
