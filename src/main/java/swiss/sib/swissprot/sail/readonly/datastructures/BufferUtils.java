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
package swiss.sib.swissprot.sail.readonly.datastructures;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BufferUtils {

	private BufferUtils() {

	}

	/*
	 * This makes a buffer size of 960 MB.
	 */
	private static final int BUCKET_SIZE = Integer.MAX_VALUE >>> 4;

	/**
	 * Open a byte buffer against a memory mapped file. The memory mapped files do need addressable memory space.
	 * However, this memory is not in the heap and may actually not be claimed. This means that for the large datasets a
	 * 32bit machine is not good enough.
	 *
	 * @param source the file containing the mapped accessions or sorted data.
	 * @return a ByteBuffer in which can be wrapped in an int or long buffer.
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static ByteBuffer[] openByteBuffer(final Path source) throws FileNotFoundException, IOException {
		if (!Files.exists(source))
			return new ByteBuffer[0];
		try (final RandomAccessFile randomAccessFile = new RandomAccessFile(source.toFile(), "r");
				final FileChannel channel = randomAccessFile.getChannel()) {

			int numberOfBuffers = ((int) (randomAccessFile.length() / BUCKET_SIZE)) + 1;
			ByteBuffer[] buffers = new ByteBuffer[numberOfBuffers];

			long channelSize = channel.size();
			long totalCapacity = 0;
			for (int i = 0; i < numberOfBuffers; i++) {
				int bufferSize = BUCKET_SIZE;
				if (i + 1 == numberOfBuffers) {
					// Every buffer (bucket) is the maximum size except the last one.
					bufferSize = (int) (channelSize - totalCapacity);
				}
				assert bufferSize >= 0;
				assert bufferSize <= channelSize;
				// Have to work in long space for calculating the offset
				buffers[i] = channel.map(FileChannel.MapMode.READ_ONLY, totalCapacity, bufferSize);
				totalCapacity = totalCapacity + buffers[i].capacity();

				assert totalCapacity <= channelSize;
			}
			return buffers;
		}

	}

	public static int getIntAtIndexInByteBuffers(final long index, final ByteBuffer[] buffers) {
		int local = (int) (index % BUCKET_SIZE);
		int i = (int) (index / BUCKET_SIZE);

		ByteBuffer buffer = buffers[i];
		int limit = buffer.limit();
		if (local < limit) {
			if (local + Integer.BYTES > limit) {
				byte[] temp = new byte[Integer.BYTES];
				buffer.get((int) local, temp, 0, (int) (limit - local));
				int offset = (int) (limit - local);
				buffers[i + 1].get(0, temp, offset, Integer.BYTES - offset);
				return ByteBuffer.wrap(temp).getInt();
			} else {
				return buffer.getInt((int) local);
			}
		}
		throw new RuntimeException("Index not in buffers range:" + index);
	}

	public static long getLongAtIndexInByteBuffers(final long index, final ByteBuffer[] buffers) {
		int local = (int) (index % BUCKET_SIZE);
		int i = (int) (index / BUCKET_SIZE);

		ByteBuffer buffer = buffers[i];
		int limit = buffer.limit();
		if (local < limit) {
			if (local + Long.BYTES > limit) {
				byte[] temp = new byte[Long.BYTES];
				buffer.get((int) local, temp, 0, (int) (limit - local));
				int offset = (int) (limit - local);
				buffers[i + 1].get(0, temp, offset, Long.BYTES - offset);
				return ByteBuffer.wrap(temp).getLong();
			} else {
				return buffer.getLong((int) local);
			}
		}
		throw new RuntimeException("Index not in buffers range:" + index);
	}

	public static byte[] getByteArrayAtIndexInByteBuffers(final long index, final int length,
			final ByteBuffer[] buffers) {
		int local = (int) (index % BUCKET_SIZE);
		int i = (int) (index / BUCKET_SIZE);

		ByteBuffer buffer = buffers[i];
		int limit = buffer.limit();
		if (local < limit) {
			if (local + length > limit) {
				byte[] temp = new byte[length];
				buffer.get((int) local, temp, 0, (int) (limit - local));
				int offset = (int) (limit - local);
				buffers[i + 1].get(0, temp, offset, length - offset);
				return temp;
			} else {
				byte[] temp = new byte[length];
				buffer.get((int) local, temp);
				return temp;
			}
		}
		throw new RuntimeException("Index not in buffers range:" + index);
	}

	public static ByteBuffer getByteBufferAtIndexInByteBuffers(final long index, final int length,
			final ByteBuffer[] buffers) {
		int local = (int) (index % BUCKET_SIZE);
		int i = (int) (index / BUCKET_SIZE);

		ByteBuffer buffer = buffers[i];
		int limit = buffer.limit();
		if (local < limit) {
			if (local + length > limit) {
				byte[] temp = new byte[length];
				buffer.get(local, temp, 0, limit - local);
				int offset = limit - local;
				buffers[i + 1].get(0, temp, offset, length - offset);
				return ByteBuffer.wrap(temp);
			} else {
				return buffer.slice(local, length);
			}
		}

		throw new RuntimeException("Index not in buffers range:" + index);
	}

	public static byte getByteAtIndexInByteBuffers(final long index, final ByteBuffer[] buffers) {
		long local = index;
		for (ByteBuffer buffer : buffers) {
			int limit = BufferUtils.BUCKET_SIZE;
			if (local < limit) {
				return buffer.get((int) local);
			} else
				local = local - limit;
		}
		throw new RuntimeException("DocNumber not in buffers range:" + index);
	}

	public static byte[] readByteArrayAt(long position, ByteBuffer[] buffers) {

		int bufferToStartLookInto = 0;
		int contentLength = 0;
		int positionInCurrentBufferAsInt = 0;
		{
			long positionInCurrentBuffer = position;
			for (; bufferToStartLookInto < buffers.length; bufferToStartLookInto++) {
				int limit = BUCKET_SIZE;
				if (positionInCurrentBuffer < limit) {
					break;
				} else
					positionInCurrentBuffer = positionInCurrentBuffer - limit;
			}
			assert positionInCurrentBuffer <= Integer.MAX_VALUE && positionInCurrentBuffer >= 0;
			positionInCurrentBufferAsInt = (int) positionInCurrentBuffer;
		}
		try {
			contentLength = buffers[bufferToStartLookInto].getInt(positionInCurrentBufferAsInt);
			positionInCurrentBufferAsInt = positionInCurrentBufferAsInt + Integer.BYTES;
		} catch (IndexOutOfBoundsException e) {
			// Deals with the case of a position being written across the boundary.
			int j = 0;
			for (int i = positionInCurrentBufferAsInt; i < buffers[bufferToStartLookInto].capacity(); i++, j++)
				contentLength = contentLength << 8 | ((buffers[bufferToStartLookInto].get(i)) & 0xff);
			bufferToStartLookInto++;
			positionInCurrentBufferAsInt = 0;
			for (; j < Integer.BYTES; j++, positionInCurrentBufferAsInt++)
				contentLength = contentLength << 8
						| ((buffers[bufferToStartLookInto].get(positionInCurrentBufferAsInt)) & 0xff);
		}

		if (contentLength + positionInCurrentBufferAsInt < buffers[bufferToStartLookInto].capacity()) {
			ByteBuffer duplicate = buffers[bufferToStartLookInto].duplicate();
			duplicate.position(positionInCurrentBufferAsInt);
			byte[] data = new byte[contentLength];
			duplicate.get(data);
			return data;
		} else
			return readDataIntoByteArray(buffers, bufferToStartLookInto, contentLength, positionInCurrentBufferAsInt);
	}

	public static byte[] readDataIntoByteArray(ByteBuffer[] buffers, int bufferToStartLookInto, int contentLength,
			int positionInCurrentBufferAsInt) {
		byte[] data = new byte[contentLength];
		for (int i = 0; i < data.length; i++)
			try {
				data[i] = buffers[bufferToStartLookInto].get(positionInCurrentBufferAsInt);
				positionInCurrentBufferAsInt++;
			} catch (IndexOutOfBoundsException e) {
				// Deals with the case of an entry being written across the boundary.
				bufferToStartLookInto++;
				positionInCurrentBufferAsInt = 0;
				data[i] = buffers[bufferToStartLookInto].get(positionInCurrentBufferAsInt);
				positionInCurrentBufferAsInt++;
			}
		return data;
	}

	public static ByteBuffer readByteBufferAt(long position, ByteBuffer[] buffers) {

		int bufferToStartLookInto = 0;
		int contentLength = 0;
		int positionInCurrentBufferAsInt = 0;
		{
			long positionInCurrentBuffer = position;
			for (; bufferToStartLookInto < buffers.length; bufferToStartLookInto++) {
				int limit = BUCKET_SIZE;
				if (positionInCurrentBuffer < limit) {
					break;
				} else
					positionInCurrentBuffer = positionInCurrentBuffer - limit;
			}
			assert positionInCurrentBuffer <= Integer.MAX_VALUE && positionInCurrentBuffer >= 0;
			positionInCurrentBufferAsInt = (int) positionInCurrentBuffer;
		}
		try {
			contentLength = buffers[bufferToStartLookInto].getInt(positionInCurrentBufferAsInt);
			positionInCurrentBufferAsInt = positionInCurrentBufferAsInt + Integer.BYTES;
		} catch (IndexOutOfBoundsException e) {
			// Deals with the case of a position being written across the boundary.
			int j = 0;
			for (int i = positionInCurrentBufferAsInt; i < buffers[bufferToStartLookInto].capacity(); i++, j++)
				contentLength = contentLength << 8 | ((buffers[bufferToStartLookInto].get(i)) & 0xff);
			bufferToStartLookInto++;
			positionInCurrentBufferAsInt = 0;
			for (; j < Integer.BYTES; j++, positionInCurrentBufferAsInt++)
				contentLength = contentLength << 8
						| ((buffers[bufferToStartLookInto].get(positionInCurrentBufferAsInt)) & 0xff);
		}

		if (contentLength + positionInCurrentBufferAsInt < buffers[bufferToStartLookInto].capacity()) {
			ByteBuffer duplicate = buffers[bufferToStartLookInto].asReadOnlyBuffer();
			duplicate.position(positionInCurrentBufferAsInt);
			duplicate.limit(positionInCurrentBufferAsInt + contentLength);
			duplicate.mark();
			return duplicate;
		} else
			return ByteBuffer.wrap(
					readDataIntoByteArray(buffers, bufferToStartLookInto, contentLength, positionInCurrentBufferAsInt));
	}

	public static ByteBuffer readByteBufferAtOfLength(long position, ByteBuffer[] buffers, int contentLength) {

		int bufferToStartLookInto = 0;
		int positionInCurrentBufferAsInt = 0;
		{
			long positionInCurrentBuffer = position;
			for (; bufferToStartLookInto < buffers.length; bufferToStartLookInto++) {
				int limit = BUCKET_SIZE;
				if (positionInCurrentBuffer < limit) {
					break;
				} else
					positionInCurrentBuffer = positionInCurrentBuffer - limit;
			}
			assert positionInCurrentBuffer <= Integer.MAX_VALUE && positionInCurrentBuffer >= 0;
			positionInCurrentBufferAsInt = (int) positionInCurrentBuffer;
		}

		if (contentLength + positionInCurrentBufferAsInt <= buffers[bufferToStartLookInto].capacity()) {
			ByteBuffer duplicate = buffers[bufferToStartLookInto].asReadOnlyBuffer();
			duplicate.limit(positionInCurrentBufferAsInt + contentLength);
			duplicate.position(positionInCurrentBufferAsInt);
			duplicate.mark();
			return duplicate;
		} else
			return ByteBuffer.wrap(
					readDataIntoByteArray(buffers, bufferToStartLookInto, contentLength, positionInCurrentBufferAsInt));
	}
}
