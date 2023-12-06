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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.tukaani.xz.FilterOptions;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import net.jpountz.lz4.LZ4FrameOutputStream.FLG;

public enum Compression {

	LZ4(".lz4") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new BufferedInputStream(new LZ4FrameInputStream(new BufferedInputStream(new FileInputStream(f))));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("lz4", "-qdcf", f.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("lz4", "-qzcf");
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder(File to) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("lz4", "-qzf", "-", to.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public OutputStream compress(File f) throws FileNotFoundException, IOException {
			if (Runtime.getRuntime().availableProcessors() > 8) {
				return new MultiFrameCompressor(new BufferedOutputStream(new FileOutputStream(f)));
			} else {
				return new LZ4FrameOutputStream(new FileOutputStream(f), LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB);
			}
		}

		/**
		 * This works because frames are allowed to be concatenated. We collect 4
		 * Megabytes of input and compress this into one frame with known inputsize;
		 *
		 * Compression happens in the common fork join pool. But does not depend on this
		 * to make progress. If there are more than 16 buffered sections in the
		 * compression queue we always write out the first one. Either waiting for
		 * compression task to finish or to do it in it's own thread if the submitted
		 * task was not picked up for compression in the common pool.
		 */
		static final class MultiFrameCompressor extends OutputStream {
			private static final int MAX_QUEUED_COMPRESSION_TASKS = 16;
			private static final int BUFFER_SIZE = 4 * 1024 * 1024;
			private byte[] buffer = new byte[BUFFER_SIZE];
			private final Deque<CompressionTask<byte[]>> cts = new ArrayDeque<>();
			private int offset = 0;
			private final OutputStream wrapped;

			public MultiFrameCompressor(OutputStream wrapped) {
				super();
				this.wrapped = wrapped;
			}

			@Override
			public void write(int arg0) throws IOException {
				buffer[offset++] = (byte) arg0;
				if (offset == BUFFER_SIZE) {
					sumbitCompressionTask();
				}
				tryToWriteFirst();
			}

			private void sumbitCompressionTask() throws IOException {
				CompressionTask<byte[]> ct = new Lz4FrameCompressionTask(buffer);
				cts.add(ct);
				buffer = new byte[BUFFER_SIZE];
				offset = 0;
				execs.submit(ct::attemptCompress);
				if (cts.size() > MAX_QUEUED_COMPRESSION_TASKS) {
					CompressionTask<byte[]> first = cts.pollFirst();
					CompressionTask.writeCompressionTask(first, wrapped);
				}
			}

			private void tryToWriteFirst() throws IOException {
				CompressionTask<byte[]> first = cts.peekFirst();
				while (first != null) {
					if (first.e != null) {
						throw first.e;
					} else if (first.output != null) {
						first = cts.pollFirst();
						CompressionTask.writeCompressionTask(first, wrapped);
						// Allows us to check if any other element is ready
						// be written out
						first = cts.peekFirst();
					} else {
						// There is nothing to throw or write right now.
						// so return and do something else.
						return;
					}
				}
			}

			@Override
			public void close() throws IOException {
				if (offset > 0) {
					CompressionTask<byte[]> ct = new Lz4FrameCompressionTask(Arrays.copyOf(buffer, offset));
					cts.add(ct);
					offset = 0;
				}
				while (!cts.isEmpty()) {
					CompressionTask<byte[]> first = cts.pollFirst();
					CompressionTask.writeCompressionTask(first, wrapped);
				}
				wrapped.close();
			}

			@Override
			public void write(byte[] toWrite, int off, int len) throws IOException {
				int writeAble = buffer.length - this.offset;
				if (len < writeAble) {
					System.arraycopy(toWrite, off, buffer, this.offset, len);
					this.offset += len;
					tryToWriteFirst();
				} else if (len > 0) {
					// Fill the current buffer
					System.arraycopy(toWrite, off, buffer, this.offset, writeAble);
					this.offset += writeAble;
					sumbitCompressionTask();
					// write to the next buffer object
					write(toWrite, off + writeAble, len - writeAble);
				}
			}

		}

	},
	GZIP(".gz") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new GZIPInputStream(new FileInputStream(f));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("gunzip", "-c");
			pb.redirectInput(f);
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public OutputStream compress(File f) throws FileNotFoundException, IOException {
			return new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("gzip", "-c");
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

	},
	XZ(".xz") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new XZInputStream(new FileInputStream(f));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("xz", "-cdf", f.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public OutputStream compress(File f) throws FileNotFoundException, IOException {
			return new XZOutputStream(new BufferedOutputStream(new FileOutputStream(f)), new FilterOptions[] {});
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("xz", "-dc");
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	ZSTD(".zstd") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new ZstdInputStream(new FileInputStream(f));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("zstd", "-cqdk", "-T0", f.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public OutputStream compress(File f) throws FileNotFoundException, IOException {
			return new ZstdOutputStream(new BufferedOutputStream(new FileOutputStream(f)));
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("zstd", "-zcq");
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	NONE("") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new FileInputStream(f);
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("cat");
			pb.redirectInput(f);
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public OutputStream compress(File f) throws FileNotFoundException, IOException {
			return new BufferedOutputStream(new FileOutputStream(f));
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("cat");
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	BZIP2(".bz2") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("bunzip2");
			pb.redirectInput(f);
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}

		@Override
		public OutputStream compress(File f) throws FileNotFoundException, IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("bzip2", "-c");
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	};

	private static final ExecutorService execs = Executors
			.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

	private final String extension;

	private Compression(String extension) {
		this.extension = extension;
	}

	public String extension() {
		return extension;
	}

	public static Compression fromExtension(String extension) {
		for (Compression c : Compression.values()) {
			if (c.extension.equals(extension)) {
				return c;
			}
		}
		return Compression.NONE;
	}

	public abstract InputStream decompress(File f) throws FileNotFoundException, IOException;

	public abstract OutputStream compress(File f) throws FileNotFoundException, IOException;

	public final Process decompressInExternalProcess(File f) throws FileNotFoundException, IOException {
		return decompressInExternalProcessBuilder(f).start();
	}

	public abstract ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException;

	public final Process decompressInExternalProcess(File f, File to) throws FileNotFoundException, IOException {
		ProcessBuilder pb = decompressInExternalProcessBuilder(f);
		pb.redirectOutput(to);
		return pb.start();
	}

	public abstract ProcessBuilder compressInExternalProcessBuilder() throws FileNotFoundException, IOException;

	public ProcessBuilder compressInExternalProcessBuilder(File to) throws FileNotFoundException, IOException {
		ProcessBuilder pb = compressInExternalProcessBuilder();
		pb.redirectOutput(to);
		return pb;

	}

	public final Process compressInExternalProcess(File from, File to) throws FileNotFoundException, IOException {
		ProcessBuilder pb = compressInExternalProcessBuilder(from);
		pb.redirectOutput(to);
		return pb.start();

	}

	public static String removeExtension(String name) {
		for (Compression c : values()) {
			if (!c.extension.isEmpty() && name.endsWith(c.extension())) {
				return name.substring(0, name.length() - c.extension.length());
			}
		}
		return name;
	}

	public static abstract class CompressionTask<T> {
		protected T input;
		private volatile boolean inprogres;
		protected volatile byte[] output;
		private volatile IOException e;

		public CompressionTask(T input) {
			super();
			this.input = input;
		}

		public final void attemptCompress() {
			if (output == null && inprogres == false) {
				compress();
			}
		}

		private void compress() {
			inprogres = true;
			try {
				output = compressAction(input);
			} catch (UncheckedIOException e) {
				this.e = e.getCause();
			}
			input = null;
			inprogres = false;
		}

		protected abstract byte[] compressAction(T input);

		public static void writeCompressionTask(CompressionTask<?> ct, OutputStream wrapped) throws IOException {

			if (ct.inprogres) {
				while (ct.output == null && ct.e == null) {
					Thread.onSpinWait();
				}
			}
			if (ct.e != null) {
				throw ct.e;
			}
			if (ct.output != null) {
				wrapped.write(ct.output);
			} else {
				ct.compress();
				if (ct.e != null) {
					throw ct.e;
				}
				wrapped.write(ct.output);
			}
		}
	}

	private static final class Lz4FrameCompressionTask extends CompressionTask<byte[]> {

		public Lz4FrameCompressionTask(byte[] input) {
			super(input);
		}

		@Override
		protected byte[] compressAction(byte[] input) {
			try {
				// if input is null we have either already run, so we can return the output that
				// we have
				// already stored.
				if (input == null)
					return output;
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (LZ4FrameOutputStream lz4FrameOutputStream = new LZ4FrameOutputStream(baos, BLOCKSIZE.SIZE_4MB,
						input.length, FLG.Bits.BLOCK_INDEPENDENCE, FLG.Bits.CONTENT_SIZE)) {
					lz4FrameOutputStream.write(input);
				}
				baos.close();
				return baos.toByteArray();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}
}
