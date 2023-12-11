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
package swiss.sib.swissprot.sail.readonly.datastructures.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;

public class RawIO {
	private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

	public interface IO {
		public int write(DataOutputStream dos, Value v) throws IOException;

		public default int write(DataOutputStream dos, byte[] content) throws IOException {
			assert !fixedWidth();
			dos.writeInt(content.length);
			dos.write(content);
			return Integer.BYTES + content.length;
		}

		// May read the length first if variable width;
		public default Value read(DataInputStream in) throws IOException {
			return read(readRaw(in));
		}

		// Without the length if variable
		public Value read(byte[] in);

		public Value read(byte[] in, int offset, int length);

		// Without the length;
		public default byte[] getBytes(Value v) {
			return v.stringValue().getBytes(StandardCharsets.UTF_8);
		}

		public default boolean fixedWidth() {
			return false;
		}

		public default byte[] readRaw(DataInputStream dis) throws IOException {
			int length = dis.readInt();
			return readExactlyXBytes(dis, length);
		};
	}

	private static class IRIIO implements IO {

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof IRI;
			return write(dos, getBytes(v));
		}

		@Override
		public IRI read(byte[] content) {
			return SVF.createIRI(new String(content, StandardCharsets.UTF_8));
		}
		
		@Override
		public IRI read(byte[] content, int offset, int length) {
			return SVF.createIRI(new String(content, offset, length, StandardCharsets.UTF_8));
		}
	}

	private static record LangStringIO(String lang) implements IO {

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof Literal;
			return write(dos, getBytes(v));
		}

		@Override
		public Literal read(byte[] in) {
			return SVF.createLiteral(new String(in, StandardCharsets.UTF_8), lang);
		}
		
		@Override
		public Literal read(byte[] content, int offset, int length) {
			return SVF.createLiteral(new String(content, offset, length, StandardCharsets.UTF_8), lang);
		}
	}

	private static class BNodeIO implements IO {

		public int write(DataOutputStream dos, byte[] content) throws IOException {
			assert content.length == Long.BYTES;
			dos.write(content);
			return Long.BYTES;
		}

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof BNode;
			BNode bNode = (BNode) v;
			dos.writeLong(Long.parseLong(bNode.getID()));
			return Long.BYTES;
		}

		@Override
		public BNode read(DataInputStream in) throws IOException {
			return SVF.createBNode(Long.toString(in.readLong()));
		}

		@Override
		public byte[] getBytes(Value v) {
			byte[] content = new byte[Long.BYTES];
			assert v instanceof BNode;
			BNode bNode = (BNode) v;
			long id = Long.parseLong(bNode.getID());
			ByteBuffer.wrap(content).putLong(0, id);
			return content;
		}

		@Override
		public BNode read(byte[] in) {
			return SVF.createBNode(Long.toString(ByteBuffer.wrap(in).getLong(0)));
		}
		
		@Override
		public BNode read(byte[] in, int offset, int length) {
			return SVF.createBNode(Long.toString(ByteBuffer.wrap(in).getLong(offset)));
		}

		@Override
		public boolean fixedWidth() {
			return true;
		}

		public byte[] readRaw(DataInputStream dis) throws IOException {
			return readExactlyXBytes(dis, Long.BYTES);
		}
	}

	private static record DatatypeAsUTF8IO(IRI datatype) implements IO {

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof Literal;
			byte[] content = v.stringValue().getBytes(StandardCharsets.UTF_8);
			return write(dos, content);
		}

		@Override
		public Value read(byte[] in) {
			return SVF.createLiteral(new String(in, StandardCharsets.UTF_8), datatype);
		}
		
		@Override
		public Value read(byte[] in, int offset, int length) {
			return SVF.createLiteral(new String(in, offset, length, StandardCharsets.UTF_8), datatype);
		}
	}

	private static class IntIO implements IO {

		public int write(DataOutputStream dos, byte[] content) throws IOException {
			assert content.length == Integer.BYTES;
			dos.write(content);
			return Integer.BYTES;
		}

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof Literal;
			dos.writeInt(((Literal) v).intValue());
			return Integer.BYTES;
		}

		@Override
		public Literal read(DataInputStream in) throws IOException {
			return SVF.createLiteral(in.readInt());
		}

		@Override
		public byte[] getBytes(Value v) {
			byte[] content = new byte[Integer.BYTES];
			ByteBuffer.wrap(content).putInt(0, ((Literal) v).intValue());
			return content;
		}

		@Override
		public Value read(byte[] in) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getInt(0));
		}
		
		@Override
		public Value read(byte[] in, int offset, int length) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getInt(offset));
		}

		@Override
		public boolean fixedWidth() {
			return true;
		}

		public byte[] readRaw(DataInputStream dis) throws IOException {
			return readExactlyXBytes(dis, Integer.BYTES);
		}

	}

	private static class LongIO implements IO {

		public int write(DataOutputStream dos, byte[] content) throws IOException {
			assert content.length == Long.BYTES;
			dos.write(content);
			return Long.BYTES;
		}

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof Literal;
			dos.writeLong(((Literal) v).longValue());
			return Long.BYTES;
		}

		@Override
		public Literal read(DataInputStream in) throws IOException {
			return SVF.createLiteral(in.readLong());
		}

		@Override
		public byte[] getBytes(Value v) {
			byte[] content = new byte[Long.BYTES];
			ByteBuffer.wrap(content).putLong(0, ((Literal) v).longValue());
			return content;
		}

		@Override
		public Value read(byte[] in) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getLong(0));
		}
		
		@Override
		public Value read(byte[] in, int offset, int length) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getLong(offset));
		}

		@Override
		public boolean fixedWidth() {
			return true;
		}

		public byte[] readRaw(DataInputStream dis) throws IOException {
			return readExactlyXBytes(dis, Long.BYTES);
		}
	}

	private static class DoubleIO implements IO {

		public int write(DataOutputStream dos, byte[] content) throws IOException {
			assert content.length == Double.BYTES;
			dos.write(content);
			return Double.BYTES;
		}

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof Literal;
			dos.writeDouble(((Literal) v).doubleValue());
			return Double.BYTES;
		}

		@Override
		public Literal read(DataInputStream in) throws IOException {
			return SVF.createLiteral(in.readDouble());
		}

		@Override
		public byte[] getBytes(Value v) {
			byte[] content = new byte[Double.BYTES];
			ByteBuffer.wrap(content).putDouble(0, ((Literal) v).doubleValue());
			return content;
		}

		@Override
		public Value read(byte[] in) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getDouble(0));
		}

		@Override
		public Value read(byte[] in, int offset, int length) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getDouble(offset));
		}
		
		@Override
		public boolean fixedWidth() {
			return true;
		}

		public byte[] readRaw(DataInputStream dis) throws IOException {
			return readExactlyXBytes(dis, Double.BYTES);
		};
	}

	private static class FloatIO implements IO {

		public int write(DataOutputStream dos, byte[] content) throws IOException {
			assert content.length == Float.BYTES;
			dos.write(content);
			return Float.BYTES;
		}

		@Override
		public int write(DataOutputStream dos, Value v) throws IOException {
			assert v instanceof Literal;
			dos.writeFloat(((Literal) v).floatValue());
			return Float.BYTES;
		}

		@Override
		public Literal read(DataInputStream in) throws IOException {
			return SVF.createLiteral(in.readFloat());
		}

		@Override
		public byte[] getBytes(Value v) {
			byte[] content = new byte[Float.BYTES];
			ByteBuffer.wrap(content).putFloat(0, ((Literal) v).floatValue());
			return content;
		}

		@Override
		public Value read(byte[] in) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getFloat(0));
		}

		@Override
		public Value read(byte[] in, int offset, int length) {
			return SVF.createLiteral(ByteBuffer.wrap(in).getFloat(offset));
		}

		@Override
		public boolean fixedWidth() {
			return true;
		}

		public byte[] readRaw(DataInputStream dis) throws IOException {
			return readExactlyXBytes(dis, Float.BYTES);
		};
	}

	private static IO forDatatype(IRI datatype) {
		CoreDatatype from = CoreDatatype.from(datatype);
		if (from != null && from.isXSDDatatype()) {
			switch (from.asXSDDatatype().get()) {
			case INT:
				return new IntIO();
			case LONG:
				return new LongIO();
			case FLOAT:
				return new FloatIO();
			case DOUBLE:
				return new DoubleIO();
			case STRING:
				return new DatatypeAsUTF8IO(datatype);
			default:
				return new DatatypeAsUTF8IO(datatype);
			}
		}
		return new DatatypeAsUTF8IO(datatype);
	}

	public static IO forOutput(Kind kind, IRI datatype, String lang) {
		switch (kind) {
		case IRI:
			return new IRIIO();
		case BNODE:
			return new BNodeIO();
		case LITERAL: {
			if (lang != null) {
				return new LangStringIO(lang);
			} else {

				return forDatatype(datatype);
			}
		}
		default:
		case TRIPLE:
			throw new UnsupportedOperationException("No RDF-star support yet");
		}
	}

	public static IO forOutput(Kind kind) {
		switch (kind) {
		case IRI:
			return new IRIIO();
		case BNODE:
			return new BNodeIO();
		case LITERAL:
		case TRIPLE:
		default:
			throw new UnsupportedOperationException("No RDF-star support yet");
		}
	}

	private static byte[] readExactlyXBytes(DataInputStream dis, int bytesToRead) throws IOException, EOFException {
		byte[] content = new byte[bytesToRead];
		int allread = 0;
		int read = 0;

		while (read != -1 && allread < bytesToRead) {
			read = dis.read(content, allread, bytesToRead - allread);
			if (read == -1) {
				throw new EOFException();
			}
			allread += read;
		}
		return content;
	};
}
