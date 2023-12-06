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
package swiss.sib.swissprot.sail.readonly.datastructures.pages;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.function.Predicate;

import swiss.sib.swissprot.sail.readonly.datastructures.iterators.CollectingOfLong;
import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;
import swiss.sib.swissprot.sail.readonly.datastructures.pages.Page.Type;

/**
 *
 @author <a href="mailto:jerven.bolleman@sib.swiss">Jerven Bolleman</a>
 * @param <T> the type of object represented by this key value store
 */
public class LongLongSpinalList<T> {

	public static final int CHUNK_SIZE = 32 * 1024;
	private final LongLongToObj<T> reconstructor;
	private final ToLong<T> getKey;
	private final ToLong<T> getValue;
	private final Comparator<T> comparator;
	private final ArrayList<Page<T>> pages = new ArrayList<>();
	private BasicPage<T> current;

	public LongLongSpinalList(LongLongToObj<T> reconstructor, ToLong<T> getKey, ToLong<T> getValue,
			Comparator<T> comparator) {
		this.reconstructor = reconstructor;
		this.getKey = getKey;
		this.getValue = getValue;
		this.comparator = comparator;
	}

	public void toStream(DataOutputStream stream) throws IOException {
		stream.writeInt(pages.size());
		for (Page<T> page : pages) {
			stream.write(page.getType().getCode());
			page.toStream(stream);
		}
	}

	public void fromStream(RandomAccessFile raf) throws IOException {
		int noOfPages = raf.readInt();
		while (pages.size() < noOfPages) {
			byte type = raf.readByte();
			int size = raf.readInt();
			MappedByteBuffer map = raf.getChannel().map(READ_ONLY, raf.getFilePointer(), size);
			raf.seek(raf.getFilePointer() + size);
			Type fromCode = Type.fromCode(type);
			switch (fromCode) {
			case BASIC:
			case BASIC_BUFFERED:
				pages.add(new BasicBufferedPage<>(map, reconstructor));
				break;
			case COMPRESSED:
			case COMPRESSED_BUFFERED:
				pages.add(new CompressedBufferedPage<>(map, reconstructor, getKey));
				break;
			case SORT:
				throw new IllegalStateException("SORT is an Page type that should not exist here");
			}
		}
	}

	public Iterator<T> iterator() {
		Iterator<Page<T>> iterator = pages.iterator();
		return Iterators.concat(Iterators.map(iterator, Page::iterator));
	}

	public void trimAndSort() {
		if (current != null) {
			current.sort();
			pages.add(current);
			current = null;
		}
		sort();
	}

	public Iterator<T> iterateWithKey(long key) {
		if (pages.isEmpty()) {
			return Collections.emptyIterator();
		}
		int firstIndex = 0;
		int lastIndex = 1;
		if (pages.size() > 1) {
			firstIndex = findFirstPageThatMightMatch(key);
			lastIndex = lastFirstPageThatMightMatch(firstIndex, key);
		}
		List<Page<T>> potential = pages.subList(firstIndex, lastIndex);
		if (potential.isEmpty()) {
			return Collections.emptyIterator();
		}
		Iterator<Page<T>> pagesHavingKey = Iterators.filter(potential.iterator(), c -> c.hasKey(key));
		return Iterators.concat(Iterators.map(pagesHavingKey, c -> c.fromKey(key, getKey)));
	}

	private int findFirstPageThatMightMatch(long key) {
		int index = Collections.binarySearch(pages, new SearchPage<>(key),
				(l, r) -> Long.compare(l.firstKey(), r.firstKey()));
		if (index > 0) {
			// We might need to backtrack as we found a chunck in which
			// have the key, but pages that are before this one might have the
			// key as well
			while (index > 0 && pages.get(index - 1).firstKey() == key) {
				index--;
			}
		} else if (index < 0) {
			index = Math.abs(index + 1);
			// If the insertion spot is after the last page the
			// content can only be in the last page
			if (index >= pages.size()) {
				return pages.size() - 1;
			}
			// Make sure that we do not need to go to an earlier page
			while (index > 0 && pages.get(index).firstKey() > key) {
				index--;
			}
		}
		return index;
	}

	private int lastFirstPageThatMightMatch(int index, long key) {
		index++;
		// Check if there are more pages that might have the data
		while (index < pages.size() - 1 && pages.get(index + 1).lastKey() <= key) {
			index++;
		}
		return index;
	}

	void sort() {
		for (Page<T> c : pages) {
			c.sort();
		}
		pages.sort((l, r) -> Long.compare(getKey.apply(l.first()), getKey.apply(r.first())));
	}

	public void add(T t) {

		if (current == null) {
			current = new BasicPage<>(reconstructor, getKey, getValue, comparator);
		} else if (current.isFull()) {
			current.sort();
			if (CompressedPage.<T>canCompress(current, getKey, getValue)) {
				CompressedPage<T> cc = new CompressedPage<>(current, reconstructor, getKey, getValue);
				pages.add(cc);
			} else {
				pages.add(current);
			}
			current = new BasicPage<>(reconstructor, getKey, getValue, comparator);
		}
		current.add(t);
	}

	boolean isEmpty() {
		if (pages.isEmpty() && current == null) {
			return true;
		} else if (current != null) {
			return current.size() == 0;
		} else {
			for (Page<T> c : pages) {
				if (c.first() != null) {
					return false;
				}
			}
		}
		return true;
	}

	public OfLong keyIterator() {
		Iterator<OfLong> iter = pages.stream().map(Page::keyIterator).iterator();
		return new CollectingOfLong(iter);
	}

	public OfLong valueIterator() {
		Iterator<OfLong> iter = pages.stream().map(Page::valueIterator).iterator();
		return new CollectingOfLong(iter);
	}

	public long size() {
		return pages.stream().mapToLong(c -> c.size()).sum();
	}

	Iterator<T> iterateWithValue(long id) {
		Predicate<T> p = e -> getValue.apply(e) == id;
		return Iterators.filter(iterator(), p);

	}

	private static class SearchPage<T> implements Page<T> {

		private static final String NOT_SUPPORTED = "Not supported, this is just for the binary search.";
		private final long key;

		public SearchPage(long key) {
			this.key = key;
		}

		@Override
		public long size() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public boolean isFull() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public void sort() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public T first() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public T last() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public OfLong keyIterator() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public OfLong valueIterator() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public Iterator<T> iterator(int start) {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public long firstKey() {
			return key;
		}

		@Override
		public long lastKey() {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public void toStream(DataOutputStream stream) throws IOException {
			throw new UnsupportedOperationException(NOT_SUPPORTED);
		}

		@Override
		public Type getType() {
			return Type.SORT;
		}
	}
}
