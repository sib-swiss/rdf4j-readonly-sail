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
package swiss.sib.swissprot.sail.readonly.datastructures.iterators;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Merges sorted input and maintains sorting via the passed in comparators. Removes duplicates during running if
 * distinct is set.
 *
 * @implNote we maintain a pair of priority queues that we try to make as small as possible. The first contains the
 *           elements that are currently sorted in order of priority. The second contains the indexes in the same order
 *           used to advance the readers This avoids modifying the readers (the to be merged iterators) array.
 * @param <T>
 */
class MergeSortedIterators<T> implements Iterator<T> {

	// The known list of already sorted
	private final T[] queue;
	// A queue of indexes into the array of readers.
	private final char[] queueOfReaderIndex;
	// Comparator function used for the sorting
	private final Comparator<T> comparator;
	// The iterators to merged into one unique in order iteration.
	private final Iterator<T>[] readers;
	// The field showing the current size of the queue.
	// Unfortunately increased and decreased for each take/refill
	private int size;
	private final boolean distinct;

	@SuppressWarnings("unchecked")
	MergeSortedIterators(Comparator<T> comparator, List<Iterator<T>> fileReaders) {
		this(comparator, fileReaders, true);
	}

	MergeSortedIterators(Comparator<T> comparator, List<Iterator<T>> fileReaders, boolean distinct) {
		super();
		this.distinct = distinct;
		assert fileReaders.size() < Character.MAX_VALUE;
		this.comparator = comparator;
		this.queue = (T[]) new Object[fileReaders.size()];
		this.readers = new Iterator[fileReaders.size()];
		this.queueOfReaderIndex = new char[fileReaders.size()];
		char i = 0;
		for (Iterator<T> fileReader : fileReaders) {
			this.readers[i] = fileReader;
			if (fileReader.hasNext()) {
				T nextLine = fileReader.next();
				while (!add(nextLine, i) && fileReader.hasNext()) {
					nextLine = fileReader.next();
				}
				i++;
			}
		}
		assert i != Character.MAX_VALUE;
	}

	/**
	 * Try to add the next element to be sorted into the right place.
	 *
	 * @param nextLine      the thing to be sorted
	 * @param fileReaderIdx the index of the file reader
	 * @return if it was added (false if already in the prioroty queue)
	 */
	private boolean add(T nextLine, char fileReaderIdx) {

		// if the queue is empty we can always just add it to the end
		if (size == 0) {
			return addAtEnd(nextLine, fileReaderIdx);
		} else {
			// Test if the thing to add is beyond the last position, then we can just add it
			// to the end and no need to binarySearch.
			int compared = comparator.compare(queue[size - 1], nextLine);
			// If it is already in the queue then we don't need to add.
			if (compared == 0 && distinct) {
				return false;
			} else if (compared == 0) {
				// we added it at the end
				return addAtEnd(nextLine, fileReaderIdx);
			} else if (compared < 0) {
				// The value sorts beyond the last element
				return addAtEnd(nextLine, fileReaderIdx);
			} else {
				// The value sorts inside the queue
				return addInMiddle(nextLine, fileReaderIdx);
			}
		}
	}

	// Simply add it to the end of the queues and increase the known size.
	// Normally this just overwrites the last element in the queues
	private boolean addAtEnd(T nextLine, char fileReaderIdx) {
		queueOfReaderIndex[size] = fileReaderIdx;
		queue[size++] = nextLine;
		return true;
	}

	// We need are very likely modify the arrays in the middle
	private boolean addInMiddle(T nextLine, char fileReaderIdx) {
		int searchIndex = Arrays.binarySearch(queue, 0, size + 1, nextLine, comparator);
		if (searchIndex >= 0) {
			if (distinct) {
				// If it is already in the queue then we don't need to add.
				return false;
			}
			return insert(nextLine, fileReaderIdx, searchIndex);
		} else {
			int insertAt = Math.abs(searchIndex) - 1;
			return insert(nextLine, fileReaderIdx, insertAt);
		}
	}

	private boolean insert(T nextLine, char fileReaderIdx, int insertAt) {
		if (insertAt != size) {
			// We only need to shift if it is not the last element.
			System.arraycopy(queue, insertAt, queue, insertAt + 1, size - insertAt);
			System.arraycopy(queueOfReaderIndex, insertAt, queueOfReaderIndex, insertAt + 1, size - insertAt);
		}
		queue[insertAt] = nextLine;
		queueOfReaderIndex[insertAt] = fileReaderIdx;
		size++;
		return true;
	}

	/**
	 * Advance the subreader that was taken from
	 *
	 * @param fileReaderIdx index
	 */
	private void readFromSubIterator(char fileReaderIdx) {
		Iterator<T> iter = readers[fileReaderIdx];
		if (iter.hasNext()) {
			T nextLine = iter.next();
			while (!add(nextLine, fileReaderIdx) && iter.hasNext()) {
				nextLine = iter.next();
			}
		}
	}

	@Override
	public boolean hasNext() {
		return size != 0;
	}

	@Override
	public T next() {
		char readerIdx = queueOfReaderIndex[0];
		T next = take();
		readFromSubIterator(readerIdx);
		return next;
	}

	private T take() {
		T n = queue[0];
		size--;
		System.arraycopy(queue, 1, queue, 0, size);
		System.arraycopy(queueOfReaderIndex, 1, queueOfReaderIndex, 0, size);
		return n;
	}
}
