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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class Iterators {
	private Iterators() {

	}

	/**
	 * Early terminate the resulting iterator upon first T that returns true for the given predicate.
	 *
	 * @param <T>
	 * @param original
	 * @param test
	 * @return An iterator that takes elements from wrapped until the predicate returns true for a potential value.
	 */
	public static <T> Iterator<T> earlyTerminate(Iterator<T> original, Predicate<T> test) {
		return new Iterator<>() {
			private T t = null;

			@Override
			public boolean hasNext() {
				while (original.hasNext() && t == null) {
					T p = original.next();
					if (test.test(p)) {
						t = p;
						return true;
					} else
						return false;
				}
				return false;
			}

			@Override
			public T next() {
				return t;
			}
		};
	}

	/**
	 * Filter an iterator with a predicate.
	 *
	 * @param <T>
	 * @param original the iterator that might return unwanted results
	 * @param filter   the filter to remove unwanted results
	 * @return an iterator giving only wanted results.
	 */
	public static <T> Iterator<T> filter(Iterator<T> original, Predicate<T> filter) {
		return new Iterator<>() {
			T next;

			@Override
			public boolean hasNext() {
				while (next == null && original.hasNext()) {
					T pn = original.next();
					if (filter.test(pn)) {
						next = pn;
						return true;
					}
				}
				return next != null;
			}

			@Override
			public T next() {
				T temp = next;
				next = null;
				return temp;
			}
		};
	}

	/**
	 * @See java.util.stream.map
	 * @param <I>
	 * @param <O>
	 * @param iterator
	 * @param mapper
	 * @return an iterator that returns objects of type O.
	 */
	public static <I, O> Iterator<O> map(Iterator<I> iterator, Function<I, O> mapper) {
		return new Iterator<>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public O next() {
				return mapper.apply(iterator.next());
			}
		};
	}

	/**
	 * Takes multiple iterators and return as if one. Giving all results of one iterator and then another.
	 */
	public static <T> Iterator<T> concat(Collection<Iterator<T>> iter) {
		if (iter.isEmpty()) {
			return Collections.emptyIterator();
		} else if (iter.size() == 1) {
			return iter.iterator().next();
		} else {
			return new ConcatenationOfIterators<>(iter.iterator());
		}
	}

	/**
	 * Takes multiple iterators and return as if one. Giving all results of one iterator and then another.
	 *
	 * @param <T>
	 * @param iter
	 * @return a single iterator
	 */
	public static <T> Iterator<T> concat(Iterator<Iterator<T>> iter) {
		if (iter.hasNext()) {
			List<Iterator<T>> col = new ArrayList<>();
			iter.forEachRemaining(col::add);
			return concat(col);
		} else {
			return Collections.emptyIterator();
		}
	}

	/**
	 * Merge iterators of sorted input, removing duplicates into one iterator
	 *
	 * @param <T>
	 * @param comparator
	 * @param sources
	 * @return one iterator in comparator order
	 */
	public static <T> Iterator<T> mergeDistinctSorted(Comparator<T> comparator, List<Iterator<T>> sources) {
		if (sources.isEmpty()) {
			return Collections.emptyIterator();
		} else if (sources.size() == 1) {
			Iterator<T> source = sources.get(0);
			return new ReducingIterator<>(source, comparator);
		} else {
			return new MergeSortedIterators<>(comparator, sources, true);
		}
	}

	/**
	 * Merge iterators of sorted input repeating duplicates into one iterator
	 *
	 * @param <T>
	 * @param comparator
	 * @param sources
	 * @return one iterator in comparator order
	 */
	public static <T> Iterator<T> mergeSorted(Comparator<T> comparator, List<Iterator<T>> sources) {
		if (sources.isEmpty()) {
			return Collections.emptyIterator();
		} else if (sources.size() == 1) {
			Iterator<T> source = sources.get(0);
			return source;
		} else {
			return new MergeSortedIterators<>(comparator, sources, false);
		}
	}
}
