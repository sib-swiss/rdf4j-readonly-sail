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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public final class ThreadSafeSecondIterator<T> implements Iterator<T> {
	volatile boolean done = false;

	private Iterator<T> current;
	private final BlockingQueue<Collection<T>> readBytes = new ArrayBlockingQueue<>(32);

	@Override
	public boolean hasNext() {
		if (current != null && current.hasNext())
			return true;
		while (!done || !readBytes.isEmpty()) {
			Collection<T> poll = readBytes.poll();
			if (poll != null) {
				current = poll.iterator();
				if (current != null && current.hasNext())
					return true;
			}
		}
		return false;
	}

	@Override
	public T next() {
		return current.next();
	}

	public Future<?> addToQueue(Iterator<T> from, ExecutorService exec) {
		return exec.submit(() -> {
			readInSecondThread(from);
		});
	}

	public Future<?> addToQueue(Supplier<Iterator<T>> from, ExecutorService exec) {
		return exec.submit(() -> {
			readInSecondThread(from.get());
		});
	}

	private void readInSecondThread(Iterator<T> from) {
		List<T> more = new ArrayList<>(1024);
		while (from.hasNext()) {
			for (int i = 0; from.hasNext() && i < 1024; i++) {
				T next = from.next();
				more.add(next);
			}
			try {
				readBytes.put(more);
				more = new ArrayList<>(1024);
			} catch (InterruptedException e1) {
				Thread.interrupted();
			}
		}
		done = true;
	}
}
