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

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ExternalProcessHelper {
	private ExternalProcessHelper() {

	}

	static void runPipeline(ProcessBuilder... programs) throws IOException {
		List<ProcessBuilder> steps = Arrays.asList(programs);
		for (ProcessBuilder step : steps)
			step.redirectError(Redirect.INHERIT);
		List<Process> pipeline = ProcessBuilder.startPipeline(steps);
		for (Process process : pipeline) {
			waitForProcessToBeDone(process);
		}
	}

	public static void waitForProcessToBeDone(Process process) {
		boolean done = false;
		while (!done) {
			try {
				int waitFor = process.waitFor();
				if (waitFor != 0) {
					throw new RuntimeException("Failed exit code:" + waitFor + process.info().commandLine().get());
				}
				done = true;
			} catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	public static void waitForProcessesToBeDone(List<Process> processes) {
		for (Process p : processes)
			waitForProcessToBeDone(p);
	}

	public static void waitForFutures(List<Future<IOException>> closers) throws IOException {
		while (!closers.isEmpty()) {
			//We remove the last element without using an iterator to avoid
			//Concurrent Modification exceptions.
			//In a rare case of merging we may create a new feature in a different thread. 
			int idx = closers.size() - 1;
			Future<IOException> fut = closers.get(idx);
			try {
				IOException ioException = fut.get();
				if (ioException != null)
					throw ioException;
				closers.remove(idx);
			} catch (InterruptedException e) {
				Thread.interrupted();
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static void inheritErrorLcAllC(List<ProcessBuilder> steps) {
		for (ProcessBuilder step : steps) {
			inheritErrorLcAllC(step);
		}
	}

	public static void inheritErrorLcAllC(ProcessBuilder step) {

		step.environment().put("LC_ALL", "C");
		step.redirectError(Redirect.INHERIT);

	}
}
