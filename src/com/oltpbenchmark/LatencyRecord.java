/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/** Efficiently stores a record of (start time, latency) pairs. */
public class LatencyRecord implements Iterable<LatencyRecord.Sample> {
	/** Allocate space for 128k samples at a time */
	static final int ALLOC_SIZE = 1 << 17;

	/**
	 * Contains (start time, latency, transactionType, workerid, phaseid) pentiplets 
	 * in microsecond form. The start times are "compressed" by encoding them as 
	 * increments, starting from startNs. A 32-bit integer provides sufficient resolution
	 * for an interval of 2146 seconds, or 35 minutes.
	 */
	private ArrayList<Sample[]> values;
	private volatile int nextIndex;

	public LatencyRecord() {
		nextIndex = 0;
		values = new ArrayList<Sample[]>();
	}

    public void addLatency(int transType, long startNs, long endNs, int workerId, int phaseId) {
		assert endNs >= startNs;

		int chunkIdx = nextIndex % ALLOC_SIZE;
		if (chunkIdx == 0) {
			values.add(new Sample[ALLOC_SIZE]);
		}
		Sample[] chunk = values.get(nextIndex / ALLOC_SIZE);
		int latencyUs = (int) ((endNs - startNs + 500) / 1000);
		chunk[chunkIdx] = new Sample(transType, startNs, latencyUs, workerId, phaseId);
		nextIndex += 1;
	}

	/** Returns the number of recorded samples. */
	public int size() {
		return nextIndex;
	}

	/** Stores the start time and latency for a single sample. Immutable. */
	public static final class Sample implements Comparable<Sample> {
		public final int tranType;
		public long startNs;
		public final int latencyUs;
		public final int workerId;
		public final int phaseId;

        public Sample(int tranType, long startNs, int latencyUs, int workerId, int phaseId) {
			this.tranType = tranType;
			this.startNs = startNs;
			this.latencyUs = latencyUs;
			this.workerId = workerId;
			this.phaseId = phaseId;
		}

		public int compareTo(Sample other) {
			long diff = this.startNs - other.startNs;

			// explicit comparison to avoid long to int overflow
			if (diff > 0)
				return 1;
			else if (diff < 0)
				return -1;
			else {
				assert diff == 0;
				return 0;
			}
		}
	}

	public final class LatencyRecordIterator implements Iterator<Sample> {
		private int curIndex;
		private int lastIndex;

		LatencyRecordIterator() {
			this(0, nextIndex);
		}

		LatencyRecordIterator(int start, int end) {
			assert start <= end;
			curIndex = start;
			lastIndex = end;
		}

		public boolean hasNext() {
			return curIndex < lastIndex;
		}

		public Sample next() {
			Sample sample = values.get(curIndex / ALLOC_SIZE)[curIndex % ALLOC_SIZE];
			curIndex += 1;
			return sample;
		}

		public void remove() {
			throw new UnsupportedOperationException("remove is not supported");
		}

		public int size() {
			return lastIndex - curIndex;
		}
	}

	public Iterator<Sample> iterator() {
		return new LatencyRecordIterator();
	}

	public Iterator<Sample> iterator(int start, int end) {
		return new LatencyRecordIterator(start, end);
	}
}
