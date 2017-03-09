/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Factory for custom futures used by the quorum replication.
 * Created by Konstantin Spirov on 2/3/2017.
 */
@Slf4j
class QuorumFutureFactory {

    /**
     * Get a thread safe future that will complete only when n/2+1 futures complete or if there is no hope
     * (if n/2+1 futures are canceled or have conflicting value).
     *
     * The future returned does not block explicitly, it aggregates the futures and delegates the blocking.
     *
     * In case of normal execution, any of the compete futures can be used to return the result.
     * In case of termination, the cancel flag will be updated and if any of the futures threw an exception,
     * ExecutionException will be thrown,  otherwise the future will return null
     *
     * @param comparator Any comparator consistent with equals that is able to distinguish the results
     * @param futures The N futures
     * @return The composite future
     */
    static <R> CompositeFuture<R> getQuorumFuture(Comparator<R> comparator, CompletableFuture<R>... futures) {
        return new CompositeFuture<R>(comparator, futures.length/2+1, futures);
    }

    /**
     * Get a thread safe future that will complete only when a single futures complete.
     *
     * The future returned does not block explicitly, it aggregates the futures and delegates the blocking.
     *
     * In case if some future completes successfully its value will be returned.
     * In case of termination, the cancel flag will be updated and if any of the futures threw an exception,
     * ExecutionException will be thrown,  otherwise the future will return null
     *
     * @param comparator Any comparator consistent with equals that is able to distinguish the results
     * @param futures The N futures
     * @return The composite future
     */
    static <R> CompositeFuture<R> getFirstWinsFuture(Comparator<R> comparator, CompletableFuture<R>... futures) {
        return new CompositeFuture<R>(comparator, 1, futures);
    }


    public static class CompositeFuture<R> implements Future<R> {
        private final Comparator<R> comparator;
        private final int quorum;
        private final CompletableFuture<R>[] futures;
        private volatile boolean done = false;
        private volatile boolean canceled = false;
        private volatile boolean conflict = false;
        private volatile Throwable throwable;

        private CompositeFuture(Comparator<R> comparator, int quorum, CompletableFuture<R>... futures) {
            this.comparator = comparator;
            this.quorum = quorum;
            this.futures = futures;
        }

        @Override
        public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            Comparator<Integer> ic = Integer::compareTo;
            TreeMultimap<Integer, R> valuesSortedByCount = TreeMultimap.create(ic.reversed(), comparator);
            HashMultimap<R, Integer> indexesByValue = HashMultimap.create();

            long until = 0;
            boolean infinite = (timeout==Long.MAX_VALUE);
            if (!infinite) {
                until = System.nanoTime() + unit.toNanos(timeout);
            }

            while (infinite || System.nanoTime() < until) {
                Integer lastExceptionIndex = null;
                int numIncompleteFutures = 0;
                CompletableFuture aggregatedFuture = null; // block until some future completes
                for (int i = 0; i < futures.length; i++) {
                    CompletableFuture<R> c = futures[i];
                    if (!c.isDone()) {
                        numIncompleteFutures++;
                        if (aggregatedFuture == null) {
                            aggregatedFuture = c;
                        } else {
                            aggregatedFuture = CompletableFuture.anyOf(aggregatedFuture, c);
                        }
                    } else {
                        if (c.isCancelled()) {
                        } else if (c.isCompletedExceptionally()) {
                            lastExceptionIndex = i;
                        } else {
                            R value = c.get();
                            Set<Integer> indexes = indexesByValue.get(value);
                            if (!indexes.contains(i)) {
                                valuesSortedByCount.remove(indexes.size(), value);
                                indexes.add(i);
                                valuesSortedByCount.put(indexes.size(), value);
                            }
                            if (indexesByValue.keySet().size()>1) {
                                conflict = true;
                            }
                        }
                    }
                }

                int greatestNumCompleteFutures = valuesSortedByCount.size()==0? 0:
                        valuesSortedByCount.keySet().iterator().next();
                if (greatestNumCompleteFutures>=quorum) { // normal exit, quorum
                    done = true;
                    return valuesSortedByCount.entries().iterator().next().getValue();
                }
                boolean noMoreHope = numIncompleteFutures+greatestNumCompleteFutures < quorum;
                if (noMoreHope) {
                    done = canceled = true;
                    if (lastExceptionIndex != null) {
                        try {
                            futures[lastExceptionIndex].get(); // this will throw the ExecutionException
                        } catch (ExecutionException e) {
                            throwable = e.getCause();
                            throw e;
                        }
                    }
                    throw new ExecutionException(
                            new QuorumUnreachableException(greatestNumCompleteFutures, quorum));
                }
                if (infinite) {
                    aggregatedFuture.get();
                } else {
                    aggregatedFuture.get(timeout, unit);
                }
            }
            throw new TimeoutException();
        }


        @Override
        public R get() throws InterruptedException, ExecutionException {
            try {
                return get(Long.MAX_VALUE, null);
            } catch (TimeoutException e) {
                log.error(e.getMessage(), e); // not likely to happen in near future
                return null;
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            for (CompletableFuture f : futures) {
                f.cancel(mayInterruptIfRunning);
            }
            done = canceled = true;
            return canceled;
        }

        /**
         * @return true the future was canceled explicitly, or if the future was unable to reach quorum due to
         * conflicts, cancaled futures or futures that have thrown exception.
         */
        @Override
        public boolean isCancelled() {
            return canceled;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        /**
         * @return true if there were two successful results with different values, otherwise false
         */
        public boolean isConflict() {
            return conflict;
        }

        public Throwable getThrowable() {return throwable;}
    }


}
