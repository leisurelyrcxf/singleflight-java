package com.okx.im.common.concurrency;

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * SingleFlight implements call deduplication for equal keys.
 * <p/>
 * Example:
 * <pre>
 * public Result expensiveOperation(final Parameters parameters) throws Exception {
 *     return singleFlight.execute(parameters, new Callable&lt;Result&gt;() {
 *         &#64;Override
 *         public Result call() {
 *             return expensiveFunction(parameters);
 *         }
 *     });
 * }
 * </pre>
 * <p>
 * The code is inspired from golang singleflight <a href="https://github.com/golang/sync/blob/master/singleflight/singleflight.go">golang singleflight</a>
 */
public class SingleFlight<K> {

    private static class Call<V> {
        private final Object lock = new Object();
        private boolean finished = false;
        @Nullable
        private V result = null;
        @Nullable
        private ExecutionException error = null;

        public @Nonnull V await() throws ExecutionException, InterruptedException {
            synchronized (lock) {
                while (!finished) {
                    lock.wait();
                }
                Preconditions.checkState(error == null != (result == null));
                if (error != null) {
                    throw error;
                }
                return result;
            }
        }

        public V exec(Callable<V> callable) throws Exception {
            V res = null;
            Exception err = null;
            try {
                res = callable.call();
                return res;
            } catch (Exception e) {
                err = e;
                throw e;
            } finally {
                synchronized (lock) {
                    // NON-NLS
                    Preconditions.checkState(err != null || res != null, "callable must return a non-null result");
                    if (err != null) {
                        this.error = new ExecutionException(err);
                    }
                    this.result = res;
                    this.finished = true;
                    lock.notifyAll();
                }
            }
        }
    }

    private final ConcurrentMap<K, Call> calls;

    public SingleFlight() {
        this.calls = new ConcurrentHashMap<>();
    }

    /**
     * Execute a {@link Callable} if no other calls for the same  {@code key} are currently running.
     * Concurrent calls for the same  {@code key} result in one caller invoking the {@link Callable} and sharing the result
     * with the other callers.
     * <p/>
     * The result of an invocation is not cached, only concurrent calls share the same result.
     *
     * @param key      A unique identification of the method call.
     *                 The {@code key} must be uniquely identifiable by it's {@link Object#hashCode()} and {@link Object#equals(Object)} methods.
     * @param callable The {@link Callable} where the result can be obtained from, the result mustn't be null.
     * @return The result of invoking the {@link Callable}.
     * @throws Exception The {@link Exception} which was thrown by the {@link Callable}.
     *                   Alternatively a {@link InterruptedException} can be thrown if
     *                   the executing {@link Thread} was interrupted while waiting for the result.
     */
    @SuppressWarnings("unchecked")
    public <V> V execute(K key, Callable<V> callable) throws Exception {
        final boolean[] result = new boolean[]{false};
        Call<V> call = this.calls.computeIfAbsent(key, k -> {
            result[0] = true;
            return new Call<>();
        });
        if (!result[0]) {
            // Prev exists
            return call.await();
        }

        // I am the first call
        try {
            return call.exec(callable);
        } finally {
            this.calls.remove(key, call); // remove only if it remains the same
        }
    }

    /**
     * Forget a key, so caller can execute a new callable.
     *
     * @param key key of the task.
     */
    public void forget(K key) {
        this.calls.remove(key);
    }
}
