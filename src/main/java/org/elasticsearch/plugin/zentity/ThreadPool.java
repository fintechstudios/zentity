package org.elasticsearch.plugin.zentity;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Like {@link org.elasticsearch.threadpool.ThreadPool}, this class holds common thread pools for async work.
 */
public class ThreadPool {
    final class ThreadPerTaskExecutor implements Executor {
        private final ThreadFactory threadFactory;

        public ThreadPerTaskExecutor(ThreadFactory threadFactory) {
            this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        }

        @Override
        public void execute(Runnable command) {
            threadFactory.newThread(command).start();
        }
    }

    private final Executor requestExecutor;

    private final Executor resolutionExecutor;

    public ThreadPool(ZentityConfig config) {
        requestExecutor = EsExecutors.newScaling(
            "common_request_pool",
            4,
            50,
            1,
            TimeUnit.SECONDS,
            EsExecutors.daemonThreadFactory("zentity-request"),
            new ThreadContext(config.getEnvironment().settings())
        );

        resolutionExecutor = EsExecutors.newScaling(
            "zentity-resolution",
            4,
            config.getResolutionMaxConcurrentJobs(),
            1,
            TimeUnit.SECONDS,
            EsExecutors.daemonThreadFactory("zentity-resolution"),
            new ThreadContext(config.getEnvironment().settings())
        );

//        requestExecutor = new ThreadPerTaskExecutor(EsExecutors.daemonThreadFactory("zentity-request"));
        //        resolutionExecutor = new ThreadPerTaskExecutor(EsExecutors.daemonThreadFactory("zentity-resolution"));

//        final int allocatedProcessors = EsExecutors.allocatedProcessors(config.getEnvironment().settings());
//        requestExecutor = EsExecutors.newAutoQueueFixed(
//            "request",
//            org.elasticsearch.threadpool.ThreadPool.searchThreadPoolSize(allocatedProcessors),
//            1000, 1000, 1000, 2000,
//            TimeValue.timeValueSeconds(1),
//            EsExecutors.daemonThreadFactory("zentity-request"),
//            new ThreadContext(config.getEnvironment().settings())
//        );
//        resolutionExecutor = EsExecutors.newAutoQueueFixed(
//            "resolution",
//            org.elasticsearch.threadpool.ThreadPool.searchThreadPoolSize(allocatedProcessors),
//            1000, 1000, 1000, 2000,
//            TimeValue.timeValueSeconds(1),
//            EsExecutors.daemonThreadFactory("zentity-resolution"),
//            new ThreadContext(config.getEnvironment().settings())
//        );
    }

    /**
     * The {@link Executor} that should be used for all miscellaneous async tasks when handling
     * a request.
     *
     * @return The requests executor.
     */
    public Executor requests() {
        return requestExecutor;
    }

    /**
     * The {@link Executor} that should be used for running resolution jobs.
     *
     * @return The resolution executor.
     */
    public Executor resolution() {
        return resolutionExecutor;
    }
}
