/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util;

import static io.netty.util.internal.ObjectUtil.checkInRange;
import static io.netty.util.internal.ObjectUtil.checkPositive;
import static io.netty.util.internal.ObjectUtil.checkNotNull;

import io.netty.util.concurrent.ImmediateExecutor;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.util.internal.StringUtil.simpleClassName;

/*
 * 针对近似 I/O 超时调度进行优化的Timer 。
 *
 * 刻度持续时间
 * 正如“近似”所述，该计时器不会按时执行预定的TimerTask 。 HashedWheelTimer在每个时间点上都会检查是否有任何TimerTask落后于计划并执行它们。
 * 您可以通过在构造函数中指定更小或更大的刻度持续时间来提高或降低执行计时的准确性。在大多数网络应用中，I/O 超时不需要准确。因此，默认滴答持续时间为 100 毫秒，大多数情况下您无需尝试不同的配置。
 *
 * 每个车轮的刻度（车轮尺寸）
 * HashedWheelTimer维护一个称为“轮”的数据结构。简单来说，轮子就是TimerTask的哈希表，其哈希函数是“任务的截止时间”。每个轮子的默认刻度数（即轮子的大小）是 512。如果要安排大量超时，可以指定更大的值。
 *
 * 不要创建很多实例。
 * HashedWheelTimer每当实例化和启动时都会创建一个新线程。因此，您应该确保仅创建一个实例并在您的应用程序中共享它。导致应用程序无响应的常见错误之一是为每个连接创建一个新实例。
 *
 * 实施细节
 * HashedWheelTimer基于George Varghese和 Tony Lauck 的论文“散列和分层定时轮：有效实现计时器设施的数据结构” 。更全面的幻灯片位于此处
 */

/**
 * A {@link Timer} optimized for approximated I/O timeout scheduling.
 *
 * <h3>Tick Duration</h3>
 * <p>
 * As described with 'approximated', this timer does not execute the scheduled
 * {@link TimerTask} on time.  {@link HashedWheelTimer}, on every tick, will
 * check if there are any {@link TimerTask}s behind the schedule and execute
 * them.
 * <p>
 * You can increase or decrease the accuracy of the execution timing by
 * specifying smaller or larger tick duration in the constructor.  In most
 * network applications, I/O timeout does not need to be accurate.  Therefore,
 * the default tick duration is 100 milliseconds and you will not need to try
 * different configurations in most cases.
 *
 * <h3>Ticks per Wheel (Wheel Size)</h3>
 * <p>
 * {@link HashedWheelTimer} maintains a data structure called 'wheel'.
 * To put simply, a wheel is a hash table of {@link TimerTask}s whose hash
 * function is 'dead line of the task'.  The default number of ticks per wheel
 * (i.e. the size of the wheel) is 512.  You could specify a larger value
 * if you are going to schedule a lot of timeouts.
 *
 * <h3>Do not create many instances.</h3>
 * <p>
 * {@link HashedWheelTimer} creates a new thread whenever it is instantiated and
 * started.  Therefore, you should make sure to create only one instance and
 * share it across your application.  One of the common mistakes, that makes
 * your application unresponsive, is to create a new instance for every connection.
 *
 * <h3>Implementation Details</h3>
 * <p>
 * {@link HashedWheelTimer} is based on
 * <a href="https://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="https://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="https://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 */
public class HashedWheelTimer implements Timer {

    static final InternalLogger logger =
            InternalLoggerFactory.getInstance(HashedWheelTimer.class);
    /**
     * 实例计数器
     */
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    // 警告次数过多
    private static final AtomicBoolean WARNED_TOO_MANY_INSTANCES = new AtomicBoolean();
    // 实例计数限制
    private static final int INSTANCE_COUNT_LIMIT = 64;
    // 一毫秒的纳秒数值
    private static final long MILLISECOND_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    // 检漏仪
    private static final ResourceLeakDetector<HashedWheelTimer> leakDetector = ResourceLeakDetectorFactory.instance()
            .newResourceLeakDetector(HashedWheelTimer.class, 1);
    // 工作状态更新器
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");

    private final ResourceLeakTracker<HashedWheelTimer> leak;
    private final Worker worker = new Worker();
    private final Thread workerThread;

    public static final int WORKER_STATE_INIT = 0;
    public static final int WORKER_STATE_STARTED = 1;
    public static final int WORKER_STATE_SHUTDOWN = 2;
    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    private volatile int workerState; // 0 - init, 1 - started, 2 - shut down
    // 持续时间
    private final long tickDuration;
    // hash轮
    private final HashedWheelBucket[] wheel;
    private final int mask;
    // 开始时间已初始化
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
    // 任务队列
    private final Queue<HashedWheelTimeout> timeouts = PlatformDependent.newMpscQueue();
    // 取消超时
    private final Queue<HashedWheelTimeout> cancelledTimeouts = PlatformDependent.newMpscQueue();
    // 待处理任务计数器
    private final AtomicLong pendingTimeouts = new AtomicLong(0);
    // 最大待处理任务计数器
    private final long maxPendingTimeouts;
    // 任务执行者
    private final Executor taskExecutor;
    // 开始时间
    private volatile long startTime;

    /**
     * 使用默认线程工厂 ( Executors.defaultThreadFactory() )、默认滴答持续时间和每个轮的默认滴答数创建一个新计时器
     * <p>
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration the duration between tick
     * @param unit         the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * 创建一个新的计时器，每个轮子具有默认的滴答数。
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick                   刻度之间的持续时间
     * @param unit          the time unit of the {@code tickDuration}   时间单位
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel   车轮的尺寸
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, true);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @param leakDetection {@code true} if leak detection should be enabled always,
     *                      if false it will only be enabled if the worker thread is not
     *                      a daemon thread.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection, -1);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory      a {@link ThreadFactory} that creates a
     *                           background {@link Thread} which is dedicated to
     *                           {@link TimerTask} execution.
     * @param tickDuration       the duration between tick
     * @param unit               the time unit of the {@code tickDuration}
     * @param ticksPerWheel      the size of the wheel
     * @param leakDetection      {@code true} if leak detection should be enabled always,
     *                           if false it will only be enabled if the worker thread is not
     *                           a daemon thread.
     * @param maxPendingTimeouts The maximum number of pending timeouts after which call to
     *                           {@code newTimeout} will result in
     *                           {@link java.util.concurrent.RejectedExecutionException}
     *                           being thrown. No maximum pending timeouts limit is assumed if
     *                           this value is 0 or negative.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
            long maxPendingTimeouts) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, leakDetection,
                maxPendingTimeouts, ImmediateExecutor.INSTANCE);
    }

    /*
     * k1 HashedWheelTimer 构造函数
     * @param threadFactory
     * @param tickDuration          刻度之间的间隔时间
     * @param unit                  tickDuration 的时间单位
     * @param ticksPerWheel         车轮的尺寸
     * @param leakDetection         true: 如果泄漏检测应始终启用. false:则仅当工作线程不是守护线程时才会启用。
     * @param maxPendingTimeouts    挂起超时的最大数量，超过此数量后调用 newTimeout 将导致抛出 RejectedExecutionException。
     *                              如果该值为 0 或负数，则假定没有最大挂起超时限制。
     * @param taskExecutor          用于执行提交的  TimerTask 的 Executor。一旦不再需要，调用者有责任关闭 Executor
     */

    /**
     * Creates a new timer.
     *
     * @param threadFactory      a {@link ThreadFactory} that creates a
     *                           background {@link Thread} which is dedicated to
     *                           {@link TimerTask} execution.
     * @param tickDuration       the duration between tick
     * @param unit               the time unit of the {@code tickDuration}
     * @param ticksPerWheel      the size of the wheel
     * @param leakDetection      {@code true} if leak detection should be enabled always,
     *                           if false it will only be enabled if the worker thread is not
     *                           a daemon thread.
     * @param maxPendingTimeouts The maximum number of pending timeouts after which call to
     *                           {@code newTimeout} will result in
     *                           {@link java.util.concurrent.RejectedExecutionException}
     *                           being thrown. No maximum pending timeouts limit is assumed if
     *                           this value is 0 or negative.
     * @param taskExecutor       The {@link Executor} that is used to execute the submitted {@link TimerTask}s.
     *                           The caller is responsible to shutdown the {@link Executor} once it is not needed
     *                           anymore.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration,
            TimeUnit unit,
            int ticksPerWheel,
            boolean leakDetection,
            long maxPendingTimeouts,
            Executor taskExecutor) {

        // 校验线程工厂不为 null
        checkNotNull(threadFactory, "threadFactory");
        checkNotNull(unit, "unit");
        // 校验是否为正数,必须 >=0
        checkPositive(tickDuration, "tickDuration");
        checkPositive(ticksPerWheel, "ticksPerWheel");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor");

        // 将ticksPerWheel标准化为2的幂并初始化轮子。
        // Normalize ticksPerWheel to power of two and initialize the wheel.
        wheel = createWheel(ticksPerWheel);  // 创建 HashedWheelBucket[]
        mask = wheel.length - 1;             // HashedWheelBucket[] 的长度 - 1

        // Convert tickDuration to nanos.
        long duration = unit.toNanos(tickDuration);  // 将 tickDuration 转换为纳秒。

        // Prevent overflow. 防止溢出。
        // 如果客户端之间的间隔时间 大于 Long.MAX_VALUE / 时间轮槽数
        if (duration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(
                    String.format("tickDuration: %d (expected: 0 < tickDuration in nanos < %d"
                    , tickDuration, Long.MAX_VALUE / wheel.length));
        }

        // 刻度之间的间隔不能小于 1 毫秒
        if (duration < MILLISECOND_NANOS) {
            logger.warn("Configured tickDuration {} smaller than {}, using 1ms.", tickDuration, MILLISECOND_NANOS);
            this.tickDuration = MILLISECOND_NANOS;
        } else {
            this.tickDuration = duration;
        }

        // 创建一个工作线程
        workerThread = threadFactory.newThread(worker);

        leak = leakDetection || !workerThread.isDaemon() ? leakDetector.track(this) : null;

        // 最大待处理超时时间
        this.maxPendingTimeouts = maxPendingTimeouts;

        if (INSTANCE_COUNTER.incrementAndGet() > INSTANCE_COUNT_LIMIT &&
                WARNED_TOO_MANY_INSTANCES.compareAndSet(false, true)) {
            // 报告实例过多
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // This object is going to be GCed and it is assumed the ship has sailed to do a proper shutdown. If
            // we have not yet shutdown then we want to make sure we decrement the active instance count.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
            }
        }
    }

    /**
     * @param ticksPerWheel 每轮刻度数. 默认 512
     */
    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        // 校验传入的参数是否满足范围条件 1 < ticksPerWheel < 1073741824
        //ticksPerWheel may not be greater than 2^30
        checkInRange(ticksPerWheel, 1, 1073741824, "ticksPerWheel");

        // 规范化每轮刻度
        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        // 创建一个 HashedWheelBucket 数组
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];  // 这个就是一个时间轮
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    /**
     * 规范化每轮刻度
     *
     * @param ticksPerWheel 每轮刻度数
     */
    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        // 工作状态更新器
        switch (WORKER_STATE_UPDATER.get(this)) {
            case WORKER_STATE_INIT:  // 工作状态初始化
                // 使用原子类设置状态
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    // 工作线程启动
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // 等待直到startTime被worker初始化。
        // Wait until the startTime is initialized by the worker.
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }

        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
                if (leak != null) {
                    boolean closed = leak.close(this);
                    assert closed;
                }
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                workerThread.interrupt();
                try {
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            INSTANCE_COUNTER.decrementAndGet();
            if (leak != null) {
                boolean closed = leak.close(this);
                assert closed;
            }
        }
        return worker.unprocessedTimeouts();
    }

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        checkNotNull(task, "task");
        checkNotNull(unit, "unit");

        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts ("
                    + pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
                    + "timeouts (" + maxPendingTimeouts + ")");
        }

        // 创建第一个超时任务的时候启动工作线程
        start();

        // 将超时添加到超时队列中，该队列将在下一个tick 时处理。
        // 在处理过程中，所有排队的 HashedWheelTimeouts 将被添加到正确的 HashedWheelBucket 中。
        // Add the timeout to the timeout queue which will be processed on the next tick.
        // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.

        // 处理时间 = 当前时间 + 延迟时间 - 时间轮的开始时间
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Guard against overflow. 防止溢出。
        // 如果延迟时间 > 0 并且 处理时间 < 0, 则把处理时间设置为 Long.MAX_VALUE
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }
        // 创建延迟任务
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * Returns the number of pending timeouts of this {@link Timer}.
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        if (logger.isErrorEnabled()) {
            String resourceType = simpleClassName(HashedWheelTimer.class);
            logger.error("You are creating too many " + resourceType + " instances. " +
                    resourceType + " is a shared resource that must be reused across the JVM, " +
                    "so that only a few instances are created.");
        }
    }

    private final class Worker implements Runnable {
        // 未处理的任务队列
        private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();

        /**
         * 记录时间轮指针跳过的次数
         */
        private long tick;

        @Override
        public void run() {
            // Initialize the startTime. 初始化开始之间
            startTime = System.nanoTime();
            if (startTime == 0) {
                // 我们在这里使用 0 作为未初始化值的指示符，因此请确保初始化时它不是 0。
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // 通知其他线程在start()处等待初始化。
            // Notify the other threads waiting for the initialization at start().
            startTimeInitialized.countDown();

            // 工作线程
            do {
                // 线程暂停: 等待下一次滴答
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    // 当前时间轮指针指向的索引 = 跳转次数 & (时间轮长度-1)
                    int idx = (int) (tick & mask);
                    // 处理取消的任务
                    processCancelledTasks();
                    // 获取到双向链表
                    HashedWheelBucket bucket = wheel[idx];
                    // 将任务传输到存储桶
                    transferTimeoutsToBuckets();

                    bucket.expireTimeouts(deadline);
                    tick++;
                }
            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // 填充 unprocessedTimeouts，以便我们可以从 stop() 方法返回它们。
            // Fill the unprocessedTimeouts so we can return them from stop() method.
            for (HashedWheelBucket bucket : wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            for (; ;) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            processCancelledTasks();
        }

        private void transferTimeoutsToBuckets() {
            // transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
            // adds new timeouts in a loop.
            for (int i = 0; i < 100000; i++) {
                // 任务队列取出第一个元素
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                // 如果任务被取消
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    // Was cancelled in the meantime.
                    continue;
                }

                // 时间轮指针还需要跳多少次 = 等待时间 / 指针跳1次的间隔时间
                long calculated = timeout.deadline / tickDuration;

                // 剩余回合数(计算出时间轮需要旋转的圈数) = (计算的次数 - 已跳的字数) / 时间轮长度
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                final long ticks = Math.max(calculated, tick); // Ensure we don't schedule for past.  确保我们不会安排过去的事情。
                int stopIndex = (int) (ticks & mask);   // 停止索引

                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);     // 将延迟任务添加到双向链表的末尾
            }
        }

        // 执行取消的任务
        private void processCancelledTasks() {
            // 无限循环
            for (; ;) {
                // 从取消的任务队列中弹出元素
                // 检索并删除此队列的头部，如果此队列为空，则返回null
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    // 执行到这里,说明队列中没有任务了.跳出循环
                    break;
                }
                try {
                    // 从时间轮的槽位上移除此任务
                    timeout.remove();
                } catch (Throwable t) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * 根据 startTime 和当前刻度数计算目标 nanoTime，然后等待，直到达到该目标。
         * <p>
         * calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         *
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         */
        private long waitForNextTick() {
            // 最后期限 = 间隔时间 * (滴答次数 + 1)
            long deadline = tickDuration * (tick + 1);

            for (; ;) {
                // 当前时间 = 系统时间 - 工作线程开始时间
                final long currentTime = System.nanoTime() - startTime;
                // 线程暂停时间 = 最后期限 - 当前时间
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                // 检查我们是否在 Windows 上运行，就好像在这种情况下，我们需要舍入 sleepTime 作为仅影响 JVM（如果它在 Windows 上运行）的错误的解决方法。
                // Check if we run on windows, as if thats the case we will need
                // to round the sleepTime as workaround for a bug that only affect
                // the JVM if it runs on windows.
                //
                // See https://github.com/netty/netty/issues/356
                if (PlatformDependent.isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                    if (sleepTimeMs == 0) {
                        sleepTimeMs = 1;
                    }
                }

                try {
                    // 线程暂停
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    private static final class HashedWheelTimeout implements Timeout, Runnable {

        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");

        private final HashedWheelTimer timer;
        private final TimerTask task;
        // 处理时间
        private final long deadline;

        @SuppressWarnings({"unused", "FieldMayBeFinal", "RedundantFieldInitialization"})
        private volatile int state = ST_INIT;

        // 剩余回合数
        // 在将 HashedWheelTimeout 添加到正确的 HashedWheelBucket 之前，
        // 将通过 Worker.transferTimeoutsToBuckets() 计算和设置剩余Rounds。
        //
        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        long remainingRounds;

        // This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
        // As only the workerThread will act on it there is no need for synchronization / volatile.
        HashedWheelTimeout next;
        HashedWheelTimeout prev;

        // The bucket to which the timeout was added
        HashedWheelBucket bucket;

        /**
         * @param timer    时间轮
         * @param task     任务
         * @param deadline 处理时间
         */
        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            timer.cancelledTimeouts.add(this);
            return true;
        }

        void remove() {
            // 获取哈希轮桶
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                // 移除元素
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                timer.taskExecutor.execute(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown while submit " + TimerTask.class.getSimpleName()
                            + " for execution.", t);
                }
            }
        }

        @Override
        public void run() {
            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192)
                    .append(simpleClassName(this))
                    .append('(')
                    .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining)
                        .append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining)
                        .append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ")
                    .append(task())
                    .append(')')
                    .toString();
        }
    }

    /**
     * 存储 HashedWheelTimeouts 的存储桶。它们存储在类似链接列表的数据结构中，以便轻松删除中间的 HashedWheelTimeouts。
     * 此外，HashedWheelTimeout 本身也充当节点，因此不需要创建额外的对象。
     * <p>
     * Bucket that stores HashedWheelTimeouts. These are stored in a linked-list like datastructure to allow easy
     * removal of HashedWheelTimeouts in the middle. Also the HashedWheelTimeout act as nodes themself and so no
     * extra object creation is needed.
     */
    private static final class HashedWheelBucket {
        // 用于链表数据结构
        // Used for the linked-list datastructure
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * Add {@link HashedWheelTimeout} to this bucket.
         */
        public void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * Expire all {@link HashedWheelTimeout}s for the given {@code deadline}.
         * 使给定的 {@code Deadline} 内的所有 {@link HashedWheelTimeout} 过期。
         * <p>
         * 遍历时间
         */
        public void expireTimeouts(long deadline) {
            // 双向链表的头元素
            HashedWheelTimeout timeout = head;

            // process all timeouts 执行所有的延迟任务
            while (timeout != null) {
                //
                HashedWheelTimeout next = timeout.next;
                if (timeout.remainingRounds <= 0) {
                    next = remove(timeout);
                    if (timeout.deadline <= deadline) {
                        // 执行延迟任务
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format(
                                "timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
                    }
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    timeout.remainingRounds--;
                }
                timeout = next;
            }
        }

        /**
         * 由于 HashedWheelTimeout 的结构是一个双向链表,
         * 因此移除自身元素只需要维护好前后节点两个节点的引用即可
         */
        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         */
        public void clearTimeouts(Set<Timeout> set) {
            for (; ;) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // null out prev and next to allow for GC.
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }
}
