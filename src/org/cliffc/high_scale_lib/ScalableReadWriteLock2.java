/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package org.cliffc.high_scale_lib;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import sun.misc.Unsafe;

/**
 * Based on org.cliffc.ConcurrentAutoTable by Cliff Click and on
 * the read-write lock by Joe Duffy here: http://www.bluebytesoftware.com/blog/2009/02/21/AMoreScalableReaderwriterLockAndABitLessHarshConsiderationOfTheIdea.aspx
 *
 */
public class ScalableReadWriteLock2 implements Serializable {

    private static final AtomicReferenceFieldUpdater<ScalableReadWriteLock2, CAT> _catUpdater =
            AtomicReferenceFieldUpdater.newUpdater(ScalableReadWriteLock2.class, CAT.class, "_cat");
    private static final AtomicIntegerFieldUpdater<ScalableReadWriteLock2> _writerUpdater =
            AtomicIntegerFieldUpdater.newUpdater(ScalableReadWriteLock2.class, "_writer");
    private volatile int _writer;
    private volatile CAT _cat = new CAT(null, 4/*Start Small, Think Big!*/, 0); // The underlying array of concurrently updated read counters
    private final Collection<Thread> waitingReaders;
    private ThreadLocal<CAT> myCat = new ThreadLocal<CAT>();

    public ScalableReadWriteLock2() {
        this(false);
    }

    public ScalableReadWriteLock2(boolean spin) {
        waitingReaders = spin ? null : new NonBlockingHashSet<Thread>();
    }

    public void read() {
        try {
            read1(false);
        } catch (InterruptedException ex) {
            throw new AssertionError();
        }
    }

    public void readInterruptably() throws InterruptedException {
        read1(true);
    }

    private void read1(boolean interrupt) throws InterruptedException {
        if (waitingReaders != null)
            readPark(interrupt);
        else
            readSpin(interrupt);
    }

    private void readSpin(boolean interrupt) throws InterruptedException {
        SPW sw = new SPW();

        CAT cat;
        int hash = hash();
        while (true) {
            while (_writer != 0)
                sw.spin(interrupt);

            cat = _cat;
            cat.add(1, hash, this); // Try to take the read lock.
            if (_writer == 0) // Success, no writer, proceed.
                break;
            else
                cat.add(-1, hash, this); // Back off, to let the writer go through.
        }
        myCat.set(cat);
    }

    private void readPark(boolean interrupt) throws InterruptedException {
        if (tryRead())
            return;

        boolean wasInterrupted = false;
        Thread current = Thread.currentThread();
        CAT cat;

        waitingReaders.add(current);

        loop:
        while (true) {
            while (_writer != 0) {
                cat = _cat; // _cat doesn't change inside writer. unlockWrite also makes sure to use old _cat
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    if (interrupt)
                        throw new InterruptedException();
                    else
                        wasInterrupted = true; // ignore interrupts while waiting
                }
                if (!waitingReaders.contains(current))
                    break loop; // the exiting writer took care of me
            }

            int hash = hash(current);
            cat = _cat;
            cat.add(1, hash, this); // Try to take the read lock.
            if (_writer == 0) { // Success, no writer, proceed.
                waitingReaders.remove(current);
                break;
            } else
                cat.add(-1, hash, this); // Back off, to let the writer go through. Still waiting in the set.
        }

        myCat.set(cat);
        if (wasInterrupted)          // reassert interrupt status on exit
            current.interrupt();
    }

    public boolean tryRead() {
        if (_writer != 0)
            return false;

        CAT cat = _cat;

        int hash = hash();
        cat.add(1, hash, this); // Try to take the read lock.
        if (_writer != 0) {
            cat.add(-1, hash, this); // Back off, to let the writer go through.
            return false;
        } else {
            myCat.set(cat);
            return true;
        }
    }

    public void unlockRead() {
        CAT cat = myCat.get();
        cat.add(-1, hash(), this); // Just note that the current reader has left the lock.
        myCat.remove();
    }

    public void write() {
        try {
            write1(false);
        } catch (InterruptedException ex) {
            throw new AssertionError();
        }
    }

    public void writeInterruptably() throws InterruptedException {
        write1(true);
    }

    private void write1(boolean interrupt) throws InterruptedException {
        SPW sw = new SPW();
        while (true) {
            if (_writer == 0 && _writerUpdater.compareAndSet(this, 0, 1)) {
                _cat.awaitZero(interrupt); // We now hold the write lock, and prevent new readers, but we must ensure no readers exist before proceeding.
                break;
            }

            sw.spin(interrupt); // We failed to take the write lock; wait a bit and retry.
        }
    }

    public boolean tryWrite() {
        if (_writer != 0)
            return false;
        if (_writerUpdater.compareAndSet(this, 0, 1)) {
            if (_cat.isZero(this))
                return true;
            else
                _writer = 0;
        }
        return false;
    }

    public void unlockWrite() {
        if (waitingReaders == null)
            _writer = 0;
        else { // wake readers
            CAT cat = _cat;
            for (Iterator<Thread> it = waitingReaders.iterator(); it.hasNext();) { // readers that miss this loop would have to take care of themselves
                Thread t = it.next();

                cat.add(1, hash(t), this);
                it.remove();

                LockSupport.unpark(t);
            }
            _writer = 0;
        }
    }

    public int getLock() {
        if(_writer != 0)
            return -1;
        if(!_cat.isZero(this))
            return 1;
        return 0;
    }

    /**
     * Return the internal counter striping factor.  Useful for diagnosing
     * performance problems.
     */
    public int internalSize() {
        return _cat._t.length;
    }

    /**
     * A more verbose print than {@link #toString}, showing internal structure.
     * Useful for debugging.
     */
    public void print() {
        System.out.print(_writer);
        System.out.print(" ");
        _cat.print();
        System.out.println();
    }

    private boolean CAS_cat(CAT oldcat, CAT newcat) {
        return _catUpdater.compareAndSet(this, oldcat, newcat);
    }

    // Hash spreader
    private static final int hash() {
        return hash(Thread.currentThread());
    }

    private static final int hash(Thread thread) {
        int h = System.identityHashCode(thread);
        // You would think that System.identityHashCode on the current thread
        // would be a good hash fcn, but actually on SunOS 5.8 it is pretty lousy
        // in the low bits.
        h ^= (h >>> 20) ^ (h >>> 12);   // Bit spreader, borrowed from Doug Lea
        h ^= (h >>> 7) ^ (h >>> 4);
        return h << 2;                // Pad out cache lines.  The goal is to avoid cache-line contention
    }

    // --- CAT -----------------------------------------------------------------
    private static class CAT implements Serializable {

        // Unsafe crud: get a function which will CAS arrays
        private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
        private static final int _Lbase = _unsafe.arrayBaseOffset(long[].class);
        private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
        private static final AtomicLongFieldUpdater<CAT> _resizerUpdater = AtomicLongFieldUpdater.newUpdater(CAT.class, "_resizers");
        private static final int MAX_SPIN = 2;
        private volatile CAT _next;
        private final long[] _t;     // Power-of-2 array
        volatile long _resizers;    // count of threads attempting a resize

        CAT(CAT next, int sz, int init) {
            _next = next;
            _t = new long[sz];
            _t[0] = init;
        }

        private static long rawIndex(long[] ary, int i) {
            assert i >= 0 && i < ary.length;
            return _Lbase + i * _Lscale;
        }

        private final static boolean CAS(long[] A, int idx, long old, long nnn) {
            return _unsafe.compareAndSwapLong(A, rawIndex(A, idx), old, nnn);
        }

        // Only add 'x' to some slot in table, hinted at by 'hash'. Value is CAS'd so no counts are lost.  The CAS is attempted ONCE.
        public long add(int x, int hash, ScalableReadWriteLock2 master) {
            long[] t = _t;
            int idx = hash & (t.length - 1);
            // Peel loop; try once fast
            long old = t[idx];
            if (x > 0 && old > (Long.MAX_VALUE - x))
                throw new Error("Maximum lock count exceeded");
            if (old + x < 0)
                throw new IllegalMonitorStateException();

            boolean ok = CAS(t, idx, old, old + x);

            if (ok)
                return old; // Got it
            // Try harder
            int cnt = 0;
            while (true) {
                old = t[idx];
                if (CAS(t, idx, old, old + x))
                    break; // Got it!
                cnt++;
            }
            if (cnt < MAX_SPIN)
                return old; // Allowable spin loop count
            if (t.length >= 1024 * 1024)
                return old; // too big already

            // Too much contention; double array size in an effort to reduce contention
            long r = _resizers;
            int newbytes = (t.length << 1) << 3/*word to bytes*/;
            while (!_resizerUpdater.compareAndSet(this, r, r + newbytes))
                r = _resizers;
            r += newbytes;
            if (master._cat != this)
                return old; // Already doubled, don't bother
            if ((r >> 17) != 0) {      // Already too much allocation attempts?
                // TODO - use a wait with timeout, so we'll wakeup as soon as the new
                // table is ready, or after the timeout in any case.  Annoyingly, this
                // breaks the non-blocking property - so for now we just briefly sleep.
                //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
                try {
                    Thread.sleep(r >> 17);
                } catch (InterruptedException e) {
                }
                if (master._cat != this)
                    return old;
            }

            CAT newcat = new CAT(this, t.length * 2, 0);
            // Take 1 stab at updating the CAT with the new larger size.  If this
            // fails, we assume some other thread already expanded the CAT - so we
            // do not need to retry until it succeeds.
            master.CAS_cat(this, newcat);
            return old;
        }

        public boolean isZero(ScalableReadWriteLock2 master) {
            if (_next != null) {
                if (_next.isZero(master)) {
                    if (master._writer != 0)
                        _next = null;
                } else
                    return false;
            }
            long[] t = _t;
            for (int i = 0; i < t.length; i++) {
                if (t[i] != 0)
                    return false;
            }
            return true;
        }

        public void awaitZero(boolean interrupt) throws InterruptedException {
            SPW spw = new SPW();

            if (_next != null) {
                _next.awaitZero(interrupt);
                _next = null;
            }
            long[] t = _t;
            for (int i = 0; i < t.length; i++) {
                while (t[i] != 0)
                    spw.spin(interrupt);
            }
        }

        public void print() {
            long[] t = _t;
            for (int i = 1; i < t.length; i++)
                System.out.print("," + t[i]);
            System.out.print("]");
            if (_next != null)
                _next.print();
        }
    }

    private static class SPW {

        private int count = 0;

        void spin(boolean interrupt) throws InterruptedException {
            if (count++ > 32) {
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                }
            } else if (count > 12)
                Thread.yield();
            else {
                for (int i = 0; i < (2 << count); i++) {
                    if (interrupt && Thread.interrupted())  // Clears interrupted status!
                        throw new InterruptedException();
                }
            }
        }
    }
}

