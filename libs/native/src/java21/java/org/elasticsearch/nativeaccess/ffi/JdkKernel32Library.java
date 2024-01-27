/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffi;

import org.elasticsearch.nativeaccess.NativeAccess.ConsoleCtrlHandler;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.function.IntConsumer;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static java.lang.foreign.MemoryLayout.paddingLayout;
import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.ffi.RuntimeHelper.downcallHandle;
import static org.elasticsearch.nativeaccess.ffi.RuntimeHelper.upcallHandle;
import static org.elasticsearch.nativeaccess.ffi.RuntimeHelper.upcallStub;

class JdkKernel32Library implements Kernel32Library {

    private static final MethodHandle GetCurrentProcess$mh;
    private static final MethodHandle CloseHandle$mh;
    private static final MethodHandle VirtualLock$mh;
    private static final MethodHandle VirtualQueryEx$mh;
    private static final MethodHandle SetProcessWorkingSetSize$mh;
    private static final MethodHandle GetCompressedFileSizeW$mh;
    private static final MethodHandle GetShortPathNameW$mh;
    private static final MethodHandle SetConsoleCtrlHandler$mh;
    private static final MethodHandle CreateJobObjectW$mh;
    private static final MethodHandle AssignProcessToJobObject$mh;
    private static final MethodHandle QueryInformationJobObject$mh;
    private static final MethodHandle SetInformationJobObject$mh;

    private static final MethodHandle ConsoleCtrlHandler_handle$mh;
    private static final FunctionDescriptor ConsoleCtrlHandler_handle$fd = FunctionDescriptor.of(JAVA_BOOLEAN, JAVA_INT);

    static {
        GetCurrentProcess$mh = downcallHandle("GetCurrentProcess", FunctionDescriptor.of(ADDRESS));
        CloseHandle$mh = downcallHandleWithError("CloseHandle", FunctionDescriptor.ofVoid(ADDRESS));
        VirtualLock$mh = downcallHandleWithError("VirtualLock", FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_LONG));
        VirtualQueryEx$mh = downcallHandleWithError(
            "VirtualQueryEx",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG));
        SetProcessWorkingSetSize$mh = downcallHandleWithError(
            "SetProcessWorkingSetSize",
            FunctionDescriptor.of(ADDRESS, JAVA_LONG, JAVA_LONG));
        GetCompressedFileSizeW$mh = downcallHandleWithError(
            "GetCompressedFileSizeW",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
        GetShortPathNameW$mh = downcallHandleWithError(
            "GetShortPathNameW",
            FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        SetConsoleCtrlHandler$mh = downcallHandleWithError(
            "SetConsoleCtrlHandler",
            FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_BOOLEAN));
        CreateJobObjectW$mh = downcallHandleWithError(
            "CreateJobObjectW",
            FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS));
        AssignProcessToJobObject$mh = downcallHandleWithError(
            "AssignProcessToJobObject",
            FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, ADDRESS));
        QueryInformationJobObject$mh = downcallHandleWithError(
            "QueryInformationJobObject",
            FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));
        SetInformationJobObject$mh = downcallHandleWithError(
            "SetInformationJobObject",
            FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));

        ConsoleCtrlHandler_handle$mh = upcallHandle(ConsoleCtrlHandler.class, "handle", ConsoleCtrlHandler_handle$fd);
    }

    // GetLastError can change from other Java threads so capture it
    private static final StructLayout CAPTURE_GETLASTERROR_LAYOUT = Linker.Option.captureStateLayout();
    private static final Linker.Option CAPTURE_GETLASTERROR_OPTION = Linker.Option.captureCallState("GetLastError");
    private static final VarHandle GetLastError$vh = CAPTURE_GETLASTERROR_LAYOUT.varHandle(groupElement("GetLastError"));

    private static MethodHandle downcallHandleWithError(String function, FunctionDescriptor functionDescriptor) {
        return downcallHandle(function, functionDescriptor, CAPTURE_GETLASTERROR_OPTION);
    }

    static class JdkHandle implements Handle {
        MemorySegment address;

        JdkHandle(MemorySegment address) {
            this.address = address;
        }
    }

    static class JdkMemoryBasicInformation implements MemoryBasicInformation {
        private static final MemoryLayout layout = MemoryLayout.structLayout(
            ADDRESS,
            ADDRESS,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG,
            JAVA_LONG);
        private static final VarHandle BaseAddress$vh = layout.varHandle(groupElement(0));
        private static final VarHandle AllocationBase$vh = layout.varHandle(groupElement(1));
        private static final VarHandle AllocationProtect$vh = layout.varHandle(groupElement(2));
        private static final VarHandle RegionSize$vh = layout.varHandle(groupElement(3));
        private static final VarHandle State$vh = layout.varHandle(groupElement(4));
        private static final VarHandle Protect$vh = layout.varHandle(groupElement(5));
        private static final VarHandle Type$vh = layout.varHandle(groupElement(6));

        private final MemorySegment segment;

        JdkMemoryBasicInformation() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
        }

        @Override
        public long BaseAddress() {
            return ((MemorySegment)BaseAddress$vh.get(segment)).address();
        }

        @Override
        public long AllocationBase() {
            return ((MemorySegment)AllocationBase$vh.get(segment)).address();
        }

        @Override
        public long AllocationProtect() {
            return (long)AllocationProtect$vh.get(segment);
        }

        @Override
        public long RegionSize() {
            return (long)RegionSize$vh.get(segment);
        }

        @Override
        public long State() {
            return (long)State$vh.get(segment);
        }

        @Override
        public long Protect() {
            return (long)Protect$vh.get(segment);
        }

        @Override
        public long Type() {
            return (long)Type$vh.get(segment);
        }
    }

    static class JdkJobObjectBasicLimitInformation implements JobObjectBasicLimitInformation {
        private static final MemoryLayout layout = MemoryLayout.structLayout(
            paddingLayout(16),
            JAVA_INT,
            paddingLayout(16),
            JAVA_INT,
            paddingLayout(16)
        ).withByteAlignment(8);

        private static final VarHandle LimitFlags$vh = layout.varHandle(groupElement(1));
        private static final VarHandle ActiveProcessLimit$vh = layout.varHandle(groupElement(3));

        private final MemorySegment segment;

        JdkJobObjectBasicLimitInformation() {
            var arena = Arena.ofAuto();
            this.segment = arena.allocate(layout);
        }

        @Override
        public void setLimitFlags(int v) {
            LimitFlags$vh.set(segment, v);
        }

        @Override
        public void setActiveProcessLimit(int v) {
            ActiveProcessLimit$vh.set(segment, v);
        }
    }

    private final MemorySegment lastErrorState;

    JdkKernel32Library() {
        Arena arena = Arena.ofAuto();
        lastErrorState = arena.allocate(CAPTURE_GETLASTERROR_LAYOUT);
    }

    @Override
    public Handle GetCurrentProcess() {
        try {
            return new JdkHandle((MemorySegment) GetCurrentProcess$mh.invokeExact());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean CloseHandle(Handle handle) {
        assert handle instanceof JdkHandle;
        var jdkHandle = (JdkHandle) handle;
        try {
            return (boolean)CloseHandle$mh.invokeExact(jdkHandle.address, lastErrorState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetLastError() {
        return (int)GetLastError$vh.get(lastErrorState);
    }

    @Override
    public MemoryBasicInformation newMemoryBasicInformation() {
        return new JdkMemoryBasicInformation();
    }

    @Override
    public boolean VirtualLock(long address, long size) {
        try {
            return (boolean)VirtualLock$mh.invokeExact(MemorySegment.ofAddress(address), size, lastErrorState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int VirtualQueryEx(Handle process, long address, MemoryBasicInformation memoryInfo) {
        assert process instanceof JdkHandle;
        assert memoryInfo instanceof JdkMemoryBasicInformation;
        var jdkProcess = (JdkHandle) process;
        var jdkMemoryInfo = (JdkMemoryBasicInformation)memoryInfo;
        try {
            return (int)VirtualQueryEx$mh.invokeExact(
                jdkProcess.address,
                MemorySegment.ofAddress(address),
                jdkMemoryInfo.segment,
                jdkMemoryInfo.segment.byteSize(),
                lastErrorState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetProcessWorkingSetSize(Handle process, long minSize, long maxSize) {
        assert process instanceof JdkHandle;
        var jdkProcess = (JdkHandle) process;
        try {
            return (boolean)SetProcessWorkingSetSize$mh.invokeExact(jdkProcess.address, minSize, maxSize, lastErrorState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetCompressedFileSizeW(String lpFileName, IntConsumer lpFileSizeHigh) {
        try (Arena arena = Arena.ofConfined()) {
            // TODO: in Java 22 this can use allocateFrom to encode the string
            MemorySegment wideFileName = arena.allocateArray(JAVA_BYTE, (lpFileName + "\0").getBytes(StandardCharsets.UTF_16LE));
            MemorySegment fileSizeHigh = arena.allocate(JAVA_INT);

            int ret = (int)GetCompressedFileSizeW$mh.invokeExact(wideFileName, fileSizeHigh, lastErrorState);
            lpFileSizeHigh.accept(fileSizeHigh.get(JAVA_INT, 0));
            return ret;
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public int GetShortPathNameW(String lpszLongPath, char[] lpszShortPath, int cchBuffer) {
        try (Arena arena = Arena.ofConfined()) {
            // TODO: in Java 22 this can use allocateFrom to encode the string
            MemorySegment wideFileName = arena.allocateArray(JAVA_BYTE, (lpszLongPath + "\0").getBytes(StandardCharsets.UTF_16LE));
            MemorySegment shortPath = null;
            if (lpszShortPath != null) {
                shortPath = MemorySegment.ofArray(lpszShortPath);
            }

            return (int)GetShortPathNameW$mh.invokeExact(wideFileName, shortPath, cchBuffer, lastErrorState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetConsoleCtrlHandler(ConsoleCtrlHandler handler, boolean add) {
        Arena arena = Arena.ofAuto(); // auto arena so it lasts as long as the handler lasts
        MemorySegment nativeHandler = upcallStub(ConsoleCtrlHandler_handle$mh, handler, ConsoleCtrlHandler_handle$fd, arena);
        try {
            return (boolean)SetConsoleCtrlHandler$mh.invokeExact(nativeHandler, add);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public Handle CreateJobObjectW() {
        try {
            return new JdkHandle((MemorySegment) CreateJobObjectW$mh.invokeExact(null, null, lastErrorState));
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean AssignProcessToJobObject(Handle job, Handle process) {
        assert job instanceof JdkHandle;
        assert process instanceof JdkHandle;
        var jdkJob = (JdkHandle) job;
        var jdkProcess = (JdkHandle) process;

        try {
            return (boolean)AssignProcessToJobObject$mh.invokeExact(jdkJob.address, jdkProcess.address);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public JobObjectBasicLimitInformation newJobObjectBasicLimitInformation() {
        return new JdkJobObjectBasicLimitInformation();
    }

    @Override
    public boolean QueryInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JdkHandle;
        assert info instanceof JdkJobObjectBasicLimitInformation;
        var jdkJob = (JdkHandle) job;
        var jdkInfo = (JdkJobObjectBasicLimitInformation) info;

        try {
            return (boolean)QueryInformationJobObject$mh.invokeExact(
                jdkJob.address,
                infoClass,
                jdkInfo.segment,
                jdkInfo.segment.byteSize(),
                null);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean SetInformationJobObject(Handle job, int infoClass, JobObjectBasicLimitInformation info) {
        assert job instanceof JdkHandle;
        assert info instanceof JdkJobObjectBasicLimitInformation;
        var jdkJob = (JdkHandle) job;
        var jdkInfo = (JdkJobObjectBasicLimitInformation) info;

        try {
            return (boolean)SetInformationJobObject$mh.invokeExact(jdkJob.address, infoClass, jdkInfo.segment, jdkInfo.segment.byteSize());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
