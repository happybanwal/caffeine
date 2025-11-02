/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;

import com.google.errorprone.annotations.Var;

/**
 * A striped, non-blocking, bounded buffer.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <E> the type of elements maintained by this buffer
 */
final class BoundedBuffer<E> extends StripedBuffer<E> {
  /*
   * A circular ring buffer stores the elements being transferred by the producers to the consumer.
   * The monotonically increasing count of reads and writes allow indexing sequentially to the next
   * element location based upon a power-of-two sizing.
   *
   * The producers race to read the counts, check if there is available capacity, and if so then try
   * once to CAS to the next write count. If the increment is successful then the producer lazily
   * publishes the element. The producer does not retry or block when unsuccessful due to a failed
   * CAS or the buffer being full.
   *
   * The consumer reads the counts and takes the available elements. The clearing of the elements
   * and the next read count are lazily set.
   *
   * This implementation is striped to further increase concurrency by rehashing and dynamically
   * adding new buffers when contention is detected, up to an internal maximum. When rehashing in
   * order to discover an available buffer, the producer may retry adding its element to determine
   * whether it found a satisfactory buffer or if resizing is necessary.
   */

  /** The maximum number of elements per buffer. */
  static final int BUFFER_SIZE = 16;
  static final int MASK = BUFFER_SIZE - 1;

  @Override
  protected Buffer<E> create(E e) {
    return new RingBuffer<>(e);
  }

  static final class RingBuffer<E> extends BBHeader.ReadAndWriteCounterRef implements Buffer<E> {
    static final VarHandle BUFFER = MethodHandles.arrayElementVarHandle(Object[].class);

    final Object[] buffer;

    public RingBuffer(E e) {
      buffer = new Object[BUFFER_SIZE];
      BUFFER.set(buffer, 0, e);
      WRITE.set(this, 1);
    }

    @Override
    @SuppressWarnings("Varifier")
    public int offer(E e) {
      long head = readCounter;
      long tail = writeCounterOpaque();
      long size = (tail - head);
      if (size >= BUFFER_SIZE) {
        return Buffer.FULL;
      }
      if (casWriteCounter(tail, tail + 1)) {
        int index = (int) (tail & MASK);
        BUFFER.setRelease(buffer, index, e);
        return Buffer.SUCCESS;
      }
      return Buffer.FAILED;
    }

    @Override
    @SuppressWarnings("Varifier")
    public void drainTo(Consumer<E> consumer) {
      @Var long head = readCounter;
      long tail = writeCounterOpaque();
      long size = (tail - head);
      if (size == 0) {
        return;
      }
      do {
        int index = (int) (head & MASK);
        @SuppressWarnings("unchecked")
        var e = (E) BUFFER.getAcquire(buffer, index);
        if (e == null) {
          // not published yet
          break;
        }
        BUFFER.setRelease(buffer, index, null);
        consumer.accept(e);
        head++;
      } while (head != tail);
      setReadCounterOpaque(head);
    }

    @Override
    public long reads() {
      return readCounter;
    }

    @Override
    public long writes() {
      return writeCounter;
    }
  }
}

/** The namespace for field padding through inheritance. */
@SuppressWarnings({"MemberName", "MultiVariableDeclaration"})
final class BBHeader {

  private BBHeader() {}

  @SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
  abstract static class PadReadCounter {
    
    byte p000;
    byte p001;
    byte p002;
    byte p003;
    byte p004;
    byte p005;
    byte p006;
    byte p007;
    byte p008;
    byte p009;
    byte p010;
    byte p011;
    byte p012;
    byte p013;
    byte p014;
    byte p015;
    byte p016;
    byte p017;
    byte p018;
    byte p019;
    byte p020;
    byte p021;
    byte p022;
    byte p023;
    byte p024;
    byte p025;
    byte p026;
    byte p027;
    byte p028;
    byte p029;
    byte p030;
    byte p031;
    byte p032;
    byte p033;
    byte p034;
    byte p035;
    byte p036;
    byte p037;
    byte p038;
    byte p039;
    byte p040;
    byte p041;
    byte p042;
    byte p043;
    byte p044;
    byte p045;
    byte p046;
    byte p047;
    byte p048;
    byte p049;
    byte p050;
    byte p051;
    byte p052;
    byte p053;
    byte p054;
    byte p055;
    byte p056;
    byte p057;
    byte p058;
    byte p059;
    byte p060;
    byte p061;
    byte p062;
    byte p063;
    byte p064;
    byte p065;
    byte p066;
    byte p067;
    byte p068;
    byte p069;
    byte p070;
    byte p071;
    byte p072;
    byte p073;
    byte p074;
    byte p075;
    byte p076;
    byte p077;
    byte p078;
    byte p079;
    byte p080;
    byte p081;
    byte p082;
    byte p083;
    byte p084;
    byte p085;
    byte p086;
    byte p087;
    byte p088;
    byte p089;
    byte p090;
    byte p091;
    byte p092;
    byte p093;
    byte p094;
    byte p095;
    byte p096;
    byte p097;
    byte p098;
    byte p099;
    byte p100;
    byte p101;
    byte p102;
    byte p103;
    byte p104;
    byte p105;
    byte p106;
    byte p107;
    byte p108;
    byte p109;
    byte p110;
    byte p111;
    byte p112;
    byte p113;
    byte p114;
    byte p115;
    byte p116;
    byte p117;
    byte p118;
    byte p119;
  }

  /** Enforces a memory layout to avoid false sharing by padding the read count. */
  abstract static class ReadCounterRef extends PadReadCounter {
    volatile long readCounter;
  }

  abstract static class PadWriteCounter extends ReadCounterRef {
    byte p120, p121, p122, p123, p124, p125, p126, p127;
    byte p128, p129, p130, p131, p132, p133, p134, p135;
    byte p136, p137, p138, p139, p140, p141, p142, p143;
    byte p144, p145, p146, p147, p148, p149, p150, p151;
    byte p152, p153, p154, p155, p156, p157, p158, p159;
    byte p160, p161, p162, p163, p164, p165, p166, p167;
    byte p168, p169, p170, p171, p172, p173, p174, p175;
    byte p176, p177, p178, p179, p180, p181, p182, p183;
    byte p184, p185, p186, p187, p188, p189, p190, p191;
    byte p192, p193, p194, p195, p196, p197, p198, p199;
    byte p200, p201, p202, p203, p204, p205, p206, p207;
    byte p208, p209, p210, p211, p212, p213, p214, p215;
    byte p216, p217, p218, p219, p220, p221, p222, p223;
    byte p224, p225, p226, p227, p228, p229, p230, p231;
    byte p232, p233, p234, p235, p236, p237, p238, p239;
  }

  /** Enforces a memory layout to avoid false sharing by padding the write counter. */
  abstract static class ReadAndWriteCounterRef extends PadWriteCounter {
    static final VarHandle READ = findVarHandle(
        ReadCounterRef.class, "readCounter", long.class);
    static final VarHandle WRITE = findVarHandle(
        ReadAndWriteCounterRef.class, "writeCounter", long.class);

    volatile long writeCounter;

    void setReadCounterOpaque(long count) {
      READ.setOpaque(this, count);
    }

    long writeCounterOpaque() {
      return (long) WRITE.getOpaque(this);
    }

    boolean casWriteCounter(long expect, long update) {
      return WRITE.weakCompareAndSet(this, expect, update);
    }

    static VarHandle findVarHandle(Class<?> recv, String name, Class<?> type) {
      try {
        return MethodHandles.lookup().findVarHandle(recv, name, type);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }
}
