/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/regexp.go

package com.steveniemitz.binaryre2j;

/**
 * MachineInput abstracts different representations of the input text supplied to the Machine. It
 * provides one-character lookahead.
 */
abstract class MachineInput {

  static final int EOF = (-1 << 3);

  static MachineInput fromBytes(byte[] b) {
    return new BinaryInput(b);
  }

  //// Interface

  // Returns the rune at the specified index; the units are
  // unspecified, but could be UTF-8 byte, UTF-16 char, or rune
  // indices.  Returns the width (in the same units) of the rune in
  // the lower 3 bits, and the rune (Unicode code point) in the high
  // bits.  Never negative, except for EOF which is represented as -1
  // << 3 | 0.
  abstract int step(int pos);

  // can we look ahead without losing info?
  abstract boolean canCheckPrefix();

  // Returns the index relative to |pos| at which |re2.prefix| is found
  // in this input stream, or a negative value if not found.
  abstract int index(RE2 re2, int pos);

  // Returns a bitmask of EMPTY_* flags.
  abstract int context(int pos);

  // Returns the end position in the same units as step().
  abstract int endPos();

  //// Implementations
  private static class BinaryInput extends MachineInput {
    final byte[] b;
    final int start;
    final int end;

    BinaryInput(byte[] b) {
      this.b = b;
      this.start = 0;
      this.end = b.length;
    }

    @Override
    int step(int pos) {
      pos += start;
      if (pos >= end) {
        return EOF;
      } else {
        int rune = b[pos] & 0xFF;
        return rune << 3 | 1;
      }
    }

    @Override
    boolean canCheckPrefix() {
      return true;
    }

    @Override
    int index(RE2 re2, int pos) {
      pos += start;
      int i = indexOf(b, re2.prefix, pos);
      return i < 0 ? i : i - pos;
    }

    private int indexOf(byte[] hayStack, byte[] needle, int pos) {
      return Utils.indexOf(hayStack, needle, pos);
    }

    @Override
    int context(int pos) {
      pos += start;
      int r1 = pos > start && pos <= end ? b[pos - 1] : -1;
      int r2 = pos < end ? b[pos] : -1;
      return Utils.emptyOpContext(r1, r2);
    }

    @Override
    int endPos() {
      return end;
    }
  }
}
