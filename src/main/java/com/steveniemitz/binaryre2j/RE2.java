/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/regexp.go

// Beware, submatch results may pin a large underlying String into
// memory.  Consider creating explicit string copies if submatches are
// long-lived and inputs are large.
//
// The JDK API supports incremental processing of the input without
// necessarily consuming it all; we do not attempt to do so.

// The Java API emphasises UTF-16 Strings, not UTF-8 byte[] as in Go, as
// the primary input datatype, and the method names have been changed to
// reflect this.

package com.steveniemitz.binaryre2j;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

/**
 * An RE2 class instance is a compiled representation of an RE2 regular expression, independent of
 * the public Java-like Pattern/Matcher API.
 *
 * <p>
 * This class also contains various implementation helpers for RE2 regular expressions.
 *
 * <p>
 * See the {@code Matcher} and {@code Pattern} classes for the public API, and the <a
 * href='package.html'>package-level documentation</a> for an overview of how to use this API.
 */
public class RE2 {

  // (In the Go implementation this structure is just called "Regexp".)

  //// Parser flags.

  // Fold case during matching (case-insensitive).
  static final int FOLD_CASE = 0x01;

  // Treat pattern as a literal string instead of a regexp.
  static final int LITERAL = 0x02;

  // Allow character classes like [^a-z] and [[:space:]] to match newline.
  static final int CLASS_NL = 0x04;

  // Allow '.' to match newline.
  static final int DOT_NL = 0x08;

  // Treat ^ and $ as only matching at beginning and end of text, not
  // around embedded newlines.  (Perl's default).
  static final int ONE_LINE = 0x10;

  // Make repetition operators default to non-greedy.
  static final int NON_GREEDY = 0x20;

  // allow Perl extensions:
  //   non-capturing parens - (?: )
  //   non-greedy operators - *? +? ?? {}?
  //   flag edits - (?i) (?-i) (?i: )
  //     i - FoldCase
  //     m - !OneLine
  //     s - DotNL
  //     U - NonGreedy
  //   line ends: \A \z
  //   \Q and \E to disable/enable metacharacters
  //   (?P<name>expr) for named captures
  // \C (any byte) is not supported.
  static final int PERL_X = 0x40;

  // Allow \p{Han}, \P{Han} for Unicode group and negation.
  static final int UNICODE_GROUPS = 0x80;

  // Regexp END_TEXT was $, not \z.  Internal use only.
  static final int WAS_DOLLAR = 0x100;

  static final int MATCH_NL = CLASS_NL | DOT_NL;

  // As close to Perl as possible.
  static final int PERL = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;

  // POSIX syntax.
  static final int POSIX = 0;

  //// Anchors
  static final int UNANCHORED = 0;
  static final int ANCHOR_START = 1;
  static final int ANCHOR_BOTH = 2;

  //// RE2 instance members.

  final String expr; // as passed to Compile
  final Prog prog; // compiled program
  final int cond; // EMPTY_* bitmask: empty-width conditions
  // required at start of match
  final int numSubexp;
  boolean longest;

  byte[] prefix; // required UTF-8 prefix in unanchored matches
  boolean prefixComplete; // true iff prefix is the entire regexp

  // Cache of machines for running regexp.
  // Accesses must be serialized using |this| monitor.
  // @GuardedBy("this")
  private final Queue<Machine> machine = new ArrayDeque<Machine>();
  public Map<String, Integer> namedGroups;

  // This is visible for testing.
  RE2(String expr) {
    RE2 re2 = RE2.compile(expr);
    // Copy everything.
    this.expr = re2.expr;
    this.prog = re2.prog;
    this.cond = re2.cond;
    this.numSubexp = re2.numSubexp;
    this.longest = re2.longest;
    this.prefix = re2.prefix;
    this.prefixComplete = re2.prefixComplete;
  }

  private RE2(String expr, Prog prog, int numSubexp, boolean longest) {
    this.expr = expr;
    this.prog = prog;
    this.numSubexp = numSubexp;
    this.cond = prog.startCond();
    this.longest = longest;
  }

  public static RE2 compile(byte[] expr) throws PatternSyntaxException {
    return compile(new String(expr, StandardCharsets.ISO_8859_1));
  }

  /**
   * Parses a regular expression and returns, if successful, an {@code RE2} instance that can be
   * used to match against text.
   *
   * <p>
   * When matching against text, the regexp returns a match that begins as early as possible in the
   * input (leftmost), and among those it chooses the one that a backtracking search would have
   * found first. This so-called leftmost-first matching is the same semantics that Perl, Python,
   * and other implementations use, although this package implements it without the expense of
   * backtracking.
   */
  public static RE2 compile(String expr) throws PatternSyntaxException {
    return compileImpl(expr, PERL, /*longest=*/ false);
  }

  // Exposed to ExecTests.
  static RE2 compileImpl(String expr, int mode, boolean longest) throws PatternSyntaxException {
    Regexp re = Parser.parse(expr, mode);
    int maxCap = re.maxCap(); // (may shrink during simplify)
    re = Simplify.simplify(re);
    Prog prog = Compiler.compileRegexp(re);
    RE2 re2 = new RE2(expr, prog, maxCap, longest);
    ByteArrayOutputStream prefixBuilder = new ByteArrayOutputStream();
    re2.prefixComplete = prog.prefix(prefixBuilder);
    re2.prefix = prefixBuilder.toByteArray();
    re2.namedGroups = re.namedGroups;
    return re2;
  }

  /**
   * Returns the number of parenthesized subexpressions in this regular expression.
   */
  int numberOfCapturingGroups() {
    return numSubexp;
  }

  // get() returns a machine to use for matching |this|.  It uses |this|'s
  // machine cache if possible, to avoid unnecessary allocation.
  Machine get() {
    synchronized (this) {
      if (!machine.isEmpty()) {
        return machine.remove();
      }
    }
    return new Machine(this);
  }

  // Clears the memory associated with this machine.
  synchronized void reset() {
    machine.clear();
  }

  // put() returns a machine to |this|'s machine cache.  There is no attempt to
  // limit the size of the cache, so it will grow to the maximum number of
  // simultaneous matches run using |this|.  (The cache empties when |this|
  // gets garbage collected.)
  synchronized void put(Machine m) {
    machine.add(m);
  }

  @Override
  public String toString() {
    return expr;
  }

  // doExecute() finds the leftmost match in the input and returns
  // the position of its subexpressions.
  // Derived from exec.go.
  private int[] doExecute(MachineInput in, int pos, int anchor, int ncap) {
    Machine m = get();
    m.init(ncap);
    int[] cap = m.match(in, pos, anchor) ? m.submatches() : null;
    put(m);
    return cap;
  }

  public boolean matchBinary(byte[] b) {
    return doExecute(MachineInput.fromBytes(b), 0, UNANCHORED, 0) != null;
  }

}
