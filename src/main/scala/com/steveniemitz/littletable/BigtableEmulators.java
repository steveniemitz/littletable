package com.steveniemitz.littletable;

/**
 * A java-friendly helper for BigtableEmulator.newBuilder()
 */
public class BigtableEmulators {
  private BigtableEmulators() { }

  public static BigtableEmulator.Builder newBuilder() {
    return BigtableEmulator$.MODULE$.newBuilder();
  }
}
