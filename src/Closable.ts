export interface Closable {
  /**
   * Closes all existing connections,
   * waits until all pending stuff
   * is successfully stopped.
   */
  close(): Promise<void>;
}