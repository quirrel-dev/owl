export function computeTimestampForNextRetry(
  currentRunAt: Date,
  retryIntervals: number[],
  currentTry: number
): number | undefined {
  if (currentTry > retryIntervals.length) {
    return undefined;
  }

  const durationOfPreviousRun = retryIntervals[currentTry - 2] ?? 0;
  return +currentRunAt + retryIntervals[currentTry - 1] - durationOfPreviousRun;
}
