export interface Job<ScheduleType extends string = string> {
  id: string;
  queue: string;
  payload: string;

  runAt: Date;
  exclusive: boolean;

  schedule?: {
    type: ScheduleType;
    meta: string;
    times?: number;
  };

  count: number;
}

export interface JobEnqueue<ScheduleType extends string = string> {
  id: string;
  queue: string;
  payload: string;

  runAt?: Date;

  /**
   * If set to `true`, no other job on the same queue
   * will be executed at the same time as this job.
   */
  exclusive?: boolean;

  /**
   * Override if ID already exists
   */
  override?: boolean;

  /**
   * Retry a job on the specified schedule.
   * @example [ 10, 100, 1000 ] a job was scheduled for t=0ms. It fails, so it's scheduled for retry t=10ms. It fails again, so it's scheduled for retry at t=100ms, and so forth.
   */
  retry?: number[];

  /**
   * Optional: Schedule options.
   */
  schedule?: {
    /**
     * The type of the schedule.
     * Used by ScheduleKeeper to re-schedule
     * after enqueueing.
     */
    type: ScheduleType;

    /**
     * Metadata passed to ScheduleKeeper.
     */
    meta: string;

    /**
     * Maximum number of executions to be made.
     */
    times?: number;
  };
}
