export interface Job<ScheduleType extends string = string> {
  id: string;
  queue: string;
  payload: string;

  runAt: Date;
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
   * Override if ID already exists
   */
  override?: boolean;

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
