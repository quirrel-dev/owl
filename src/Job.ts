export interface Job<ScheduleType extends string = string> {
  id: string;
  queue: string;
  payload: string;

  runAt: Date;
  schedule?: {
    type: ScheduleType;
    meta: string;
  };
}

export interface JobEnqueue<ScheduleType extends string = string> {
  id: string;
  queue: string;
  payload: string;

  runAt?: Date;

  upsert?: boolean;

  /**
   * Optional: Scheduled data.
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
  };

  /**
   * Maximum number of executions to be made.
   */
  times?: number;
}
