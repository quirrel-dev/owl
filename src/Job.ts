export interface Job {
  id: string;
  queue: string;
  payload: string;
}

export interface JobEnqueue {
  id: string;
  queue: string;
  payload: string;
}

export interface JobSchedule<ScheduleType extends string> extends JobEnqueue {
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
