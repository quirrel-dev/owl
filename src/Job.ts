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