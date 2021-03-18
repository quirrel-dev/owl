const timerMaxLimit = 2147483647;

export class EggTimer {
  private currentTimer: NodeJS.Timeout | undefined;

  constructor(private readonly functionToCall: () => void) {}

  public setTimer(timestamp: number) {
    this.reset();

    const duration = Math.min(timestamp - Date.now(), timerMaxLimit);

    this.currentTimer = setTimeout(this.functionToCall, duration);
  }

  public reset() {
    if (this.currentTimer) {
      clearTimeout(this.currentTimer);
      this.currentTimer = undefined;
    }
  }
}
