export class EggTimer {
  private currentTimer: NodeJS.Timeout | undefined;

  constructor(private readonly functionToCall: () => void) {}

  public setTimer(timestamp: number) {
    if (this.currentTimer) {
      clearTimeout(this.currentTimer);
    }

    this.currentTimer = setTimeout(this.functionToCall, timestamp - Date.now());
  }
}
