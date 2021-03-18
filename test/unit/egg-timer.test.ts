import { expect } from "chai";
import { EggTimer } from "../../src/worker/egg-timer";

describe("EggTimer", () => {
  describe("when set multiple times", () => {
    it("triggers only once", (done) => {
      const startTime = Date.now();

      const eggTimer = new EggTimer(() => {
        const endTime = Date.now();

        expect(endTime - startTime).to.be.closeTo(50, 10);
        done();
      });

      eggTimer.setTimer(Date.now() + 100);
      eggTimer.setTimer(Date.now() + 200);
      eggTimer.setTimer(Date.now() + 50);
    });
  });

  describe("when set for > 32 bit", () => {
    it("will time for max timeout instead", () => {
      process.on("warning", (warning) => {
        expect(warning).to.be.null
      })

      const eggTimer = new EggTimer(() => {});

      eggTimer.setTimer(Date.now() + 3147483647);
    });
  });
});
