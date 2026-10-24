import { expect, it } from "vitest";
import { RecoveryFence } from "./recovery-fence.js";
it("never grants recovery while any runtime exists", () => {
  expect(new RecoveryFence().inspect("thread", true)).toBeNull();
});
it("consumes a token exactly once", () => {
  const fence = new RecoveryFence();
  const token = fence.inspect("thread", false)!;
  const generation = fence.begin("thread", token);
  expect(() => fence.begin("thread", token)).toThrow("recovery_fence_lost");
  fence.assertCurrent("thread", generation);
});
it.each(["stop", "new-input", "archive", "cancel", "goal-pause"])(
  "fences a delayed recovery after %s",
  () => {
    const fence = new RecoveryFence();
    const token = fence.inspect("thread", false)!;
    const generation = fence.begin("thread", token);
    fence.begin("thread");
    expect(() => fence.assertCurrent("thread", generation)).toThrow(
      "recovery_fence_lost",
    );
  },
);
it("does not accept a probe token from another daemon process", () => {
  const token = new RecoveryFence().inspect("thread", false)!;
  expect(() => new RecoveryFence().begin("thread", token)).toThrow(
    "recovery_fence_lost",
  );
});
it("a second worker's probe invalidates the first probe", () => {
  const fence = new RecoveryFence();
  const token = fence.inspect("thread", false)!;
  fence.inspect("thread", false);
  expect(() => fence.begin("thread", token)).toThrow("recovery_fence_lost");
});
