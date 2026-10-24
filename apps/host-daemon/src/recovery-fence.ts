import { randomUUID } from "node:crypto";

export class RecoveryFence {
  private generations = new Map<string, number>();
  private tokens = new Map<string, string>();

  version(threadId: string): number {
    return this.generations.get(threadId) ?? 0;
  }

  inspect(threadId: string, present: boolean): string | null {
    this.tokens.delete(threadId);
    if (present) return null;
    const token = randomUUID();
    this.tokens.set(threadId, token);
    return token;
  }

  begin(threadId: string, token?: string): number {
    if (token !== undefined && this.tokens.get(threadId) !== token) {
      throw new Error("recovery_fence_lost");
    }
    this.tokens.delete(threadId);
    const generation = (this.generations.get(threadId) ?? 0) + 1;
    this.generations.set(threadId, generation);
    return generation;
  }

  assertCurrent(threadId: string, generation: number): void {
    if (this.version(threadId) !== generation)
      throw new Error("recovery_fence_lost");
  }
}
