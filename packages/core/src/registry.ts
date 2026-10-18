import { WorkflowFunction } from './replay';

export class WorkflowRegistry {
  private readonly workflows = new Map<string, WorkflowFunction<any, any>>();

  register<TInput = any, TOutput = any>(
    type: string,
    fn: WorkflowFunction<TInput, TOutput>,
  ): void {
    if (this.workflows.has(type)) {
      throw new Error(`Workflow type "${type}" is already registered.`);
    }
    this.workflows.set(type, fn);
  }

  get(type: string): WorkflowFunction<any, any> {
    const fn = this.workflows.get(type);
    if (!fn) {
      throw new Error(`Unknown workflow type "${type}".`);
    }
    return fn;
  }

  has(type: string): boolean {
    return this.workflows.has(type);
  }
}