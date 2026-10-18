export type WorkflowEventType =
  | 'WorkflowStarted'
  | 'StepScheduled'
  | 'StepCompleted'
  | 'StepFailed'
  | 'ValueRecorded'
  | 'TimerStarted'
  | 'TimerFired'
  | 'SignalWaiting'
  | 'SignalReceived'
  | 'WorkflowCompleted'
  | 'WorkflowFailed'
  | 'WorkflowCancelled';

export interface WorkflowEvent<TPayload = any> {
  workflowId: string;
  seq: number;
  stepSeq: number | null;
  type: WorkflowEventType;
  name: string | null;
  payload: TPayload;
  createdAt?: Date;
}

export interface NewWorkflowEvent<TPayload = any> {
  stepSeq: number | null;
  type: WorkflowEventType;
  name: string | null;
  payload: TPayload;
}