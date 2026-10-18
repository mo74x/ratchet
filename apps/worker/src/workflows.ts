import { WorkflowContext, WorkflowRegistry } from '@ratchet/core';

export interface FiveStepInput {
  orderId: string;
}

export function createDefaultRegistry(
  stepSideEffectHook?: (
    stepName: string,
    stepSeq: number,
    idempotencyKey: string,
  ) => void | Promise<void>,
): WorkflowRegistry {
  const registry = new WorkflowRegistry();

  registry.register(
    'five-step-pipeline',
    async (ctx: WorkflowContext, input: FiveStepInput) => {
      const s1 = await ctx.step('validate-order', async (key: string) => {
        await stepSideEffectHook?.('validate-order', 1, key);
        return `validated:${input.orderId}`;
      });

      const s2 = await ctx.step('reserve-inventory', async (key: string) => {
        await stepSideEffectHook?.('reserve-inventory', 2, key);
        return `${s1}->reserved`;
      });

      const s3 = await ctx.step('charge-payment', async (key: string) => {
        await stepSideEffectHook?.('charge-payment', 3, key);
        return `${s2}->charged`;
      });

      const s4 = await ctx.step('generate-invoice', async (key: string) => {
        await stepSideEffectHook?.('generate-invoice', 4, key);
        return `${s3}->invoiced`;
      });

      const s5 = await ctx.step('dispatch-shipment', async (key: string) => {
        await stepSideEffectHook?.('dispatch-shipment', 5, key);
        return `${s4}->shipped`;
      });

      return { finalState: s5 };
    },
  );

  registry.register(
    'onboard-customer',
    async (
      ctx: WorkflowContext,
      input: { email: string; welcomeDelay?: string; kycTimeout?: string },
    ) => {
      const account = await ctx.step('create-account', async (key) => {
        await stepSideEffectHook?.('create-account', 1, key);
        return { id: `acc_${input.email.split('@')[0]}`, email: input.email };
      });

      await ctx.step('send-welcome', async (key) => {
        await stepSideEffectHook?.('send-welcome', 2, key);
        return { sentTo: account.id };
      });

      await ctx.sleep((input.welcomeDelay ?? '3d') as any);

      const approved = await ctx.waitForSignal<{ approvedBy: string }>(
        'kyc-approved',
        { timeout: (input.kycTimeout ?? '7d') as any },
      );

      if (!approved) {
        return ctx.fail('KYC timeout');
      }

      await ctx.step('activate', async (key) => {
        await stepSideEffectHook?.('activate', 5, key);
        return { activated: true, approvedBy: approved.approvedBy };
      });

      return {
        accountId: account.id,
        status: 'active',
        approvedBy: approved.approvedBy,
      };
    },
  );

  return registry;
}
