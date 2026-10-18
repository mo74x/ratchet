import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Post,
} from '@nestjs/common';
import { RatchetRepository } from '@ratchet/store';

export interface StartWorkflowDto {
  type: string;
  input?: Record<string, unknown>;
  idempotencyKey?: string;
}

export interface SendSignalDto {
  name: string;
  payload?: unknown;
}
@Controller('workflows')
export class WorkflowsController {
  constructor(private readonly repo: RatchetRepository) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async startWorkflow(@Body() body: StartWorkflowDto) {
    if (!body?.type || typeof body.type !== 'string') {
      throw new BadRequestException('Field "type" is required.');
    }

    const workflow = await this.repo.startWorkflow({
      type: body.type,
      input: body.input ?? {},
      idempotencyKey: body.idempotencyKey,
    });

    return workflow;
  }

  @Get(':id')
  async getWorkflow(@Param('id') id: string) {
    const workflow = await this.repo.getWorkflow(id);
    if (!workflow) {
      throw new NotFoundException(`Workflow ${id} not found.`);
    }
    return workflow;
  }

  @Get(':id/history')
  async getWorkflowHistory(@Param('id') id: string) {
    const workflow = await this.repo.getWorkflow(id);
    if (!workflow) {
      throw new NotFoundException(`Workflow ${id} not found.`);
    }
    const events = await this.repo.getWorkflowHistory(id);
    return {
      workflowId: workflow.id,
      status: workflow.status,
      events,
    };
  }
  @Post(':id/signal')
  @HttpCode(HttpStatus.OK)
  async sendSignal(@Param('id') id: string, @Body() body: SendSignalDto) {
    if (!body?.name || typeof body.name !== 'string') {
      throw new BadRequestException('Signal "name" is required.');
    }

    const workflow = await this.repo.getWorkflow(id);
    if (!workflow) {
      throw new NotFoundException(`Workflow ${id} not found.`);
    }

    const res = await this.repo.sendSignal({
      workflowId: id,
      name: body.name,
      payload: body.payload ?? null,
    });

    return {
      workflowId: id,
      signalId: res.signalId,
      deliveredImmediately: res.deliveredImmediately,
    };
  }
}