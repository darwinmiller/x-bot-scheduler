import { DurableObject } from 'cloudflare:workers';
import { TaskService } from 'twitter-bot-shared-lib';
import { BotTaskDefinition, BotTaskExecution, TaskStatus } from 'twitter-bot-shared-lib';
import { PullMentionsTaskHandler } from '../tasks/pullMentionsTask';
import { ReplyTaskHandler } from '../tasks/replyTask';
import { TaskHandler } from '../tasks/taskHandler';
import { RateLimitService } from 'twitter-bot-shared-lib';
import { RateLimitTrackingRepository } from 'twitter-bot-shared-lib';
import { ApplicationSettingsRepository } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';
import chalk from 'chalk';
import { ReplyGenerationTaskHandler } from '../tasks/replyGenerationTask';
const taskHandlers: TaskHandler[] = [
    new PullMentionsTaskHandler(),
    new ReplyTaskHandler(),
    new ReplyGenerationTaskHandler()
];

/**
 * BotScheduler is a Durable Object that manages task scheduling and execution for a single bot.
 * It handles:
 * - Starting/stopping the scheduler
 * - Scheduling alarms for task execution
 * - Executing tasks when alarms trigger
 * - Managing task locks and retries
 * - Tracking rate limits
 * - Handling task dependencies and error thresholds
 */
export class BotScheduler extends DurableObject {
    public env: Env;
    private taskService: TaskService;
    private rateLimitService: RateLimitService;
    private isRunning: boolean = false;
    private currentAlarmTime: number | null = null;
    private botId: string | null = null;

    /**
     * Constructor initializes the scheduler with required services and state.
     * @param ctx - Durable Object state for persistence
     * @param env - Environment variables and services
     */
    constructor(ctx: DurableObjectState, env: Env) {
        super(ctx, env);
        this.env = env;
        this.taskService = new TaskService(env.DB);
        const rateLimitRepo = new RateLimitTrackingRepository(env.DB);
        // Initialize rateLimitService with empty botId, will be set when botId is available
        this.rateLimitService = new RateLimitService(rateLimitRepo, 'basic', '');

        // Initialize state from storage
        ctx.blockConcurrencyWhile(async () => {
            const state = await ctx.storage.get<{ botId: string, isRunning: boolean, currentAlarmTime: number | null }>('state');
            if (state) {
                this.botId = state.botId;
                this.isRunning = state.isRunning;
                this.currentAlarmTime = state.currentAlarmTime;
                console.log(chalk.blue(`🔄 BotScheduler initialized for bot ${this.botId}`));
            }
        });
    }

    /**
     * Main entry point for all requests to the Durable Object.
     * Handles:
     * - Bot ID validation and storage
     * - Routing to appropriate handlers (start/stop/status)
     * @param request - Incoming HTTP request
     * @returns Response with appropriate status and data
     */
    async fetch(request: Request): Promise<Response> {
        const url = new URL(request.url);
        const path = url.pathname;
        const [, botId, action] = path.match(/^\/bots\/([^\/]+)\/(start|stop|status)$/) || [];

        console.log(chalk.cyan(`📡 BotScheduler request: ${path}`));

        if (!botId) {
            console.log(chalk.red('❌ Bot ID is required'));
            return new Response(JSON.stringify({ error: 'Bot ID is required' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // Set the bot ID if not already set
        if (!this.botId) {
            console.log(chalk.green(`✨ Setting bot ID for the first time: ${botId}`));
            this.botId = botId;
            await this.ctx.storage.put('state', { botId, isRunning: this.isRunning, currentAlarmTime: this.currentAlarmTime });
        }
        // Verify the bot ID matches
        else if (botId !== this.botId) {
            console.log(chalk.red(`❌ Bot ID mismatch: requested ${botId}, stored ${this.botId}`));
            return new Response(JSON.stringify({ error: 'Bot ID mismatch' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // Initialize rate limit service with the correct botId
        this.rateLimitService = new RateLimitService(new RateLimitTrackingRepository(this.env.DB), 'basic', botId);

        switch (action) {
            case 'start':
                return this.handleStart(request);
            case 'stop':
                return this.handleStop(request);
            case 'status':
                return this.handleStatus(request);
            default:
                console.log(chalk.red(`❌ Unknown action: ${action}`));
                return new Response(JSON.stringify({ error: 'Not found' }), {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' }
                });
        }
    }

    /**
     * Handles starting the scheduler.
     * - Verifies scheduler isn't already running
     * - Sets running state
     * - Attempts to schedule next alarm
     * - If no tasks to schedule, turns scheduler back off
     */
    private async handleStart(request: Request): Promise<Response> {
        if (this.isRunning) {
            console.log(chalk.yellow(`⚠️ Scheduler is already running for bot ${this.botId}`));
            return new Response(JSON.stringify({ error: 'Scheduler is already running' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        this.isRunning = true;
        await this.ctx.storage.put('state', { botId: this.botId!, isRunning: true, currentAlarmTime: this.currentAlarmTime });
        const scheduled = await this.scheduleNextAlarm();

        // If no tasks were scheduled, turn the scheduler back off
        if (!scheduled) {
            this.isRunning = false;
            this.currentAlarmTime = null;
            await this.ctx.storage.put('state', { botId: this.botId!, isRunning: false, currentAlarmTime: null });
            console.log(chalk.yellow(`⚠️ No tasks to schedule for bot ${this.botId}`));
            return new Response(JSON.stringify({
                message: 'No tasks to schedule',
                isRunning: false
            }), {
                headers: { 'Content-Type': 'application/json' }
            });
        }

        console.log(chalk.green(`🚀 Started scheduler for bot ${this.botId}`));
        return new Response(JSON.stringify({
            message: 'Scheduler started',
            isRunning: true
        }), {
            headers: { 'Content-Type': 'application/json' }
        });
    }

    /**
     * Handles stopping the scheduler.
     * - Verifies scheduler is running
     * - Clears running state and alarm time
     * - Deletes any existing alarm
     */
    private async handleStop(request: Request): Promise<Response> {
        if (!this.isRunning) {
            console.log(chalk.yellow(`⚠️ Scheduler is not running for bot ${this.botId}`));
            return new Response(JSON.stringify({ error: 'Scheduler is not running' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        this.isRunning = false;
        this.currentAlarmTime = null;
        await this.ctx.storage.put('state', { botId: this.botId!, isRunning: false, currentAlarmTime: null });
        await this.ctx.storage.deleteAlarm();
        console.log(chalk.green(`🛑 Stopped scheduler for bot ${this.botId}`));
        return new Response(JSON.stringify({ message: 'Scheduler stopped' }), {
            headers: { 'Content-Type': 'application/json' }
        });
    }

    /**
     * Handles status check requests.
     * - Verifies alarm is actually set if scheduler is running
     * - Reschedules alarm if needed (self-healing)
     * - Returns current running state and next alarm time
     */
    private async handleStatus(request: Request): Promise<Response> {
        console.log(chalk.cyan(`📊 Status check for bot ${this.botId}`));

        // If bot is running and we have a next alarm time, verify the alarm is actually set
        if (this.isRunning && this.currentAlarmTime) {
            try {
                const alarmTime = await this.ctx.storage.getAlarm();
                if (!alarmTime) {
                    console.log(chalk.yellow(`⚠️ Alarm time exists (${new Date(this.currentAlarmTime).toISOString()}) but no alarm is set. Rescheduling...`));
                    await this.scheduleNextAlarm();
                }
            } catch (error) {
                console.log(chalk.red(`❌ Error checking alarm status: ${error}`));
                // If we can't check the alarm status, try to reschedule anyway
                await this.scheduleNextAlarm();
            }
        }

        return new Response(JSON.stringify({
            isRunning: this.isRunning,
            nextAlarmTime: this.currentAlarmTime
        }), {
            headers: { 'Content-Type': 'application/json' }
        });
    }

    /**
     * Schedules the next alarm based on task definitions.
     * - Gets all active tasks for the bot
     * - Priority and race period are handled at the repository level
     * - For each task:
     *   - Uses next_planned_run_at if set
     *   - Otherwise checks execution history to determine next run time
     *   - If no history, uses lead time from settings
     * - Sets alarm for the earliest scheduled time
     * @returns boolean indicating if an alarm was scheduled
     */
    private async scheduleNextAlarm(): Promise<boolean> {
        if (!this.isRunning || !this.botId) return false;

        // Get active tasks for this bot using the stored bot ID
        const tasks = await this.taskService.getActiveTaskDefinitionsByBot(this.botId);
        if (tasks.length === 0) {
            console.log(chalk.yellow(`⚠️ No active tasks found for bot ${this.botId}`));
            return false;
        }

        // Find the next task to run
        const now = new Date();
        const nextTask = await tasks.reduce(async (earliestPromise: Promise<Date | null>, task: BotTaskDefinition) => {
            const earliest = await earliestPromise;

            // If task has a planned run time, use that
            if (task.next_planned_run_at) {
                const taskTime = new Date(task.next_planned_run_at);
                if (!earliest || taskTime < earliest) return taskTime;
                console.log(chalk.blue(`⏰ Task ${task.id} has a planned run time of ${taskTime.toISOString()}`));
                return earliest;
            }

            // For tasks without a planned run time, check execution history
            const executions = await this.taskService.getTaskExecutionsByDefinitionId(task.id);
            if (executions.length > 0) {
                const lastExecution = executions[0];
                const lastStartedAt = new Date(lastExecution.started_at);
                const timeSinceLastRun = now.getTime() - lastStartedAt.getTime();
                const intervalMs = task.interval_minutes * 60 * 1000;

                if (timeSinceLastRun < intervalMs) {
                    // Schedule for remaining time in current interval
                    const nextRunTime = new Date(lastStartedAt.getTime() + intervalMs);
                    if (!earliest || nextRunTime < earliest) {
                        console.log(chalk.blue(`⏰ Task ${task.id} has a next run time of ${nextRunTime.toISOString()}`));
                        return nextRunTime;
                    }
                }
            }

            // If no execution history or outside interval, get lead time from settings
            const settingsRepo = new ApplicationSettingsRepository(this.env.DB);
            const leadTimeSetting = await settingsRepo.getByKey('scheduler-new-task-lead-seconds');
            const leadSeconds = leadTimeSetting ? parseInt(leadTimeSetting.value) : 60; // Default to 60 seconds if not set

            const nextRunTime = new Date(now.getTime() + (leadSeconds * 1000));
            if (!earliest || nextRunTime < earliest) {
                console.log(chalk.blue(`⏰ Fresh task ${task.id} has a run time of ${nextRunTime.toISOString()}`));
                return nextRunTime;
            }

            return earliest;
        }, Promise.resolve(null));

        if (!nextTask) {
            console.log(chalk.yellow(`⚠️ No tasks scheduled for bot ${this.botId}`));
            return false;
        }

        // Schedule the alarm
        const alarmTime = nextTask.getTime();
        await this.ctx.storage.setAlarm(alarmTime);
        this.currentAlarmTime = alarmTime;
        await this.ctx.storage.put('state', { botId: this.botId!, isRunning: this.isRunning, currentAlarmTime: alarmTime });
        console.log(chalk.green(`⏰ Scheduled next alarm for bot ${this.botId} at ${new Date(alarmTime).toISOString()}`));
        return true;
    }

    /**
     * Handles alarm triggers.
     * - Generates unique worker ID for this execution
     * - Cleans up expired locks
     * - Gets all due tasks
     * - For each task:
     *   - Acquires lock
     *   - Checks rate limits
     *   - Creates execution record
     *   - Executes task
     *   - Updates execution status
     *   - Handles retries and failures
     * - Schedules next alarm
     */
    async alarm(): Promise<void> {
        if (!this.isRunning) return;

        const botId = this.botId;
        if (!botId) {
            console.log(chalk.red('❌ Alarm triggered but bot ID is not set'));
            return;
        }

        this.rateLimitService = new RateLimitService(new RateLimitTrackingRepository(this.env.DB), 'basic', botId);
        const workerId = crypto.randomUUID();
        console.log(chalk.cyan(`🔔 Alarm triggered for bot ${botId}`));

        try {
            // Cleanup any expired locks first
            await this.taskService.cleanupExpiredLocks();

            // Get all due tasks for this bot
            const dueTasks = await this.taskService.getDueTasksByBot(botId, new Date());
            console.log(chalk.blue(`📋 Found ${dueTasks.length} due tasks for bot ${botId}`));

            // Process each task
            for (const task of dueTasks) {
                console.log(chalk.cyan(`🔄 Processing task ${task.id} (${task.task_type}) for bot ${botId}`));

                // Find appropriate handler
                const handler = taskHandlers.find(h => h.canHandle(task));
                if (!handler) {
                    console.log(chalk.red(`❌ No handler found for task type ${task.task_type}`));
                    continue;
                }

                // Try to acquire lock
                if (!(await this.taskService.acquireTaskLock(task.id, workerId, 5))) {
                    console.log(chalk.yellow(`⚠️ Could not acquire lock for task ${task.id}`));
                    continue;
                }

                let execution: BotTaskExecution | null = null;
                try {
                    // Get current rate limit stats if endpoint is specified
                    let rateLimitRemaining = 0;
                    if (task.rate_limit_endpoint) {
                        const stats = await this.rateLimitService.getUsageStats(task.rate_limit_endpoint);
                        rateLimitRemaining = stats.limit - stats.current;
                    }

                    // Create execution
                    execution = await this.taskService.createTaskExecution({
                        bot_id: botId,
                        task_definition_id: task.id,
                        status: 'started' as TaskStatus,
                        started_at: new Date(),
                        completed_at: null,
                        duration_ms: 0,
                        attempt_number: 1,
                        rate_limit_remaining: rateLimitRemaining,
                        worker_id: workerId,
                        is_locked: true,
                        lock_expires_at: new Date(Date.now() + 5 * 60 * 1000),
                        result_data: null,
                        error_message: null,
                        error_stack: null
                    });

                    // Execute task
                    await handler.execute(this.env, botId, task, execution, this.taskService);

                    // Update execution status
                    if (execution) {
                        // Get updated rate limit stats if endpoint is specified
                        let updatedRateLimitRemaining = 0;
                        if (task.rate_limit_endpoint) {
                            const stats = await this.rateLimitService.getUsageStats(task.rate_limit_endpoint);
                            console.log(chalk.blue(`🔄 Rate limit stats for task ${task.id}:`, JSON.stringify(stats)));
                            updatedRateLimitRemaining = stats.limit - stats.current;
                        }

                        const startedAt = new Date(execution.started_at);
                        await this.taskService.updateTaskExecution(execution.id, {
                            status: 'completed' as TaskStatus,
                            completed_at: new Date(),
                            duration_ms: Date.now() - startedAt.getTime(),
                            rate_limit_remaining: updatedRateLimitRemaining,
                            is_locked: false,
                            lock_expires_at: null
                        });

                        // Update task definition with successful run
                        await this.taskService.updateTaskDefinition(task.id, {
                            last_successful_run_at: new Date(),
                            next_planned_run_at: new Date(Date.now() + task.interval_minutes * 60 * 1000)
                        });
                    }

                    console.log(chalk.green(`✅ Successfully completed task ${task.id}`));
                } catch (error) {
                    console.log(chalk.red(`❌ Error processing task ${task.id}:`), error);

                    if (execution) {
                        // Get current rate limit stats if endpoint is specified
                        let rateLimitRemaining = 0;
                        if (task.rate_limit_endpoint) {
                            const stats = await this.rateLimitService.getUsageStats(task.rate_limit_endpoint);
                            rateLimitRemaining = stats.limit - stats.current;
                        }

                        // Check if we should retry
                        if (execution.attempt_number < (task.max_retries || 3)) {
                            // Calculate retry delay (default to 5 minutes if not specified)
                            const retryDelayMinutes = task.retry_delay_minutes || 5;
                            const nextRetryTime = new Date(Date.now() + retryDelayMinutes * 60 * 1000);

                            const startedAt = new Date(execution.started_at);
                            // Update execution status to retrying
                            await this.taskService.updateTaskExecution(execution.id, {
                                status: 'retrying' as TaskStatus,
                                completed_at: new Date(),
                                duration_ms: Date.now() - startedAt.getTime(),
                                rate_limit_remaining: rateLimitRemaining,
                                is_locked: false,
                                lock_expires_at: null,
                                error_message: error instanceof Error ? error.message : String(error),
                                error_stack: error instanceof Error ? error.stack || null : null
                            });

                            // Update task definition with retry time
                            await this.taskService.updateTaskDefinition(task.id, {
                                next_planned_run_at: nextRetryTime
                            });

                            console.log(chalk.yellow(`🔄 Task ${task.id} will retry at ${nextRetryTime.toISOString()}`));
                        } else {
                            // Max retries exceeded, mark as failed
                            const startedAt = new Date(execution.started_at);
                            await this.taskService.updateTaskExecution(execution.id, {
                                status: 'failed' as TaskStatus,
                                completed_at: new Date(),
                                duration_ms: Date.now() - startedAt.getTime(),
                                rate_limit_remaining: rateLimitRemaining,
                                is_locked: false,
                                lock_expires_at: null,
                                error_message: error instanceof Error ? error.message : String(error),
                                error_stack: error instanceof Error ? error.stack || null : null
                            });

                            // Check if we should pause the task due to error threshold
                            if (task.error_threshold) {
                                const recentFailures = await this.taskService.getFailedTaskExecutionsByBot(botId, task.error_threshold);
                                if (recentFailures.length >= task.error_threshold) {
                                    await this.taskService.updateTaskDefinition(task.id, {
                                        is_active: false
                                    });
                                    console.log(chalk.red(`⏸️ Task ${task.id} paused due to exceeding error threshold`));
                                }
                            }
                        }
                    }
                } finally {
                    // Always release the lock
                    await this.taskService.releaseTaskLock(task.id);
                }
            }
        } catch (error) {
            console.log(chalk.red('❌ Error in alarm handler:'), error);
        } finally {
            // Schedule the next alarm
            await this.scheduleNextAlarm();
        }
    }
}