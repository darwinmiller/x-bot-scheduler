import { DurableObject } from 'cloudflare:workers';
import { TaskService } from 'twitter-bot-shared-lib';
import { BotTaskDefinition, BotTaskExecution, TaskStatus } from 'twitter-bot-shared-lib';
import { RateLimitService } from 'twitter-bot-shared-lib';
import { RateLimitTrackingRepository } from 'twitter-bot-shared-lib';
import { ApplicationSettingsRepository } from 'twitter-bot-shared-lib';
import { WebSocketMessage, ConnectionAckMessage, LogMessage, EntityUpdateNotification, SchedulerStatusUpdateMessage } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';
import chalk from 'chalk';
import { initializeTaskHandlers, taskHandlers } from '../tasks/taskHandlers';

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

        // Initialize task handlers
        initializeTaskHandlers(env);

        // Initialize state from storage
        ctx.blockConcurrencyWhile(async () => {
            const state = await ctx.storage.get<{ botId: string, isRunning: boolean, currentAlarmTime: number | null }>('state');
            if (state) {
                this.botId = state.botId;
                this.isRunning = state.isRunning;
                this.currentAlarmTime = state.currentAlarmTime;
                this.log('info', `🔄 BotScheduler (re)initialized for bot ${this.botId}`);
                // Consider sending an initial status update if WebSockets could be connected before fetch sets botId
                // However, botId is crucial for targeted messages, so usually it's fine after fetch.
            } else {
                // If state is null, it might be the first time. botId will be set in fetch.
                // Avoid logging with this.botId here as it might be null.
                console.log(chalk.blue(`🔄 BotScheduler first-time initialization or no stored state.`));
            }
        });
    }

    private broadcast(message: WebSocketMessage) {
        if (!this.botId) return; // Don't broadcast if botId isn't set

        const sockets = this.ctx.getWebSockets();
        if (sockets.length > 0) {
            const serializedMessage = JSON.stringify(message);
            sockets.forEach(ws => {
                try {
                    ws.send(serializedMessage);
                } catch (e) {
                    console.error(chalk.red(`[Bot ${this.botId}] Failed to send to WebSocket:`), e);
                }
            });
        }
    }

    public log(level: LogMessage['level'], content: string) {
        // Ensure botId is available before logging via WebSocket
        const currentBotId = this.botId || 'UNKNOWN_BOT';

        const logMessage: LogMessage = {
            type: 'log',
            id: crypto.randomUUID(),
            timestamp: Date.now(),
            level,
            content,
            botId: currentBotId,
        };
        // Standard console logging (retains chalk for server-side visibility)
        //console.log(chalk.magenta(`[WS LOG][Bot ${currentBotId}] [${level.toUpperCase()}] ${content}`));

        // Broadcast only if botId is known (to avoid sending logs for uninitialized DOs if any client connects too early)
        if (this.botId) {
            this.broadcast(logMessage);
        }
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

        // Check for WebSocket upgrade request first
        const upgradeHeader = request.headers.get('Upgrade');
        if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
            if (!this.botId) {
                // This case should ideally be prevented by the proxying worker ensuring botId is established first
                console.log(chalk.red('❌ WebSocket connection denied: Bot ID not yet established for this Durable Object instance.'));
                // No this.log here as botId isn't set reliably yet for a WebSocket-specific log message
                return new Response('Bot ID not established. Cannot accept WebSocket.', { status: 400 });
            }

            const pair = new WebSocketPair();
            const [client, server] = Object.values(pair);

            await this.ctx.acceptWebSocket(server);

            // Send an acknowledgment message including initial scheduler status
            const ackMsg: ConnectionAckMessage = {
                type: 'connection_ack',
                message: `WebSocket connection established for bot ${this.botId}`,
                botId: this.botId,
                initialSchedulerStatus: {
                    isRunning: this.isRunning,
                    currentAlarmTime: this.currentAlarmTime,
                }
            };
            server.send(JSON.stringify(ackMsg));

            return new Response(null, {
                status: 101, // Switching Protocols
                webSocket: client,
            });
        }

        // Existing HTTP endpoint logic
        const [, botIdFromPath, action] = path.match(/^\/bots\/([^\/]+)\/(start|stop|status)$/) || [];

        console.log(chalk.cyan(`📡 BotScheduler HTTP request: ${path}`));

        if (!botIdFromPath) {
            console.log(chalk.red('❌ Bot ID is required'));
            this.log('error', 'HTTP request received without Bot ID in path.');
            return new Response(JSON.stringify({ error: 'Bot ID is required' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // Set the bot ID if not already set
        if (!this.botId) {
            console.log(chalk.green(`✨ Setting bot ID for the first time: ${botIdFromPath}`));
            this.botId = botIdFromPath;
            await this.ctx.storage.put('state', { botId: this.botId!, isRunning: this.isRunning, currentAlarmTime: this.currentAlarmTime });
            this.log('info', `✨ Bot ID set for the first time: ${this.botId}`);
            this.notifySchedulerStatusUpdate(); // Notify status since botId is now set
        }
        // Verify the bot ID matches
        else if (botIdFromPath !== this.botId) {
            console.log(chalk.red(`❌ Bot ID mismatch: requested ${botIdFromPath}, stored ${this.botId}`));
            this.log('error', `HTTP Bot ID mismatch: requested ${botIdFromPath}, stored ${this.botId}`);
            return new Response(JSON.stringify({ error: 'Bot ID mismatch' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // Initialize rate limit service with the correct botId
        this.rateLimitService = new RateLimitService(new RateLimitTrackingRepository(this.env.DB), 'basic', this.botId);

        switch (action) {
            case 'start':
                return this.handleStart(request);
            case 'stop':
                return this.handleStop(request);
            case 'status':
                return this.handleStatus(request);
            default:
                console.log(chalk.red(`❌ Unknown action: ${action}`));
                this.log('error', `Unknown HTTP action received: ${action} for bot ${this.botId}`);
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
            this.log('warn', 'Start command received but scheduler already running.');
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
            this.log('warn', 'Scheduler started but no tasks found, stopping immediately.');
            this.notifySchedulerStatusUpdate(); // Notify status change
            return new Response(JSON.stringify({
                message: 'No tasks to schedule',
                isRunning: false
            }), {
                headers: { 'Content-Type': 'application/json' }
            });
        }

        console.log(chalk.green(`🚀 Started scheduler for bot ${this.botId}`));
        this.log('info', 'Scheduler started successfully.');
        this.notifySchedulerStatusUpdate();
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
            this.log('warn', 'Stop command received but scheduler not running.');
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
        this.log('info', 'Scheduler stopped successfully.');
        this.notifySchedulerStatusUpdate(); // Notify status change
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
        this.log('debug', 'Status check requested.');
        let statusChanged = false; // Flag to check if we need to notify

        // If bot is running and we have a next alarm time, verify the alarm is actually set
        if (this.isRunning && this.currentAlarmTime) {
            try {
                const alarmTime = await this.ctx.storage.getAlarm();
                if (!alarmTime) {
                    console.log(chalk.yellow(`⚠️ Alarm time exists (${new Date(this.currentAlarmTime).toISOString()}) but no alarm is set. Rescheduling...`));
                    this.log('warn', 'Alarm time mismatch found during status check, attempting to reschedule.');
                    const scheduled = await this.scheduleNextAlarm(); // This will update currentAlarmTime and persist
                    if (scheduled) statusChanged = true;
                } else if (alarmTime !== this.currentAlarmTime) {
                    // This case indicates a potential desync between in-memory currentAlarmTime and stored alarm.
                    // Trust the stored alarm and update in-memory state.
                    this.log('warn', `In-memory currentAlarmTime (${this.currentAlarmTime}) differs from stored alarm (${alarmTime}). Syncing.`);
                    this.currentAlarmTime = alarmTime;
                    // No need to call storage.put for state here unless isRunning also changed, scheduleNextAlarm handles its own persistence of currentAlarmTime
                    statusChanged = true;
                }
            } catch (error) {
                console.log(chalk.red(`❌ Error checking alarm status: ${error}`));
                this.log('error', `Error during alarm status check: ${error instanceof Error ? error.message : String(error)}`);
                // If we can't check the alarm status, try to reschedule anyway
                const scheduled = await this.scheduleNextAlarm(); // This will update currentAlarmTime and persist
                if (scheduled) statusChanged = true;
            }
        } else if (this.isRunning && !this.currentAlarmTime) {
            // Scheduler is running but has no alarm time, implies it should try to schedule one.
            this.log('warn', 'Scheduler is running but no alarm is set. Attempting to schedule.');
            const scheduled = await this.scheduleNextAlarm();
            if (scheduled) statusChanged = true;
        }

        if (statusChanged) {
            this.notifySchedulerStatusUpdate();
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
        this.log('debug', 'Attempting to schedule next alarm.');

        // Get active tasks for this bot using the stored bot ID
        const tasks = await this.taskService.getActiveTaskDefinitionsByBot(this.botId);
        if (tasks.length === 0) {
            console.log(chalk.yellow(`⚠️ No active tasks found for bot ${this.botId}`));
            this.log('info', 'No active tasks found to schedule.');
            return false;
        }

        // Find the next task to run
        const now = new Date();
        const nextTask = await tasks.reduce(async (earliestPromise: Promise<Date | null>, task: BotTaskDefinition) => {
            const earliest = await earliestPromise;

            // If task has a planned run time, use that
            if (task.next_planned_run_at) {
                const taskTime = new Date(task.next_planned_run_at);
                if (!earliest || taskTime < earliest) {
                    this.log('debug', `Task ${task.id} has a planned run time of ${taskTime.toISOString()}`);
                    return taskTime;
                }
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
                        this.log('debug', `Task ${task.id} (from history) next run time: ${nextRunTime.toISOString()}`);
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
                this.log('debug', `Fresh task ${task.id} (lead time) next run time: ${nextRunTime.toISOString()}`);
                return nextRunTime;
            }

            return earliest;
        }, Promise.resolve(null));

        if (!nextTask) {
            console.log(chalk.yellow(`⚠️ No tasks scheduled for bot ${this.botId}`));
            this.log('info', 'No tasks found to schedule for an alarm.');
            return false;
        }

        // Schedule the alarm
        const alarmTime = nextTask.getTime();
        const oldAlarmTime = this.currentAlarmTime;
        await this.ctx.storage.setAlarm(alarmTime);
        this.currentAlarmTime = alarmTime;
        await this.ctx.storage.put('state', { botId: this.botId!, isRunning: this.isRunning, currentAlarmTime: alarmTime });
        console.log(chalk.green(`⏰ Scheduled next alarm for bot ${this.botId} at ${new Date(alarmTime).toISOString()}`));
        this.log('info', `Next alarm scheduled for ${new Date(alarmTime).toISOString()}`);
        if (oldAlarmTime !== this.currentAlarmTime || !this.isRunning) { // also notify if isRunning changed implicitly by starting to run
            this.notifySchedulerStatusUpdate();
        }
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
            console.log(chalk.red('❌ [BotScheduler:Alarm] Alarm triggered but bot ID is not set'));
            // Cannot use this.log reliably here as botId is not set.
            return;
        }

        this.rateLimitService = new RateLimitService(new RateLimitTrackingRepository(this.env.DB), 'basic', botId);
        const workerId = crypto.randomUUID();
        console.log(chalk.cyan(`🔔 [BotScheduler:Alarm] Alarm triggered for bot ${botId}, workerId: ${workerId}`));
        this.log('info', `Alarm triggered. Worker ID: ${workerId}`);

        try {
            //TODO: lock refactor
            //  console.log(chalk.blue(`🧹 [BotScheduler:Alarm] Attempting to cleanup expired locks for bot ${botId}`));
            //  this.log('debug', 'Attempting to cleanup expired locks.');
            //  await this.taskService.cleanupExpiredLocks();
            //  console.log(chalk.blue(`✅ [BotScheduler:Alarm] Expired locks cleanup complete for bot ${botId}`));
            //  this.log('debug', 'Expired locks cleanup complete.');

            const dueTasks = await this.taskService.getDueTasksByBot(botId, new Date());
            console.log(chalk.blue(`📋 [BotScheduler:Alarm] Found ${dueTasks.length} due tasks for bot ${botId}:`), dueTasks.map(t => t.id));
            this.log('debug', `Found ${dueTasks.length} due tasks.`);

            for (const task of dueTasks) {
                console.log(chalk.cyan(`🔄 [BotScheduler:Alarm] Processing task ${task.id} (${task.task_type}) for bot ${botId}`));
                this.log('debug', `Processing task ${task.id} (${task.task_type})`);

                const handler = taskHandlers.find(h => h.canHandle(task));
                if (!handler) {
                    console.log(chalk.red(`❌ [BotScheduler:Alarm] No handler found for task type ${task.task_type} (task ID: ${task.id})`));
                    this.log('error', `No handler found for task type ${task.task_type} (task ID: ${task.id})`);
                    continue;
                }

                //TODO: lock refactor
                //  console.log(chalk.blue(`🔐 [BotScheduler:Alarm] Attempting to acquire lock for task ${task.id}`));
                //  this.log('debug', `Attempting to acquire lock for task ${task.id}`);
                //  if (!(await this.taskService.acquireTaskLock(task.id, workerId, 5))) {
                //    console.log(chalk.yellow(`⚠️ [BotScheduler:Alarm] Could not acquire lock for task ${task.id}`));
                //    this.log('warn', `Could not acquire lock for task ${task.id}`);
                //    continue;
                //}
                //  console.log(chalk.blue(`✅ [BotScheduler:Alarm] Lock acquired for task ${task.id}`));
                //  this.log('debug', `Lock acquired for task ${task.id}`);

                let execution: BotTaskExecution | null = null;
                try {
                    let rateLimitRemaining = 0;
                    if (task.rate_limit_endpoint) {
                        console.log(chalk.blue(`📊 [BotScheduler:Alarm] Getting initial rate limit stats for task ${task.id}, endpoint: ${task.rate_limit_endpoint}`));
                        const stats = await this.rateLimitService.getUsageStats(task.rate_limit_endpoint);
                        rateLimitRemaining = stats.limit - stats.current;
                        console.log(chalk.blue(`📊 [BotScheduler:Alarm] Initial rate limit for task ${task.id}: ${rateLimitRemaining}`));
                        this.log('debug', `Initial rate limit for task ${task.id}: ${rateLimitRemaining}`);
                    }

                    const executionData = {
                        bot_id: botId,
                        task_definition_id: task.id,
                        status: 'started' as TaskStatus,
                        started_at: new Date(),
                        completed_at: null,
                        duration_ms: 0,
                        attempt_number: 1, // Assuming this is the first attempt within this alarm
                        rate_limit_remaining: rateLimitRemaining,
                        worker_id: workerId,
                        is_locked: true,
                        lock_expires_at: new Date(Date.now() + 5 * 60 * 1000),
                        result_data: null,
                        error_message: null,
                        error_stack: null
                    };
                    console.log(chalk.blue(`➕ [BotScheduler:Alarm] Creating execution record for task ${task.id} with data:`), executionData);
                    this.log('debug', `Creating execution record for task ${task.id}`);
                    execution = await this.taskService.createTaskExecution(executionData);
                    console.log(chalk.blue(`✅ [BotScheduler:Alarm] Execution record ${execution.id} created for task ${task.id}`));
                    this.log('debug', `Execution record ${execution.id} created for task ${task.id}`);
                    this.notifyEntityUpdate('task_execution', 'created', execution.id);

                    console.log(chalk.blue(`▶️ [BotScheduler:Alarm] Executing task ${task.id} via handler`));
                    this.log('debug', `Executing task ${task.id} via handler`);
                    await handler.execute(this.env, botId, task, execution, this.taskService);
                    console.log(chalk.green(`✅ [BotScheduler:Alarm] Handler execution complete for task ${task.id}`));
                    this.log('debug', `Handler execution complete for task ${task.id}`);

                    if (execution) {
                        let updatedRateLimitRemaining = 0;
                        if (task.rate_limit_endpoint) {
                            console.log(chalk.blue(`📊 [BotScheduler:Alarm] Getting updated rate limit stats for task ${task.id}, endpoint: ${task.rate_limit_endpoint}`));
                            const stats = await this.rateLimitService.getUsageStats(task.rate_limit_endpoint);

                            updatedRateLimitRemaining = stats.limit - stats.current;
                            console.log(chalk.blue(`📊 [BotScheduler:Alarm] Updated rate limit for task ${task.id}: ${updatedRateLimitRemaining}`));
                            this.log('debug', `Updated rate limit for task ${task.id}: ${updatedRateLimitRemaining}`);
                        }

                        const startedAt = new Date(execution.started_at);
                        const updateExecutionData = {
                            status: 'completed' as TaskStatus,
                            completed_at: new Date(),
                            duration_ms: Date.now() - startedAt.getTime(),
                            rate_limit_remaining: updatedRateLimitRemaining,
                            is_locked: false,
                            lock_expires_at: null
                        };
                        console.log(chalk.blue(`💾 [BotScheduler:Alarm] Updating execution ${execution.id} for task ${task.id} to completed with data:`), updateExecutionData);
                        this.log('debug', `Updating execution ${execution.id} for task ${task.id} to completed.`);
                        await this.taskService.updateTaskExecution(execution.id, updateExecutionData);
                        console.log(chalk.blue(`✅ [BotScheduler:Alarm] Execution ${execution.id} updated for task ${task.id}`));
                        this.log('debug', `Execution ${execution.id} updated for task ${task.id}`);
                        this.notifyEntityUpdate('task_execution', 'updated', execution.id);

                        const updateTaskDefData = {
                            last_successful_run_at: new Date(),
                            next_planned_run_at: new Date(Date.now() + task.interval_minutes * 60 * 1000)
                        };
                        console.log(chalk.blue(`💾 [BotScheduler:Alarm] Updating task definition ${task.id} with data:`), updateTaskDefData);
                        this.log('debug', `Updating task definition ${task.id} with next planned run.`);
                        await this.taskService.updateTaskDefinition(task.id, updateTaskDefData);
                        console.log(chalk.blue(`✅ [BotScheduler:Alarm] Task definition ${task.id} updated`));
                        this.log('debug', `Task definition ${task.id} updated.`);
                        this.notifyEntityUpdate('task_definition', 'updated', task.id);
                    }

                    console.log(chalk.green(`🏁 [BotScheduler:Alarm] Successfully completed processing for task ${task.id}`));
                    this.log('info', `Successfully completed processing for task ${task.id}`);
                } catch (error) {
                    console.log(chalk.red(`❌ [BotScheduler:Alarm] Error processing task ${task.id} (execution ID: ${execution?.id}):`), error);
                    this.log('error', `Error processing task ${task.id} (execution ID: ${execution?.id}): ${error instanceof Error ? error.message : String(error)}`);

                    if (execution) {
                        let rateLimitRemainingOnError = 0;
                        if (task.rate_limit_endpoint) {
                            console.log(chalk.blue(`📊 [BotScheduler:Alarm] Getting rate limit stats on error for task ${task.id}, endpoint: ${task.rate_limit_endpoint}`));
                            const stats = await this.rateLimitService.getUsageStats(task.rate_limit_endpoint);
                            rateLimitRemainingOnError = stats.limit - stats.current;
                            console.log(chalk.blue(`📊 [BotScheduler:Alarm] Rate limit on error for task ${task.id}: ${rateLimitRemainingOnError}`));
                            this.log('debug', `Rate limit on error for task ${task.id}: ${rateLimitRemainingOnError}`);
                        }

                        // Ensure attempt_number is accurate if retrying from a previous execution
                        // This part of the logic might need refinement if alarms re-process tasks already in 'retrying' state.
                        // For now, assuming execution.attempt_number is correctly set by createTaskExecution or fetched if task was already 'retrying'.
                        const currentAttempt = execution.attempt_number || 1;

                        if (currentAttempt < (task.max_retries || 3)) {
                            const retryDelayMinutes = task.retry_delay_minutes || 5;
                            const nextRetryTime = new Date(Date.now() + retryDelayMinutes * 60 * 1000);
                            const errorUpdateExecutionData = {
                                status: 'retrying' as TaskStatus,
                                completed_at: new Date(),
                                duration_ms: Date.now() - new Date(execution.started_at).getTime(),
                                rate_limit_remaining: rateLimitRemainingOnError,
                                is_locked: false,
                                lock_expires_at: null,
                                error_message: error instanceof Error ? error.message : String(error),
                                error_stack: error instanceof Error ? error.stack || null : null,
                                attempt_number: currentAttempt + 1 // Increment attempt number
                            };
                            console.log(chalk.yellow(`🔄 [BotScheduler:Alarm] Updating execution ${execution.id} for task ${task.id} to retrying with data:`), errorUpdateExecutionData);
                            this.log('warn', `Updating execution ${execution.id} for task ${task.id} to retrying (attempt ${currentAttempt + 1}).`);
                            await this.taskService.updateTaskExecution(execution.id, errorUpdateExecutionData);
                            this.notifyEntityUpdate('task_execution', 'updated', execution.id);

                            const retryTaskDefData = { next_planned_run_at: nextRetryTime };
                            console.log(chalk.yellow(`🔄 [BotScheduler:Alarm] Updating task definition ${task.id} for retry with data:`), retryTaskDefData);
                            this.log('warn', `Updating task definition ${task.id} for retry at ${nextRetryTime.toISOString()}`);
                            await this.taskService.updateTaskDefinition(task.id, retryTaskDefData);
                            this.notifyEntityUpdate('task_definition', 'updated', task.id);
                            console.log(chalk.yellow(`⏳ [BotScheduler:Alarm] Task ${task.id} will retry at ${nextRetryTime.toISOString()}`));
                            this.log('warn', `Task ${task.id} will retry at ${nextRetryTime.toISOString()}`);
                        } else {
                            const failUpdateExecutionData = {
                                status: 'failed' as TaskStatus,
                                completed_at: new Date(),
                                duration_ms: Date.now() - new Date(execution.started_at).getTime(),
                                rate_limit_remaining: rateLimitRemainingOnError,
                                is_locked: false,
                                lock_expires_at: null,
                                error_message: error instanceof Error ? error.message : String(error),
                                error_stack: error instanceof Error ? error.stack || null : null
                            };
                            console.log(chalk.red(`🛑 [BotScheduler:Alarm] Updating execution ${execution.id} for task ${task.id} to failed with data:`), failUpdateExecutionData);
                            this.log('error', `Updating execution ${execution.id} for task ${task.id} to failed.`);
                            await this.taskService.updateTaskExecution(execution.id, failUpdateExecutionData);
                            this.notifyEntityUpdate('task_execution', 'updated', execution.id);

                            if (task.error_threshold) {
                                console.log(chalk.blue(`📉 [BotScheduler:Alarm] Checking error threshold for task ${task.id} (threshold: ${task.error_threshold})`));
                                this.log('debug', `Checking error threshold for task ${task.id} (threshold: ${task.error_threshold})`);
                                const recentFailures = await this.taskService.getFailedTaskExecutionsByBot(botId, task.error_threshold);
                                console.log(chalk.blue(`📉 [BotScheduler:Alarm] Found ${recentFailures.length} recent failures for bot ${botId}`));
                                this.log('debug', `Found ${recentFailures.length} recent failures for bot ${botId}`);
                                if (recentFailures.filter(f => f.task_definition_id === task.id).length >= task.error_threshold) {
                                    console.log(chalk.red(`⏸️ [BotScheduler:Alarm] Pausing task ${task.id} due to exceeding error threshold.`));
                                    this.log('warn', `Pausing task ${task.id} due to exceeding error threshold.`);
                                    await this.taskService.updateTaskDefinition(task.id, { is_active: false });
                                    this.notifyEntityUpdate('task_definition', 'updated', task.id);
                                }
                            }
                            console.log(chalk.red(`🛑 [BotScheduler:Alarm] Task ${task.id} marked as failed after max retries.`));
                            this.log('error', `Task ${task.id} marked as failed after max retries.`);
                        }
                    }
                } finally {
                    //TODO: lock refactor
                    //  console.log(chalk.blue(`🔓 [BotScheduler:Alarm] Releasing lock for task ${task.id}`));
                    //  this.log('debug', `Releasing lock for task ${task.id}`);
                    //  await this.taskService.releaseTaskLock(task.id);
                    //  console.log(chalk.blue(`✅ [BotScheduler:Alarm] Lock released for task ${task.id}`));
                    //  this.log('debug', `Lock released for task ${task.id}`);
                }
            }
        } catch (error) {
            console.log(chalk.red('❌ [BotScheduler:Alarm] Unhandled error in alarm handler main try-catch:'), error);
            this.log('error', `Unhandled error in alarm handler: ${error instanceof Error ? error.message : String(error)}`);
        } finally {
            console.log(chalk.blue(`⏭️ [BotScheduler:Alarm] Scheduling next alarm for bot ${botId}`));
            this.log('debug', 'Scheduling next alarm (end of alarm handler).');
            await this.scheduleNextAlarm();
            console.log(chalk.blue(`✅ [BotScheduler:Alarm] Next alarm scheduling attempt complete for bot ${botId}`));
            this.log('debug', 'Next alarm scheduling attempt complete.');
        }
    }

    private notifySchedulerStatusUpdate() {
        if (!this.botId) return;
        const message: SchedulerStatusUpdateMessage = {
            type: 'scheduler_status_update',
            botId: this.botId,
            isRunning: this.isRunning,
            currentAlarmTime: this.currentAlarmTime,
            timestamp: Date.now(),
        };
        this.broadcast(message);
        this.log('debug', `Sent scheduler status update: isRunning: ${this.isRunning}, alarmTime: ${this.currentAlarmTime}`);
    }

    private notifyEntityUpdate(entityType: EntityUpdateNotification['entityType'], action: EntityUpdateNotification['action'], entityId: string) {
        if (!this.botId) return;
        const message: EntityUpdateNotification = {
            type: 'entity_update',
            entityType,
            action,
            entityId,
            botId: this.botId,
            timestamp: Date.now(),
        };
        this.broadcast(message);
        this.log('debug', `Sent entity update: ${entityType} ${action} for ID ${entityId}`);
    }

    // WebSocket Lifecycle Handlers
    async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
        // Ensure botId is set before attempting to log or process
        if (!this.botId) {
            console.log(chalk.red('❌ [BotScheduler:webSocketMessage] Received message but botId is not set. Ignoring.'));
            // Consider closing the WebSocket if this state is unexpected
            // ws.close(1011, 'Bot not initialized'); 
            return;
        }
        this.log('debug', `Received WebSocket message: ${typeof message === 'string' ? message : 'binary data'}`);
        // Currently, the DO primarily sends. If UI needs to send control messages in the future,
        // they would be handled here. For example:
        // try {
        //   const parsedMessage = JSON.parse(message as string);
        //   if (parsedMessage.type === 'set_log_level') {
        //     this.log('info', `Client requested log level change to: ${parsedMessage.level}`);
        //     // (Logic to actually change log verbosity if implemented)
        //   }
        // } catch (e) {
        //   this.log('error', 'Failed to parse client WebSocket message.');
        // }
    }

    async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
        if (!this.botId) {
            console.log(chalk.red(`❌ [BotScheduler:webSocketClose] WebSocket closed (code ${code}, reason '${reason}', clean: ${wasClean}) but botId is not set.`));
            return;
        }
        this.log('info', `WebSocket closed: code ${code}, reason '${reason}', wasClean ${wasClean}`);
    }

    async webSocketError(ws: WebSocket, error: any) {
        if (!this.botId) {
            console.log(chalk.red(`❌ [BotScheduler:webSocketError] WebSocket error (${error.toString()}) but botId is not set.`));
            return;
        }
        this.log('error', `WebSocket error: ${error.toString()}`);
    }
}