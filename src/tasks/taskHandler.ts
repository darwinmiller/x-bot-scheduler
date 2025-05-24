import { BotTaskDefinition, BotTaskExecution, RateLimitService, RateLimitTrackingRepository } from 'twitter-bot-shared-lib';
import { TaskService } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';
import { PullMentionsTaskHandler } from './pullMentionsTask';
import { ReplyTaskHandler } from './replyTask';
import { ReplyGenerationTaskHandler } from './replyGenerationTask';
import chalk from 'chalk';
import { taskHandlers } from './taskHandlers';


export interface TaskHandler {
    canHandle(task: BotTaskDefinition): boolean;
    execute(env: Env, botId: string, task: BotTaskDefinition, execution: BotTaskExecution, taskService: TaskService): Promise<void>;
}

export abstract class BaseTaskHandler implements TaskHandler {
    protected env: Env;

    constructor(env: Env) {
        this.env = env;
    }

    abstract canHandle(task: BotTaskDefinition): boolean;
    abstract execute(env: Env, botId: string, task: BotTaskDefinition, execution: BotTaskExecution, taskService: TaskService): Promise<void>;

    protected async handleRateLimits(env: Env, task: BotTaskDefinition, taskService: TaskService): Promise<boolean> {
        // Check if we have enough rate limit remaining
        if (task.min_rate_limit_remaining > 0) {
            // No rate limit endpoint, no need to check
            if (!task.rate_limit_endpoint) {
                return true;
            }
            const rateLimitService = new RateLimitService(new RateLimitTrackingRepository(env.DB), 'basic', task.bot_id);
            const stats = await rateLimitService.getUsageStats(task.rate_limit_endpoint);
            const remaining = stats.limit - stats.current;
            if (remaining < task.min_rate_limit_remaining) {
                console.log(`Rate limit remaining for task ${task.id} is ${remaining}, but minimum required is ${task.min_rate_limit_remaining}`);
                return false;
            }
        }
        return true;
    }

    protected async handleDependencies(task: BotTaskDefinition, taskService: TaskService): Promise<boolean> {
        if (task.dependencies.length === 0) {
            return true;
        }

        // Fetch all dependency definitions
        const dependencyDefinitions: BotTaskDefinition[] = [];
        for (const dependencyId of task.dependencies) {
            const dependency = await taskService.getTaskDefinitionById(dependencyId);
            if (!dependency) {
                console.error(chalk.red(`❌ Dependency ${dependencyId} not found for task ${task.id}`));
                return false; // Critical error if a defined dependency is missing
            }
            dependencyDefinitions.push(dependency);
        }

        // Sort dependencies by priority (descending) and then by next_planned_run_at (ascending)
        dependencyDefinitions.sort((a, b) => {
            if (b.priority !== a.priority) {
                return b.priority - a.priority;
            }
            // If next_planned_run_at is null, treat it as later than a defined date for sorting
            const aNextRun = a.next_planned_run_at ? new Date(a.next_planned_run_at).getTime() : Infinity;
            const bNextRun = b.next_planned_run_at ? new Date(b.next_planned_run_at).getTime() : Infinity;
            return aNextRun - bNextRun;
        });

        // Check and execute each dependency in the sorted order
        for (const dependency of dependencyDefinitions) {
            const dependencyId = dependency.id; // Get ID from the sorted definition

            // Check if dependency has a recent successful execution
            const lastExecution = await taskService.getTaskExecutionsByDefinitionId(dependencyId);
            if (lastExecution && lastExecution.length > 0 && lastExecution[0].status === 'completed') {
                const lastStartedAt = new Date(lastExecution[0].started_at);
                const timeSinceLastRun = Date.now() - lastStartedAt.getTime();
                const intervalMs = dependency.interval_minutes * 60 * 1000;

                // If the last execution was within the interval, we can skip
                if (timeSinceLastRun < intervalMs) {
                    console.log(chalk.yellow(`⏭️ Dependency ${dependencyId} (Priority: ${dependency.priority}) was recently executed, skipping`));
                    continue;
                }
            }

            // Create a new execution for the dependency
            const dependencyExecution = await taskService.createTaskExecution({
                bot_id: task.bot_id,
                task_definition_id: dependencyId,
                status: 'started',
                started_at: new Date(),
                completed_at: null,
                duration_ms: 0,
                attempt_number: 1,
                rate_limit_remaining: 0,
                worker_id: 'dependency_' + task.id,
                is_locked: true,
                lock_expires_at: new Date(Date.now() + 5 * 60 * 1000),
                result_data: null,
                error_message: null,
                error_stack: null
            });

            try {
                // Find appropriate handler for the dependency
                const handler = taskHandlers.find(h => h.canHandle(dependency));
                if (!handler) {
                    throw new Error(`No handler found for dependency task type ${dependency.task_type}`);
                }

                // Execute the dependency task
                console.log(chalk.blue(`🌀 Executing dependency ${dependencyId} (Priority: ${dependency.priority}) for task ${task.id}`));
                await handler.execute(this.env, task.bot_id, dependency, dependencyExecution, taskService);

                // Update execution status
                await taskService.updateTaskExecution(dependencyExecution.id, {
                    status: 'completed',
                    completed_at: new Date(),
                    duration_ms: Date.now() - new Date(dependencyExecution.started_at).getTime(),
                    is_locked: false,
                    lock_expires_at: null
                });

                // Update task definition with successful run
                await taskService.updateTaskDefinition(dependencyId, {
                    last_successful_run_at: new Date(),
                    next_planned_run_at: new Date(Date.now() + dependency.interval_minutes * 60 * 1000)
                });

                console.log(chalk.green(`✅ Successfully executed dependency ${dependencyId} (Priority: ${dependency.priority}) for task ${task.id}`));
            } catch (error) {
                console.error(chalk.red(`❌ Error executing dependency ${dependencyId} (Priority: ${dependency.priority}) for task ${task.id}:`, error));

                // Update execution status to failed
                await taskService.updateTaskExecution(dependencyExecution.id, {
                    status: 'failed',
                    completed_at: new Date(),
                    duration_ms: Date.now() - new Date(dependencyExecution.started_at).getTime(),
                    is_locked: false,
                    lock_expires_at: null,
                    error_message: error instanceof Error ? error.message : String(error),
                    error_stack: error instanceof Error ? error.stack || null : null
                });

                return false;
            }
        }

        return true;
    }
} 