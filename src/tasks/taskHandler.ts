import { BotTaskDefinition, BotTaskExecution, RateLimitService, RateLimitTrackingRepository } from 'twitter-bot-shared-lib';
import { TaskService } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';

export interface TaskHandler {
    canHandle(task: BotTaskDefinition): boolean;
    execute(env: Env, botId: string, task: BotTaskDefinition, execution: BotTaskExecution, taskService: TaskService): Promise<void>;
}

export abstract class BaseTaskHandler implements TaskHandler {
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

        // Check if all dependencies have completed successfully
        for (const dependencyId of task.dependencies) {
            const dependency = await taskService.getTaskDefinitionById(dependencyId);
            if (!dependency) {
                console.error(`Dependency ${dependencyId} not found for task ${task.id}`);
                return false;
            }

            const lastExecution = await taskService.getTaskExecutionsByDefinitionId(dependencyId);
            if (!lastExecution || lastExecution.length === 0 || lastExecution[0].status !== 'completed') {
                console.log(`Dependency ${dependencyId} not completed for task ${task.id}`);
                return false;
            }
        }

        return true;
    }
} 