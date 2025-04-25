import { BotConfigurationRepository, BotTaskDefinition, BotTaskExecution, PostPullService, RateLimitTrackingRepository } from 'twitter-bot-shared-lib';
import { TaskService } from 'twitter-bot-shared-lib';
import { BaseTaskHandler } from './taskHandler';
import { Env } from '../types/env';
import { initializeTwitterClient } from '../utils/twitterClient';
import { RateLimitService } from 'twitter-bot-shared-lib';

export class PullMentionsTaskHandler extends BaseTaskHandler {
    canHandle(task: BotTaskDefinition): boolean {
        return task.task_type === 'pull_mentions';
    }

    async execute(env: Env, botId: string, task: BotTaskDefinition, execution: BotTaskExecution, taskService: TaskService): Promise<void> {
        // Check rate limits and dependencies
        if (!(await this.handleRateLimits(env, task, taskService))) {
            throw new Error('Rate limit check failed');
        }
        if (!(await this.handleDependencies(task, taskService))) {
            throw new Error('Dependencies not met');
        }

        try {
            console.log(`Pulling mentions for bot ${botId} and task ${task.id}`);

            // Initialize the Twitter client
            const twitterClient = await initializeTwitterClient(env, botId);

            // Get the active configuration
            // TODO: this is only here to satisfy the constraint on bot_config_id
            const botConfigRepository = new BotConfigurationRepository(env.DB);
            const activeConfig = await botConfigRepository.findActiveByBotId(botId);
            if (!activeConfig) {
                throw new Error('No active configuration found');
            }

            const postPullService = new PostPullService(env.DB, twitterClient);
            const postPull = await postPullService.createPostPull({
                bot_id: botId,
                bot_config_id: activeConfig.id,
                initiated_by: 'scheduled',
                purpose: 'reply',
                source: 'mentions',
                settings: {
                    max_results: 100
                },
                created_by: 'bot_' + botId,
                updated_by: 'bot_' + botId
            });

            // This will update rate limits if call is successful
            await postPullService.processPostPull(postPull.id);

            // Get updated post pull
            const postPullResult = await postPullService.getPostPull(postPull.id);

            // Update task execution with results
            await taskService.completeTaskExecution(execution.id, {
                mentions_pulled: postPullResult.processed_posts
            });
        } catch (error) {
            await taskService.failTaskExecution(execution.id, error as Error);
            throw error;
        }
    }
} 