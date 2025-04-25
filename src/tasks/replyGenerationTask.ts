import { BotTaskDefinition, BotTaskExecution, LLMService, PostReplyRepository, PulledPostRepository, TaskService, TwitterUserService } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';
import { BaseTaskHandler } from './taskHandler';
import chalk from 'chalk';

export interface ReplyGenerationTaskConfig {
    llm_config_id: string;
    prompt_id: string;
}

export class ReplyGenerationTaskHandler extends BaseTaskHandler {
    /**
     * Determines if this handler can process the given task.
     * @param task - The task definition to check
     * @returns true if the task type is 'reply_generation'
     */

    //TODO: fix this  LOL
    canHandle(task: BotTaskDefinition): boolean {
        return task.task_type === 'generate_replys';
    }

    /**
     * Executes the reply generation task.
     * - Checks rate limits
     * - Checks dependencies
     * - Generates replies for tweets
     * - Updates task execution status
     * @param env - Environment variables and services
     * @param botId - ID of the bot executing the task
     * @param task - Task definition
     * @param execution - Task execution record
     * @param taskService - Task service for database operations
     */
    async execute(
        env: Env,
        botId: string,
        task: BotTaskDefinition,
        execution: BotTaskExecution,
        taskService: TaskService
    ): Promise<void> {
        // Check rate limits
        if (!(await this.handleRateLimits(env, task, taskService))) {
            throw new Error('Rate limit check failed');
        }

        // Check dependencies
        if (!(await this.handleDependencies(task, taskService))) {
            throw new Error('Dependency check failed');
        }

        try {
            // Get pending replies that need generation
            const postReplyRepository = new PostReplyRepository(env.DB);
            const pendingReplies = await postReplyRepository.findPendingWithoutGeneration(botId);
            const llmService = new LLMService(env.DB, env);

            if (pendingReplies.length === 0) {
                console.log(chalk.green(`✅ No pending replies to generate for bot ${botId}`));
                await taskService.completeTaskExecution(execution.id, {
                    replies_generated: 0
                });
                return;
            }
            const pulledPostRepository = new PulledPostRepository(env.DB);
            const twitterUserService = new TwitterUserService(env.DB);

            let successCount = 0;
            let errorCount = 0;

            for (const reply of pendingReplies) {
                try {
                    // Get the pulled post this reply is associated with
                    const pulledPost = await pulledPostRepository.findById(reply.pulled_post_id);


                    if (!pulledPost) {
                        console.error(chalk.red(`❌ Pulled post not found for reply ${reply.id}`));
                        continue;
                    }
                    // Get the twitter user this reply is associated with
                    if (!pulledPost.author_id) {
                        console.error(chalk.red(`❌ Pulled post author ID not found for reply ${reply.id}`));
                        continue;
                    }
                    const twitterUser = await twitterUserService.findUserById(pulledPost.author_id);

                    if (typeof task.config.llm_config_id !== 'string' || typeof task.config.prompt_id !== 'string') {
                        console.error(chalk.red(`❌ LLM config or prompt ID not found for reply ${reply.id}`));
                        continue;
                    }

                    const result = await llmService.generate({
                        llm_config_id: task.config.llm_config_id,
                        prompt_id: task.config.prompt_id,
                        prompt: pulledPost.content,
                        variables: {},
                        purpose: 'reply',
                        reply_id: reply.id
                    }, 'bot_' + botId);

                    if (result.status === 'complete') {
                        await postReplyRepository.update(reply.id, {
                            generation_status: 'complete',
                            original_content: result.output,
                            updated_by: 'bot_' + botId
                        });
                    }
                } catch (error) {
                    console.error(`Error executing reply generation task ${task.id}:`, error);
                    errorCount++;
                    throw error;
                }
            }

            await taskService.completeTaskExecution(execution.id, {
                replies_generated: successCount,
                replies_failed: errorCount
            });
        } catch (error) {
            console.error(`Error executing reply generation task ${task.id}:`, error);
            throw error;
        }
    }
}
