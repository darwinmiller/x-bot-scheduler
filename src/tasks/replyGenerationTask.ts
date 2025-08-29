import { BotTaskDefinition, BotTaskExecution, LLMService, PostReplyRepository, PulledPostRepository, TaskService, TwitterUserService } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';
import { BaseTaskHandler } from './taskHandler';
import chalk from 'chalk';

export interface ReplyGenerationTaskConfig {
    llm_config_id: string;
    prompt_id: string;
    vision_config?: {
        vision_llm_config_id: string;
        vision_prompt_id: string;
    };
}

export class ReplyGenerationTaskHandler extends BaseTaskHandler {
    /**
     * Determines if this handler can process the given task.
     * @param task - The task definition to check
     * @returns true if the task type is 'reply_generation'
     */

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
                        errorCount++; // Increment error count for this specific failure
                        continue;
                    }

                    // Fetch the Twitter user who authored the pulledPost
                    if (!pulledPost.author_id) {
                        console.error(chalk.red(`❌ Author ID not found on pulled post ${pulledPost.id} for reply ${reply.id}`));
                        errorCount++;
                        continue;
                    }
                    const twitterUser = await twitterUserService.findUserByTwitterId(pulledPost.author_id);

                    if (!twitterUser) {
                        console.error(chalk.red(`❌ Twitter user not found for author ID ${pulledPost.author_id} (pulled post ${pulledPost.id})`));
                        errorCount++;
                        continue;
                    }

                    // Check if the user is banned
                    if (twitterUser.banned_at) {
                        console.log(chalk.yellow(`🚫 User ${twitterUser.username} (ID: ${twitterUser.twitter_id}) is banned. Skipping reply generation for reply ${reply.id}.`));
                        await postReplyRepository.update(reply.id, {
                            status: 'rejected', // Assuming 'rejected' is a valid status
                            updated_by: 'bot_' + botId,
                            error_message: `Author ${twitterUser.username} (ID: ${twitterUser.twitter_id}) is banned.`
                        });
                        // Not incrementing successCount as no reply was generated.
                        // Not incrementing errorCount unless we consider this an error in the task itself.
                        // For now, this is a successful handling of a banned user.
                        continue;
                    }

                    // Twitter user for the pulledPost is fetched inside LLMService now
                    // Conversation lineage is also fetched and formatted inside LLMService

                    if (typeof task.config.llm_config_id !== 'string' || typeof task.config.prompt_id !== 'string') {
                        console.error(chalk.red(`❌ LLM config or prompt ID not found for reply ${reply.id}`));
                        continue;
                    }

                    // Variables are now constructed within llmService.generate
                    // const variables: Record<string, string> = {}
                    // if (twitterUser) {
                    //     variables.twitter_user_name = twitterUser.name;
                    //     variables.twitter_user_handle = twitterUser.username;
                    // }
                    // if (pulledPost.reply_to_tweet_id) {
                    //     const conversationLineage = await pulledPostRepository.getConversationLineage(pulledPost.twitter_post_id);
                    //     variables.conversation_context = conversationLineage.map(post => `${post.author_username}: ${post.content}`).join('\n');
                    // }

                    const result = await llmService.generate({
                        llm_config_id: task.config.llm_config_id,
                        prompt_id: task.config.prompt_id,
                        pulled_post_id: pulledPost.id, // Pass pulled_post_id
                        purpose: 'reply',
                        reply_id: reply.id,
                        vision_config: task.config.vision_config as ReplyGenerationTaskConfig['vision_config']// Pass vision_config
                    }, 'bot_' + botId);

                    if (result.status === 'complete') {
                        await postReplyRepository.update(reply.id, {
                            generation_status: 'complete',
                            original_content: result.output,
                            updated_by: 'bot_' + botId
                        });
                        successCount++; // Increment success count
                    } else {
                        // Handle cases where LLM generation itself might fail or return a non-complete status
                        console.error(chalk.red(`❌ LLM generation failed for reply ${reply.id} with status: ${result.status}. LLM Output: ${result.output}`));
                        await postReplyRepository.update(reply.id, {
                            generation_status: 'failed',
                            updated_by: 'bot_' + botId,
                            error_message: `LLM generation returned status: ${result.status}. Error: ${result.error_message || 'No error message'}`
                        });
                        errorCount++;
                    }
                } catch (error) {
                    console.error(`Error processing reply ${reply.id} in task ${task.id}:`, error);
                    errorCount++;
                    // Update the specific reply to a failed status to avoid reprocessing indefinitely
                    try {
                        await postReplyRepository.update(reply.id, {
                            generation_status: 'failed',
                            updated_by: 'bot_' + botId,
                            error_message: error instanceof Error ? `Outer catch: ${error.message}` : 'Outer catch: Unknown error during reply processing'
                        });
                    } catch (updateError) {
                        console.error(`Error updating reply ${reply.id} to failed status:`, updateError);
                    }
                    // Continue to the next reply instead of throwing, to allow other replies to be processed.
                    // If the entire task should fail on any single reply error, then re-throw the error.
                    // For now, we log and continue.
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
