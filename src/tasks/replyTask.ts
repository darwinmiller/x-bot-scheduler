import { BotTaskDefinition, BotTaskExecution, PostReplyRepository, PostReplyService, PulledPostRepository } from 'twitter-bot-shared-lib';
import { TaskService } from 'twitter-bot-shared-lib';
import { BaseTaskHandler } from './taskHandler';
import { Env } from '../types/env';
import chalk from 'chalk';
import { initializeTwitterClient } from '../utils/twitterClient';

export class ReplyTaskHandler extends BaseTaskHandler {
    canHandle(task: BotTaskDefinition): boolean {
        return task.task_type === 'reply_to_mentions';
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
            console.log(chalk.green(`🔍 Processing replies for task ${task.id}`));

            const postReplyService = new PostReplyService(env.DB);
            const pulledPostRepository = new PulledPostRepository(env.DB);
            const replies = await postReplyService.getUnsentAutoReplies();
            const twitterClient = await initializeTwitterClient(env, botId);
            let repliesSent = 0;
            let repliesFailed = 0;
            for (const reply of replies) {
                console.log(chalk.green(`🔍 Sending reply ${reply.id}`));
                const pulledPost = await pulledPostRepository.findById(reply.pulled_post_id);

                // Use edited_content if available, otherwise original_content
                let contentToSend = reply.edited_content || reply.original_content;

                if (!pulledPost) {
                    console.error(chalk.red(`❌ Pulled post not found for reply ${reply.id}`));
                    repliesFailed++;
                    continue;
                }
                if (!contentToSend) {
                    console.error(chalk.red(`❌ Reply content not found for reply ${reply.id}, nothing to send`));
                    repliesFailed++;
                    continue;
                }
                const createPostRequest = {
                    text: contentToSend,
                    reply: {
                        in_reply_to_tweet_id: pulledPost.twitter_post_id
                    }
                };

                try {
                    const twitterApiResponse = await twitterClient.posts.createPost(createPostRequest);
                    const botTwitterReplyId = twitterApiResponse.data.id;
                    const replySentAt = new Date();

                    await postReplyService.markReplyAsSentAndNotify(
                        reply.id,
                        botTwitterReplyId,
                        replySentAt,
                        botId, // Internal system botId
                        `bot_${botId}` // updatedBy
                    );

                    if (twitterApiResponse.data) {
                        repliesSent++;
                    } else {
                        // This case should ideally not be hit if createPost throws on failure or returns non-success
                        console.warn(chalk.yellow(`⚠️ Reply sent for ${reply.id} but API response data was unexpected.`));
                        repliesFailed++;
                    }
                } catch (sendError) {
                    console.error(chalk.red(`❌ Failed to send reply ${reply.id} to Twitter or update status/notify:`), sendError);
                    repliesFailed++;
                    // Optionally, update reply status to errored here if needed via postReplyService
                    // For now, it just increments failed count and continues to next reply.
                }
            }

            // Update task execution with results
            await taskService.completeTaskExecution(execution.id, {
                replies_sent: repliesSent,
                replies_failed: repliesFailed
            });
        } catch (error) {
            await taskService.failTaskExecution(execution.id, error as Error);
            throw error;
        }
    }
} 