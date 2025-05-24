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
            const postReplyRepository = new PostReplyRepository(env.DB);
            const replies = await postReplyService.getUnsentAutoReplies();
            const twitterClient = await initializeTwitterClient(env, botId);
            let repliesSent = 0;
            let repliesFailed = 0;
            for (const reply of replies) {
                console.log(chalk.green(`🔍 Sending reply ${reply.id}`));
                const pulledPost = await pulledPostRepository.findById(reply.pulled_post_id);
                const originalContent = JSON.parse(reply.original_content || '{}').content;
                if (!pulledPost) {
                    console.error(chalk.red(`❌ Pulled post not found for reply ${reply.id}`));
                    continue;
                }
                if (!originalContent) {
                    console.error(chalk.red(`❌ Reply content not found for reply ${reply.id}, nothing to send`));
                    continue;
                }
                const createPostRequest = {
                    text: originalContent,
                    reply: {
                        in_reply_to_tweet_id: pulledPost.twitter_post_id
                    }
                };

                const response = await twitterClient.posts.createPost(createPostRequest);

                await postReplyRepository.update(reply.id, {
                    status: 'replied',
                    reply_id: response.data.id,
                    reply_created_at: new Date(),
                    updated_by: 'bot_' + botId,
                    approved_at: new Date()
                });
                if (response.data) {
                    repliesSent++;
                } else {
                    repliesFailed++;
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