import { BotTaskDefinition, BotTaskExecution, TaskService, PostReplyRepository, PulledPostRepository, TwitterUserService, TwitterDBUser, PulledPost, TwitterClient } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';
import { BaseTaskHandler } from './taskHandler';
import { initializeTwitterClient } from '../utils/twitterClient';
import chalk from 'chalk';

export class UserFetchTaskHandler extends BaseTaskHandler {
    canHandle(task: BotTaskDefinition): boolean {
        return task.task_type === 'user_fetch';
    }

    async execute(env: Env, botId: string, task: BotTaskDefinition, execution: BotTaskExecution, taskService: TaskService): Promise<void> {
        console.log(chalk.blue(`🚀 Starting UserFetchTask for bot ${botId}, task ${task.id}`));

        try {
            // 1. Standard pre-flight checks
            if (!(await this.handleRateLimits(env, task, taskService))) {
                throw new Error('Rate limit check failed');
            }
            if (!(await this.handleDependencies(task, taskService))) {
                throw new Error('Dependency check failed');
            }

            // 2. Initialize services
            const postReplyRepository = new PostReplyRepository(env.DB);
            const pulledPostRepository = new PulledPostRepository(env.DB);
            const twitterUserService = new TwitterUserService(env.DB);
            let twitterClient: TwitterClient | undefined;

            // 3. Get relevant PostReply objects
            const allRepliesForBot = await postReplyRepository.findByBotId(botId);
            const relevantReplies = allRepliesForBot.filter(
                reply => reply.status === 'pending' && reply.reply_mode === 'automatic'
            );

            if (relevantReplies.length === 0) {
                console.log(chalk.green(`✅ No pending automatic replies requiring user lookup for bot ${botId}`));
                await taskService.completeTaskExecution(execution.id, { users_checked: 0, users_identified_for_refresh: 0, users_refreshed: 0 });
                return;
            }
            console.log(chalk.cyan(`🔍 Found ${relevantReplies.length} relevant replies for user lookup.`));

            // 4. Collect user IDs (UUIDs from twitter_users table) that need fetching/refreshing
            const checkedUserIds = new Set<string>();
            const userIdsToRefresh = new Set<string>();

            for (const reply of relevantReplies) {
                const pulledPost = await pulledPostRepository.findById(reply.pulled_post_id);
                if (!pulledPost) {
                    console.error(chalk.red(`❌ Pulled post ${reply.pulled_post_id} not found for reply ${reply.id}`));
                    continue;
                }

                const postsInContext: PulledPost[] = [];
                if (pulledPost.reply_to_tweet_id) { // It's a reply, get lineage
                    try {
                        const lineage = await pulledPostRepository.getConversationLineage(pulledPost.twitter_post_id);
                        postsInContext.push(...lineage); // Lineage includes the current post
                    } catch (lineageError) {
                        console.error(chalk.red(`❌ Error fetching conversation lineage for post ${pulledPost.twitter_post_id}:`), lineageError);
                        // Still process the main pulledPost if lineage fails
                        postsInContext.push(pulledPost);
                    }
                } else { // It's an original tweet
                    postsInContext.push(pulledPost);
                }

                for (const post of postsInContext) {
                    if (post.twitter_user_id) { // twitter_user_id is the UUID FK to twitter_users.id
                        checkedUserIds.add(post.twitter_user_id);
                        const twitterUser = await twitterUserService.findUserById(post.twitter_user_id);
                        if (twitterUser) {
                            if (!twitterUser.fetched || !twitterUser.last_fetched_at) {
                                console.log(chalk.yellow(`➕ User ${twitterUser.username} (ID: ${twitterUser.id}) marked for refresh.`));
                                userIdsToRefresh.add(twitterUser.id); // Add the UUID
                            }
                        } else {
                            console.warn(chalk.yellow(`⚠️ Twitter user with UUID ${post.twitter_user_id} not found in DB for post ${post.id}. This user might need to be created first.`));
                        }
                    }
                }
            }

            // 5. Fetch/Refresh users if any identified
            let usersRefreshedCount = 0;
            if (userIdsToRefresh.size > 0) {
                console.log(chalk.blue(`🌀 Attempting to refresh ${userIdsToRefresh.size} users.`));
                twitterClient = await initializeTwitterClient(env, botId);

                const usersToRefreshList: TwitterDBUser[] = [];
                for (const userId of userIdsToRefresh) {
                    const user = await twitterUserService.findUserById(userId); // Fetch the full user object
                    if (user) {
                        usersToRefreshList.push(user);
                    } else {
                        console.warn(chalk.yellow(`⚠️ User with ID ${userId} was marked for refresh but not found when re-fetching. Skipping.`));
                    }
                }

                if (usersToRefreshList.length > 0) {
                    try {
                        if (usersToRefreshList.length === 1) {
                            const userToRefresh = usersToRefreshList[0];
                            console.log(chalk.blue(`🌀 Refreshing 1 user: ${userToRefresh.username} (UUID: ${userToRefresh.id})`));
                            await twitterUserService.refreshUser(twitterClient, userToRefresh.id);
                            usersRefreshedCount = 1;
                            console.log(chalk.green(`✔️ User ${userToRefresh.username} refreshed.`));
                        } else {
                            console.log(chalk.blue(`🌀 Refreshing ${usersToRefreshList.length} users.`));
                            const refreshedUsers = await twitterUserService.refreshUsers(twitterClient, usersToRefreshList);
                            usersRefreshedCount = refreshedUsers.length;
                            console.log(chalk.green(`✔️ ${refreshedUsers.length} users refreshed.`));
                        }
                    } catch (refreshError) {
                        console.error(chalk.red('❌ Error during Twitter user refresh:'), refreshError);
                        // Continue to complete the task, but log that some refreshes may have failed.
                        // The taskService.failTaskExecution will be called by the outer catch if this is a critical failure.
                    }
                }
            } else {
                console.log(chalk.green('✅ No users required refreshing.'));
            }

            // 6. Complete task execution
            const resultData = {
                users_checked: checkedUserIds.size,
                users_identified_for_refresh: userIdsToRefresh.size,
                users_refreshed: usersRefreshedCount
            };
            console.log(chalk.green(`✅ UserLookupTask completed for bot ${botId}. Results: ${JSON.stringify(resultData)}`));
            await taskService.completeTaskExecution(execution.id, resultData);

        } catch (error) {
            console.error(chalk.red(`❌ Error executing UserLookupTask for bot ${botId}, task ${task.id}:`), error);
            await taskService.failTaskExecution(execution.id, error instanceof Error ? error : new Error(String(error)));
            throw error; // Re-throw to ensure the calling context knows about the failure
        }
    }
}
