import { Env } from '../types/env';
import { PullMentionsTaskHandler } from './pullMentionsTask';
import { ReplyTaskHandler } from './replyTask';
import { ReplyGenerationTaskHandler } from './replyGenerationTask';
import { UserFetchTaskHandler } from './userFetchTask';
import { TaskHandler } from './taskHandler';

export let taskHandlers: TaskHandler[] = [];

export function initializeTaskHandlers(env: Env) {
    taskHandlers = [
        new PullMentionsTaskHandler(env),
        new ReplyTaskHandler(env),
        new ReplyGenerationTaskHandler(env),
        new UserFetchTaskHandler(env)
    ];
} 