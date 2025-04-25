import { BotScheduler } from './durable-objects/BotScheduler';
import { Env } from './types/env';

export default {
	/**
	 * This is the standard fetch handler for a Cloudflare Worker
	 *
	 * @param request - The request submitted to the Worker from the client
	 * @param env - The interface to reference bindings declared in wrangler.jsonc
	 * @param ctx - The execution context of the Worker
	 * @returns The response to be sent back to the client
	 */
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		const path = url.pathname;
		// Extract bot ID from path
		const match = path.match(/^\/bots\/([^\/]+)\/(start|stop|status)$/);
		if (!match) {
			console.log('Invalid path', path);
			return new Response('Invalid path', { status: 400 });
		}

		const [, botId, action] = match;
		// Get the Durable Object for this bot
		const id = env.BOT_SCHEDULER.idFromName(botId);
		const stub = env.BOT_SCHEDULER.get(id);
		// Forward the request to the Durable Object
		return stub.fetch(request);
	}
} satisfies ExportedHandler<Env>;

export { BotScheduler };
