

import { TwitterClient, type AuthMethod, type UserAccessToken } from 'twitter-bot-shared-lib';
import type { D1Database } from '@cloudflare/workers-types';
import { BotRepository, BotConfigurationRepository } from 'twitter-bot-shared-lib';
import { Env } from '../types/env';

interface TokenRefreshResponse {
    access_token: string;
    refresh_token: string;
    expires_in: number;
    scope: string;
    token_type: string;
}

/**
 * Refreshes a bot's access token using the refresh token.
 * @param platform The platform object containing environment variables
 * @param botId The ID of the bot
 * @param refreshToken The refresh token to use
 * @returns The new access token data
 * @throws Error if the refresh fails
 */
async function refreshBotToken(
    env: Env,
    botId: string,
    refreshToken: string
): Promise<UserAccessToken> {
    if (!env.TWITTER_CLIENT_ID || !env.TWITTER_CLIENT_SECRET) {
        throw new Error('Twitter OAuth credentials not configured');
    }

    const response = await fetch('https://api.twitter.com/2/oauth2/token', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': `Basic ${btoa(`${env.TWITTER_CLIENT_ID}:${env.TWITTER_CLIENT_SECRET}`)}`
        },
        body: new URLSearchParams({
            grant_type: 'refresh_token',
            refresh_token: refreshToken
        })
    });

    if (!response.ok) {
        const error = await response.text();
        throw new Error(`Failed to refresh token: ${error}`);
    }

    const data = await response.json() as TokenRefreshResponse;

    // Calculate the expiry time properly
    const now = new Date();
    const expiresAt = new Date(now.getTime() + data.expires_in * 1000);
    const unixExpiryTimestamp = Math.floor(expiresAt.getTime() / 1000);

    // Update the tokens in the database
    const botRepository = new BotRepository(env.DB);
    await botRepository.updateBotTokens(botId, {
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        expires_at: expiresAt.toISOString(),
        scopes: data.scope,
        token_type: data.token_type
    });

    return {
        access_token: data.access_token,
        token_type: data.token_type,
        scope: data.scope,
        expires_at: unixExpiryTimestamp, // Unix timestamp in seconds
        refresh_token: data.refresh_token
    };
}

/**
 * Initializes a Twitter client for a specific bot.
 * @param platform The platform object containing environment variables and database
 * @param botId The ID of the bot to initialize the client for
 * @returns A configured TwitterClient instance
 * @throws Error if the bot is not found or tokens are invalid
 */
export async function initializeTwitterClient(env: Env, botId: string): Promise<TwitterClient> {
    const botRepository = new BotRepository(env.DB);
    const botConfigRepository = new BotConfigurationRepository(env.DB);
    const bot = await botRepository.findById(botId);

    if (!bot) {
        throw new Error(`Bot with ID ${botId} not found`);
    }

    // Get the bot's tokens using the repository
    const tokens = await botRepository.getBotTokens(botId);
    if (!tokens) {
        throw new Error(`No tokens found for bot ${botId}`);
    }

    // Get the active bot configuration
    const configs = await botConfigRepository.findByBotId(botId);
    if (!configs || configs.length === 0) {
        throw new Error(`No bot configuration found for bot ${botId}`);
    }

    // Use the first active configuration
    const activeConfig = configs.find(c => c.is_active) || configs[0];

    // Check if token is expired
    const expiresAt = new Date(tokens.expires_at);
    const now = new Date();
    const unixExpiryTimestamp = Math.floor(expiresAt.getTime() / 1000);

    // Create the user access token object
    const userAccessToken: UserAccessToken = {
        access_token: tokens.access_token,
        token_type: tokens.token_type,
        scope: tokens.scopes,
        expires_at: unixExpiryTimestamp, // Unix timestamp in seconds
        refresh_token: tokens.refresh_token || ''
    };

    // Create the auth method
    const authMethod: AuthMethod = {
        user_access_token: userAccessToken,
        client_id: env.TWITTER_CLIENT_ID,
        tokenRefreshFunction: async (refreshToken: string) => {
            try {
                return await refreshBotToken(env, botId, refreshToken);
            } catch (err) {
                console.error('Failed to refresh token:', err);
                throw new Error(`Token refresh failed: ${err instanceof Error ? err.message : err}`);
            }
        }
    };

    // Initialize and return the Twitter client with the bot configuration ID
    return new TwitterClient(authMethod, env.DB, botId, activeConfig.id);
} 