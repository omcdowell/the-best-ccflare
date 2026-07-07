import { requestEvents, TIME_CONSTANTS } from "@better-ccflare/core";
import {
	sanitizeRequestHeaders,
	withSanitizedProxyHeaders,
} from "@better-ccflare/http-common";
import { Logger } from "@better-ccflare/logger";
import type { Account, RateLimitReason } from "@better-ccflare/types";
import type { ProxyContext } from "./handlers";
import { applyRateLimitCooldown } from "./handlers/rate-limit-cooldown";
import { createSseRateLimitSniffer } from "./handlers/sse-rate-limit-sniffer";
import { combineChunks, teeStream } from "./stream-tee";
import { getUsageCollector } from "./usage-collector";
import type { EndMessage, StartMessage } from "./worker-messages";

const log = new Logger("ResponseHandler");

function fireAndForgetEnd(msg: EndMessage): void {
	getUsageCollector()
		.handleEnd(msg)
		.catch((err: unknown) => {
			log.error(`handleEnd failed for request ${msg.requestId}`, err);
		});
}

function getMidStreamRateLimitCooldownMs(): number {
	return (
		Number(process.env.CCFLARE_DEFAULT_COOLDOWN_NO_RESET_MS) ||
		TIME_CONSTANTS.DEFAULT_RATE_LIMIT_NO_RESET_COOLDOWN_MS
	);
}

// Must match MAX_REQUEST_BODY_BYTES in usage-collector.ts.
// Cap applied before passing to collector to avoid multi-MB copies.
// 4MB so afterburn can see full conversation history for friction analysis.
const MAX_REQUEST_BODY_BYTES = 4 * 1024 * 1024;

function extractRequestedModel(requestBody: ArrayBuffer | null): string | null {
	if (!requestBody || requestBody.byteLength === 0) return null;
	try {
		const parsed = JSON.parse(
			new TextDecoder().decode(
				new Uint8Array(requestBody).subarray(
					0,
					Math.min(requestBody.byteLength, MAX_REQUEST_BODY_BYTES),
				),
			),
		) as { model?: unknown };
		return typeof parsed.model === "string" && parsed.model.length > 0
			? parsed.model
			: null;
	} catch {
		return null;
	}
}

/**
 * Check if a response should be considered successful/expected
 * Treats certain well-known paths that return 404 as expected
 */
function isExpectedResponse(path: string, response: Response): boolean {
	if (path.startsWith("/.well-known/") && response.status === 404) {
		return true;
	}
	return response.ok;
}

export interface ResponseHandlerOptions {
	requestId: string;
	method: string;
	path: string;
	account: Account | null;
	requestHeaders: Headers;
	requestBody: ArrayBuffer | null;
	project?: string | null;
	response: Response;
	timestamp: number;
	retryAttempt: number;
	failoverAttempts: number;
	agentUsed?: string | null;
	apiKeyId?: string | null;
	apiKeyName?: string | null;
	comboName?: string | null;
	clientPath?: string | null;
	upstreamPath?: string | null;
	routingMode?: string | null;
}

export async function forwardToClient(
	options: ResponseHandlerOptions,
	ctx: ProxyContext,
): Promise<Response> {
	const {
		requestId,
		method,
		path,
		account,
		requestHeaders,
		requestBody,
		project,
		response: responseRaw,
		timestamp,
		retryAttempt,
		failoverAttempts,
		agentUsed,
		apiKeyId,
		apiKeyName,
		comboName,
		clientPath,
		upstreamPath,
		routingMode,
	} = options;

	const response = withSanitizedProxyHeaders(responseRaw);
	const sanitizedReq = sanitizeRequestHeaders(requestHeaders);
	const requestHeadersObj = Object.fromEntries(sanitizedReq.entries());
	const responseHeadersObj = Object.fromEntries(response.headers.entries());
	const isStream = ctx.provider.isStreamingResponse?.(response) ?? false;
	const shouldStorePayloads = ctx.config.getStorePayloads?.() ?? true;
	const loggedPath = clientPath ?? path;

	// Filter out:
	//   - count_tokens requests on providers that synthesize or proxy advisory
	//     token counts; these aren't billable user traffic.
	//   - synthetic auto-refresh probes (issue #199, bug 2). Logging these
	//     pollutes the user-visible 503/200 metrics on the dashboard with
	//     internal scheduler activity. Header set by AutoRefreshScheduler
	//     mirrors the existing keepalive pattern.
	const isAutoRefreshProbe =
		requestHeaders.get("x-better-ccflare-auto-refresh") === "true";
	const isSyntheticCountTokens =
		path === "/v1/messages/count_tokens" &&
		(ctx.provider.name === "openai-compatible" ||
			ctx.provider.name === "codex");
	const shouldProcessRequest = !isSyntheticCountTokens && !isAutoRefreshProbe;

	if (shouldProcessRequest) {
		const requestedModel = extractRequestedModel(requestBody);
		const startMessage: StartMessage = {
			type: "start",
			messageId: crypto.randomUUID(),
			requestId,
			accountId: account?.id || null,
			method,
			path: loggedPath,
			timestamp,
			requestHeaders: requestHeadersObj,
			requestBody:
				shouldStorePayloads && requestBody
					? Buffer.from(
							new Uint8Array(requestBody).subarray(
								0,
								Math.min(requestBody.byteLength, MAX_REQUEST_BODY_BYTES),
							),
						).toString("base64")
					: null,
			requestedModel,
			project: project ?? null,
			responseStatus: response.status,
			responseHeaders: responseHeadersObj,
			isStream,
			providerName: ctx.provider.name,
			accountBillingType: account?.billing_type ?? null,
			accountAutoPauseOnOverageEnabled: account?.auto_pause_on_overage_enabled
				? 1
				: 0,
			accountName: account?.name ?? null,
			agentUsed: agentUsed || null,
			comboName: comboName || null,
			apiKeyId: apiKeyId || null,
			apiKeyName: apiKeyName || null,
			retryAttempt,
			failoverAttempts,
			clientPath: loggedPath,
			upstreamPath: upstreamPath ?? null,
			routingMode: routingMode ?? null,
		};
		getUsageCollector().handleStart(startMessage);
	}

	if (shouldProcessRequest) {
		requestEvents.emit("event", {
			type: "start",
			id: requestId,
			timestamp,
			method,
			path: loggedPath,
			accountId: account?.id || null,
			statusCode: response.status,
			agentUsed: agentUsed || null,
		});
	}

	if (isStream && response.body) {
		const rateLimitSniffer = account
			? createSseRateLimitSniffer({ provider: account.provider })
			: null;

		const onChunk = (value: Uint8Array): void => {
			if (shouldProcessRequest) {
				getUsageCollector().handleChunk(requestId, value);
			}

			if (account && rateLimitSniffer?.feed(value)) {
				const midStreamReason: RateLimitReason =
					rateLimitSniffer.firedReason === "overloaded_error"
						? "upstream_529_overloaded_with_reset"
						: "upstream_429_with_reset";
				applyRateLimitCooldown(
					account,
					{
						resetTime: Date.now() + getMidStreamRateLimitCooldownMs(),
						reason: midStreamReason,
					},
					ctx,
				);
			}
		};

		const onClose = (_buffered: Uint8Array[]): void => {
			if (shouldProcessRequest) {
				const endMsg: EndMessage = {
					type: "end",
					requestId,
					success: isExpectedResponse(path, response),
				};
				// Fire-and-forget: handleEnd is async for DB writes but we don't block streaming
				fireAndForgetEnd(endMsg);
			}
		};

		const onError = (err: Error): void => {
			if (shouldProcessRequest) {
				const endMsg: EndMessage = {
					type: "end",
					requestId,
					success: false,
					error: err.message,
				};
				fireAndForgetEnd(endMsg);
			}
		};

		const passthroughBody = teeStream(response.body, {
			onChunk,
			onClose,
			onError,
		});

		return new Response(passthroughBody, {
			status: response.status,
			statusText: response.statusText,
			headers: response.headers,
		});
	}

	if (!response.body) {
		if (shouldProcessRequest) {
			fireAndForgetEnd({
				type: "end",
				requestId,
				responseBody: null,
				success: isExpectedResponse(path, response),
			});
		}
		return response;
	}

	const MAX_NON_STREAM_BODY_BYTES = 256 * 1024;
	const passthroughBody = teeStream(response.body, {
		maxBytes: MAX_NON_STREAM_BODY_BYTES,
		onClose(buffered) {
			if (!shouldProcessRequest) return;
			const cappedBuf = combineChunks(buffered);
			fireAndForgetEnd({
				type: "end",
				requestId,
				responseBody:
					cappedBuf.byteLength > 0 ? cappedBuf.toString("base64") : null,
				success: isExpectedResponse(path, response),
			});
		},
		onError(err) {
			if (!shouldProcessRequest) return;
			fireAndForgetEnd({
				type: "end",
				requestId,
				success: false,
				error: err.message,
			});
		},
	});

	return new Response(passthroughBody, {
		status: response.status,
		statusText: response.statusText,
		headers: response.headers,
	});
}
