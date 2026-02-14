/**
 * WebRTC client wrapper implementing TransportClient interface.
 * Uses PipecatClient with SmallWebRTCTransport for WebRTC connectivity.
 */

import { PipecatClient } from "@pipecat-ai/client-js";
import { SmallWebRTCTransport } from "@pipecat-ai/small-webrtc-transport";
import type {
	ConnectOptions,
	EventCallbackMap,
	RTVIEventType,
	TransportClient,
	TransportState,
} from "./TransportClient";
import { isWebRTCConnectOptions } from "./TransportClient";

/**
 * Clears the transport's keepAliveInterval to prevent "InvalidStateError" spam.
 * The library's stop() has a bug where the interval isn't cleared on abrupt disconnects.
 */
function clearKeepAliveInterval(client: PipecatClient): void {
	const transport = client.transport as { keepAliveInterval?: NodeJS.Timeout };
	if (transport?.keepAliveInterval) {
		clearInterval(transport.keepAliveInterval);
		transport.keepAliveInterval = undefined;
	}
}

/**
 * WebRTC client wrapper that implements the unified TransportClient interface.
 */
export class WebRTCClient implements TransportClient {
	private client: PipecatClient;

	constructor() {
		const transport = new SmallWebRTCTransport({
			iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
		});

		this.client = new PipecatClient({
			transport,
			enableMic: false,
			enableCam: false,
		});
	}

	async initDevices(): Promise<void> {
		await this.client.initDevices();

		// Release mic after device enumeration to avoid keeping it open
		try {
			const tracks = this.client.tracks();
			if (tracks?.local?.audio) {
				tracks.local.audio.stop();
			}
		} catch {
			// Ignore cleanup errors
		}
	}

	async connect(options: ConnectOptions): Promise<void> {
		if (!isWebRTCConnectOptions(options)) {
			throw new Error("Invalid connect options for WebRTC client");
		}

		await this.client.connect({
			webrtcRequestParams: options.webrtcRequestParams,
		});
	}

	async disconnect(): Promise<void> {
		clearKeepAliveInterval(this.client);
		await this.client.disconnect();
	}

	sendClientMessage(messageType: string, data: unknown): void {
		this.client.sendClientMessage(messageType, data);
	}

	on<K extends RTVIEventType>(event: K, callback: EventCallbackMap[K]): void {
		// Map our unified event types to PipecatClient's event types
		// PipecatClient.on accepts any string event name
		// biome-ignore lint/suspicious/noExplicitAny: PipecatClient has loose event typing
		(this.client.on as (e: string, cb: any) => void)(event, callback);
	}

	off<K extends RTVIEventType>(event: K, callback: EventCallbackMap[K]): void {
		// biome-ignore lint/suspicious/noExplicitAny: PipecatClient has loose event typing
		(this.client.off as (e: string, cb: any) => void)(event, callback);
	}

	emit<K extends RTVIEventType>(
		_event: K,
		..._args: Parameters<EventCallbackMap[K]>
	): void {
		// PipecatClient doesn't have a public emit method
		// This is for WebSocketClient compatibility, no-op for WebRTC
	}

	get transport(): { state: TransportState } {
		return this.client.transport as { state: TransportState };
	}

	tracks(): { local?: { audio?: MediaStreamTrack } } | null {
		return this.client.tracks();
	}

	enableMic(enabled: boolean): void {
		this.client.enableMic(enabled);
	}
}
