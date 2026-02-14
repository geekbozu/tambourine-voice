/**
 * Unified interface for transport clients (WebRTC and WebSocket).
 * This abstraction allows the connection machine to work with either transport type.
 */

import type { BotLLMTextData, TranscriptData } from "@pipecat-ai/client-js";

// Re-export RTVIEvent for consistency
export const RTVIEvent = {
	Connected: "connected",
	Disconnected: "disconnected",
	TransportStateChanged: "transportStateChanged",
	UserTranscript: "userTranscript",
	BotLlmStarted: "botLlmStarted",
	BotLlmText: "botLlmText",
	BotLlmStopped: "botLlmStopped",
	ServerMessage: "serverMessage",
	Error: "error",
	DeviceError: "deviceError",
	TrackStarted: "trackStarted",
	TrackStopped: "trackStopped",
} as const;

export type RTVIEventType = (typeof RTVIEvent)[keyof typeof RTVIEvent];

// Transport state enum
export type TransportState =
	| "disconnected"
	| "connecting"
	| "connected"
	| "ready";

// Event callback map
export type EventCallbackMap = {
	[RTVIEvent.Connected]: () => void;
	[RTVIEvent.Disconnected]: () => void;
	[RTVIEvent.TransportStateChanged]: (state: string) => void;
	[RTVIEvent.UserTranscript]: (data: TranscriptData) => void;
	[RTVIEvent.BotLlmStarted]: () => void;
	[RTVIEvent.BotLlmText]: (data: BotLLMTextData) => void;
	[RTVIEvent.BotLlmStopped]: () => void;
	[RTVIEvent.ServerMessage]: (message: unknown) => void;
	[RTVIEvent.Error]: (error: unknown) => void;
	[RTVIEvent.DeviceError]: (error: unknown) => void;
	[RTVIEvent.TrackStarted]: (
		track: MediaStreamTrack,
		participant: { id: string; name: string; local: boolean },
	) => void;
	[RTVIEvent.TrackStopped]: (
		track: MediaStreamTrack,
		participant: { id: string; name: string; local: boolean },
	) => void;
};

/**
 * Unified interface for transport clients.
 * Both PipecatClient (WebRTC) and WebSocketClient must implement this interface.
 */
export interface TransportClient {
	/**
	 * Initialize audio/video devices (required before connect).
	 */
	initDevices(): Promise<void>;

	/**
	 * Connect to the server.
	 */
	connect(options: ConnectOptions): Promise<void>;

	/**
	 * Disconnect from the server and clean up resources.
	 */
	disconnect(): Promise<void>;

	/**
	 * Send a client message to the server (RTVI protocol).
	 */
	sendClientMessage(messageType: string, data: unknown): void;

	/**
	 * Register an event listener.
	 */
	on<K extends RTVIEventType>(event: K, callback: EventCallbackMap[K]): void;

	/**
	 * Unregister an event listener.
	 */
	off<K extends RTVIEventType>(event: K, callback: EventCallbackMap[K]): void;

	/**
	 * Emit an event to all registered listeners (for internal use).
	 */
	emit<K extends RTVIEventType>(
		event: K,
		...args: Parameters<EventCallbackMap[K]>
	): void;

	/**
	 * Get the current transport state.
	 */
	get transport(): { state: TransportState };

	/**
	 * Get tracks (may return null for WebSocket).
	 */
	tracks(): { local?: { audio?: MediaStreamTrack } } | null;

	/**
	 * Enable/disable microphone (may be no-op for WebSocket).
	 */
	enableMic?(enabled: boolean): void;

	/**
	 * Start capturing and streaming audio (WebSocket only).
	 */
	startAudioCapture?(track: MediaStreamTrack): Promise<void>;

	/**
	 * Stop capturing audio (WebSocket only).
	 */
	stopAudioCapture?(): void;
}

/**
 * Connection options that can vary by transport type.
 */
export type ConnectOptions = WebRTCConnectOptions | WebSocketConnectOptions;

export interface WebRTCConnectOptions {
	webrtcRequestParams: {
		endpoint: string;
		requestData?: Record<string, unknown>;
	};
}

export interface WebSocketConnectOptions {
	websocketUrl: string;
}

/**
 * Type guard to check if options are for WebRTC.
 */
export function isWebRTCConnectOptions(
	options: ConnectOptions,
): options is WebRTCConnectOptions {
	return "webrtcRequestParams" in options;
}

/**
 * Type guard to check if options are for WebSocket.
 */
export function isWebSocketConnectOptions(
	options: ConnectOptions,
): options is WebSocketConnectOptions {
	return "websocketUrl" in options;
}
