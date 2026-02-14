/**
 * Custom WebSocket client for audio streaming and RTVI protocol communication.
 * Implements the unified TransportClient interface.
 */

import type { BotLLMTextData, TranscriptData } from "@pipecat-ai/client-js";
import type {
	ConnectOptions,
	EventCallbackMap,
	RTVIEventType,
	TransportClient,
	TransportState,
} from "./TransportClient";
import { isWebSocketConnectOptions, RTVIEvent } from "./TransportClient";

/**
 * WebSocket client with Web Audio API for audio capture and streaming.
 * Implements the unified TransportClient interface.
 */
export class WebSocketClient implements TransportClient {
	private ws: WebSocket | null = null;
	private state: TransportState = "disconnected";
	private eventListeners: Map<
		RTVIEventType,
		Set<(...args: unknown[]) => void>
	> = new Map();

	// Audio context and nodes for capturing microphone input
	private audioContext: AudioContext | null = null;
	private audioSource: MediaStreamAudioSourceNode | null = null;
	private audioProcessor: ScriptProcessorNode | null = null;
	private micStream: MediaStream | null = null;

	/**
	 * Initialize audio devices (required before connect for compatibility).
	 * Does not actually capture audio yet.
	 */
	async initDevices(): Promise<void> {
		// Initialize audio context but don't request mic permission yet
		this.audioContext = new (
			window.AudioContext ||
			(window as unknown as { webkitAudioContext: typeof AudioContext })
				.webkitAudioContext
		)();

		// Keep audio context in suspended state until recording starts
		if (this.audioContext.state !== "suspended") {
			await this.audioContext.suspend();
		}

		console.debug("[WebSocketClient] Audio devices initialized");
	}

	/**
	 * Connect to the WebSocket server.
	 */
	async connect(options: ConnectOptions): Promise<void> {
		if (this.ws) {
			throw new Error("Already connected or connecting");
		}

		if (!isWebSocketConnectOptions(options)) {
			throw new Error("Invalid connect options for WebSocket client");
		}

		const { websocketUrl } = options;

		return new Promise((resolve, reject) => {
			this.setState("connecting");

			try {
				this.ws = new WebSocket(websocketUrl);

				// Binary data is audio or protocol messages
				this.ws.binaryType = "arraybuffer";

				this.ws.onopen = () => {
					console.debug("[WebSocketClient] WebSocket connected");
					this.setState("connected");
					// Transition to ready state immediately for WebSocket
					// (no separate data channel setup like WebRTC)
					this.setState("ready");
					this.emit(RTVIEvent.Connected);
					resolve();
				};

				this.ws.onmessage = (event) => {
					this.handleMessage(event.data);
				};

				this.ws.onerror = (error) => {
					console.error("[WebSocketClient] WebSocket error:", error);
					this.emit(RTVIEvent.Error, {
						data: { message: "WebSocket error", fatal: false },
					});
					reject(new Error("WebSocket connection failed"));
				};

				this.ws.onclose = () => {
					console.debug("[WebSocketClient] WebSocket closed");
					this.cleanup();
					this.emit(RTVIEvent.Disconnected);
				};
			} catch (error) {
				this.setState("disconnected");
				reject(error);
			}
		});
	}

	/**
	 * Disconnect from the server and clean up resources.
	 */
	async disconnect(): Promise<void> {
		console.debug("[WebSocketClient] Disconnecting...");
		this.cleanup();
	}

	/**
	 * Send a client message to the server (RTVI protocol).
	 */
	sendClientMessage(messageType: string, data: unknown): void {
		if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
			throw new Error("WebSocket not connected");
		}

		const message = {
			type: messageType,
			data,
		};

		this.ws.send(JSON.stringify(message));
		console.debug(`[WebSocketClient] Sent message: ${messageType}`, data);
	}

	/**
	 * Start capturing and streaming audio from the given track.
	 * This replaces the WebRTC track management with Web Audio API.
	 */
	async startAudioCapture(track: MediaStreamTrack): Promise<void> {
		if (!this.audioContext) {
			throw new Error("Audio context not initialized");
		}

		// Resume audio context if suspended
		if (this.audioContext.state === "suspended") {
			await this.audioContext.resume();
		}

		// Create media stream from track
		this.micStream = new MediaStream([track]);
		this.audioSource = this.audioContext.createMediaStreamSource(
			this.micStream,
		);

		// Create processor node for capturing audio data
		// ScriptProcessorNode is deprecated, but AudioWorklet is more complex to set up
		// and requires a separate worklet file. For this use case, ScriptProcessorNode
		// works reliably across all platforms and browsers.
		// TODO: Migrate to AudioWorklet for better performance and to avoid deprecation warnings
		// See: https://developer.mozilla.org/en-US/docs/Web/API/AudioWorkletNode
		const bufferSize = 4096;
		this.audioProcessor = this.audioContext.createScriptProcessor(
			bufferSize,
			1,
			1,
		);

		this.audioProcessor.onaudioprocess = (event) => {
			const inputBuffer = event.inputBuffer;
			const inputData = inputBuffer.getChannelData(0);

			// Convert Float32 PCM to Int16 PCM for transmission
			// Audio data from Web Audio API is Float32 in range [-1.0, 1.0]
			// Server expects 16-bit signed integer PCM in range [-32768, 32767]
			const MAX_INT16 = 0x7fff; // 32767 (maximum positive value)
			const MIN_INT16_MAGNITUDE = 0x8000; // 32768 (magnitude for minimum negative value)

			const pcmData = new Int16Array(inputData.length);
			for (let i = 0; i < inputData.length; i++) {
				// Clamp to [-1, 1] and convert to 16-bit signed integer
				// Negative samples: multiply by 32768, positive samples: multiply by 32767
				const sample = inputData[i] ?? 0;
				const s = Math.max(-1, Math.min(1, sample));
				pcmData[i] = s < 0 ? s * MIN_INT16_MAGNITUDE : s * MAX_INT16;
			}

			// Send audio data over WebSocket as binary
			if (this.ws && this.ws.readyState === WebSocket.OPEN) {
				this.ws.send(pcmData.buffer);
			}
		};

		// Connect audio nodes
		this.audioSource.connect(this.audioProcessor);
		this.audioProcessor.connect(this.audioContext.destination);

		console.debug("[WebSocketClient] Audio capture started");
	}

	/**
	 * Stop capturing audio.
	 */
	stopAudioCapture(): void {
		if (this.audioProcessor) {
			this.audioProcessor.disconnect();
			this.audioProcessor = null;
		}

		if (this.audioSource) {
			this.audioSource.disconnect();
			this.audioSource = null;
		}

		if (this.micStream) {
			for (const track of this.micStream.getTracks()) {
				track.stop();
			}
			this.micStream = null;
		}

		console.debug("[WebSocketClient] Audio capture stopped");
	}

	/**
	 * Handle incoming WebSocket messages.
	 */
	private handleMessage(data: string | ArrayBuffer): void {
		// Binary data could be audio from server
		if (data instanceof ArrayBuffer) {
			// Binary message - unexpected for dictation-only mode
			// Server sends no audio back (audio_out_enabled=False), so log if we receive binary data
			console.warn(
				"[WebSocketClient] Received unexpected binary message from server",
			);
			return;
		}

		// Text messages are JSON protocol messages
		try {
			const message = JSON.parse(data as string);
			this.handleProtocolMessage(message);
		} catch (error) {
			console.warn("[WebSocketClient] Failed to parse message:", error);
		}
	}

	/**
	 * Handle RTVI protocol messages from server.
	 */
	private handleProtocolMessage(message: {
		type: string;
		data?: unknown;
	}): void {
		const { type, data } = message;

		switch (type) {
			case "user-transcript":
				this.emit(RTVIEvent.UserTranscript, data as TranscriptData);
				break;

			case "bot-llm-started":
				this.emit(RTVIEvent.BotLlmStarted);
				break;

			case "bot-llm-text":
				this.emit(RTVIEvent.BotLlmText, data as BotLLMTextData);
				break;

			case "bot-llm-stopped":
				this.emit(RTVIEvent.BotLlmStopped);
				break;

			case "server-message":
				// Custom server messages (config updates, errors, etc.)
				this.emit(RTVIEvent.ServerMessage, data);
				break;

			case "error":
				this.emit(RTVIEvent.Error, data);
				break;

			case "recording-complete-with-zero-words":
			case "raw-transcription":
			case "config-updated":
			case "config-error":
				// These are custom server messages, forward as ServerMessage
				this.emit(RTVIEvent.ServerMessage, message);
				break;

			default:
				console.warn("[WebSocketClient] Unknown message type:", type);
				break;
		}
	}

	/**
	 * Register an event listener.
	 */
	on<K extends RTVIEventType>(event: K, callback: EventCallbackMap[K]): void {
		if (!this.eventListeners.has(event)) {
			this.eventListeners.set(event, new Set());
		}
		this.eventListeners
			.get(event)
			?.add(callback as (...args: unknown[]) => void);
	}

	/**
	 * Unregister an event listener.
	 */
	off<K extends RTVIEventType>(event: K, callback: EventCallbackMap[K]): void {
		this.eventListeners
			.get(event)
			?.delete(callback as (...args: unknown[]) => void);
	}

	/**
	 * Emit an event to all registered listeners.
	 */
	emit<K extends RTVIEventType>(
		event: K,
		...args: Parameters<EventCallbackMap[K]>
	): void {
		const listeners = this.eventListeners.get(event);
		if (listeners) {
			for (const listener of listeners) {
				try {
					listener(...args);
				} catch (error) {
					console.error(`[WebSocketClient] Error in ${event} listener:`, error);
				}
			}
		}
	}

	/**
	 * Get the current transport state.
	 */
	get transportState(): TransportState {
		return this.state;
	}

	/**
	 * Set transport state and emit change event.
	 */
	private setState(newState: TransportState): void {
		if (this.state !== newState) {
			this.state = newState;
			this.emit(RTVIEvent.TransportStateChanged, newState);
		}
	}

	/**
	 * Clean up all resources.
	 */
	private cleanup(): void {
		this.stopAudioCapture();

		if (this.audioContext) {
			this.audioContext.close();
			this.audioContext = null;
		}

		if (this.ws) {
			this.ws.onopen = null;
			this.ws.onmessage = null;
			this.ws.onerror = null;
			this.ws.onclose = null;

			if (this.ws.readyState === WebSocket.OPEN) {
				this.ws.close();
			}
			this.ws = null;
		}

		this.setState("disconnected");
		console.debug("[WebSocketClient] Cleanup complete");
	}

	/**
	 * Transport state getter (for compatibility with PipecatClient).
	 */
	get transport(): { state: TransportState } {
		return { state: this.state };
	}

	/**
	 * Tracks getter (for compatibility - returns null since we manage tracks differently).
	 */
	tracks(): null {
		return null;
	}

	/**
	 * Enable/disable microphone (for compatibility - no-op since we handle this externally).
	 */
	enableMic(_enabled: boolean): void {
		// No-op: microphone is controlled via startAudioCapture/stopAudioCapture
	}
}
