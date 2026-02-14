import { useSelector } from "@xstate/react";
import { createContext, type ReactNode, useContext, useEffect } from "react";
import { match } from "ts-pattern";
import { createActor } from "xstate";
import type { TransportClient } from "../lib/TransportClient";
import { tauriAPI } from "../lib/tauri";
import {
	type ConnectionMachineActor,
	type ConnectionMachineStateValue,
	connectionMachine,
} from "../machines/connectionMachine";

/**
 * React context for the XState connection machine.
 *
 * The actor is created as a singleton outside of React so:
 * - State persists across component remounts
 * - The machine can be accessed from anywhere in the app
 * - Multiple components can subscribe to the same state
 */

// Create a singleton actor instance (lives outside React lifecycle)
const connectionActor = createActor(connectionMachine);
connectionActor.start();

// Context to distribute the actor reference
const ConnectionContext = createContext<ConnectionMachineActor | null>(null);

interface ConnectionProviderProps {
	children: ReactNode;
}

export function ConnectionProvider({ children }: ConnectionProviderProps) {
	// Trigger initial connection on mount
	useEffect(() => {
		const initConnection = async () => {
			const settings = await tauriAPI.getSettings();
			const serverUrl = settings.server_url;
			const transportType = settings.transport_type || "webrtc"; // Default to webrtc
			if (serverUrl) {
				connectionActor.send({ type: "CONNECT", serverUrl, transportType });
			}
		};

		initConnection();
	}, []);

	useEffect(() => {
		let effectWasCleanedUp = false;
		let unsubscribe: (() => void) | undefined;

		const subscribeToReconnectEvents = async () => {
			const unsubscribeFn = await tauriAPI.onReconnect(() => {
				console.log("[XState] Manual reconnect requested");
				connectionActor.send({ type: "RECONNECT" });
			});

			if (effectWasCleanedUp) {
				unsubscribeFn();
			} else {
				unsubscribe = unsubscribeFn;
			}
		};

		subscribeToReconnectEvents();

		return () => {
			effectWasCleanedUp = true;
			unsubscribe?.();
		};
	}, []);

	useEffect(() => {
		let effectWasCleanedUp = false;
		let unsubscribe: (() => void) | undefined;

		const subscribeToSettingsChanges = async () => {
			const unsubscribeFn = await tauriAPI.onSettingsChanged(async () => {
				const settings = await tauriAPI.getSettings();
				const newServerUrl = settings.server_url;
				const newTransportType = settings.transport_type || "webrtc";
				const currentState = connectionActor.getSnapshot();
				const shouldHandleChange = match(
					currentState.value as ConnectionMachineStateValue,
				)
					.with(
						"idle",
						"startingRecording",
						"syncing",
						"recording",
						"processing",
						"retrying",
						() => true,
					)
					.with("disconnected", "initializing", "connecting", () => false)
					.exhaustive();
				const serverUrlChanged =
					newServerUrl && newServerUrl !== currentState.context.serverUrl;
				const transportTypeChanged =
					newTransportType !== currentState.context.transportType;

				if (shouldHandleChange && serverUrlChanged) {
					console.log("[XState] Server URL changed, reconnecting");
					connectionActor.send({
						type: "SERVER_URL_CHANGED",
						serverUrl: newServerUrl,
					});
				} else if (shouldHandleChange && transportTypeChanged) {
					console.log("[XState] Transport type changed, reconnecting");
					connectionActor.send({
						type: "TRANSPORT_TYPE_CHANGED",
						transportType: newTransportType,
					});
				}
			});

			if (effectWasCleanedUp) {
				unsubscribeFn();
			} else {
				unsubscribe = unsubscribeFn;
			}
		};

		subscribeToSettingsChanges();

		return () => {
			effectWasCleanedUp = true;
			unsubscribe?.();
		};
	}, []);

	return (
		<ConnectionContext.Provider value={connectionActor}>
			{children}
		</ConnectionContext.Provider>
	);
}

/**
 * Hook to get the raw connection actor reference.
 * Use this when you need to send events or subscribe to specific parts of the state.
 */
export function useConnectionActor(): ConnectionMachineActor {
	const actor = useContext(ConnectionContext);
	if (!actor) {
		throw new Error(
			"useConnectionActor must be used within ConnectionProvider",
		);
	}
	return actor;
}

/**
 * Hook to get the current connection state value.
 * Automatically re-renders when the state changes.
 */
export function useConnectionState(): ConnectionMachineStateValue {
	const actor = useConnectionActor();
	return useSelector(actor, (state) => state.value);
}

/**
 * Hook to get the current TransportClient instance (WebRTC or WebSocket).
 * Returns null when not connected.
 */
export function useConnectionClient(): TransportClient | null {
	const actor = useConnectionActor();
	return useSelector(actor, (state) => state.context.client);
}

/**
 * Hook to get the send function for dispatching events to the machine.
 */
export function useConnectionSend() {
	const actor = useConnectionActor();
	return actor.send;
}

// Export the actor for direct access in non-React code (like Zustand store)
export { connectionActor };
