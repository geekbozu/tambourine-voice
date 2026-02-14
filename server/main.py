#!/usr/bin/env python3
"""Tambourine Server - WebSocket-based Pipecat Server.

A FastAPI server that receives audio from a Tauri client via WebSocket,
processes it through STT and LLM formatting, and returns formatted text.

Usage:
    python main.py
    python main.py --port 8765
"""

import asyncio
from collections.abc import Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, cast

import typer
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from pipecat.audio.vad.silero import SileroVADAnalyzer, VADParams
from pipecat.frames.frames import HeartbeatFrame
from pipecat.observers.user_bot_latency_observer import UserBotLatencyObserver
from pipecat.pipeline.llm_switcher import LLMSwitcher
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.service_switcher import ServiceSwitcher, ServiceSwitcherStrategyManual
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frameworks.rtvi import RTVIProcessor
from pipecat.services.llm_service import LLMService
from pipecat.services.stt_service import STTService
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from api.config_api import config_router
from config.settings import Settings
from processors.client_manager import ClientConnectionManager
from processors.configuration import ConfigurationHandler
from processors.context_manager import DictationContextManager
from processors.llm_gate import LLMGateFilter
from processors.turn_controller import TurnController
from processors.vad_forwarding_processor import VADFrameForwardingProcessor
from protocol.messages import (
    SetLLMProviderMessage,
    SetSTTProviderMessage,
    StartRecordingMessage,
    StopRecordingMessage,
    UnknownClientMessage,
    parse_client_message,
    parse_rtvi_client_message_payload,
)
from services.providers import (
    LLMProviderId,
    STTProviderId,
    create_all_available_llm_services,
    create_all_available_stt_services,
    get_available_llm_providers,
    get_available_stt_providers,
)
from utils.logger import configure_logging
from utils.observers import PipelineLogObserver
from utils.rate_limiter import (
    RATE_LIMIT_HEALTH,
    RATE_LIMIT_REGISTRATION,
    RATE_LIMIT_VERIFY,
    RATE_LIMIT_WEBSOCKET,
    get_ip_only,
    limiter,
)

# Set to hold background tasks to prevent garbage collection before completion
_background_tasks: set[asyncio.Task[None]] = set()


def create_background_task(coroutine: Coroutine[object, object, None]) -> asyncio.Task[None]:
    """Create a background task that won't be garbage collected before completion.

    Args:
        coroutine: An awaitable coroutine to run as a background task

    Returns:
        The created asyncio Task
    """
    task = asyncio.create_task(coroutine)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


async def reject_websocket(websocket: WebSocket, code: int, reason: str) -> None:
    """Reject a WebSocket connection with a close code and reason.

    Accepts the WebSocket first to establish the connection, then immediately
    closes it with the given code and reason. This ensures the client receives
    the close frame with the reason (RFC 6455 compliance).

    Args:
        websocket: The WebSocket connection to reject
        code: The WebSocket close code (e.g., 1008 for policy violation)
        reason: Human-readable reason for rejection
    """
    await websocket.accept()
    await websocket.close(code=code, reason=reason)


def create_silero_vad_params(settings: Settings) -> VADParams:
    """Build Silero VAD params from application settings.

    Args:
        settings: Application settings containing optional VAD overrides.

    Returns:
        VADParams instance populated from settings.
    """
    vad_params_kwargs: dict[str, float] = {}
    if settings.vad_confidence is not None:
        vad_params_kwargs["confidence"] = settings.vad_confidence
    if settings.vad_start_secs is not None:
        vad_params_kwargs["start_secs"] = settings.vad_start_secs
    if settings.vad_stop_secs is not None:
        vad_params_kwargs["stop_secs"] = settings.vad_stop_secs
    if settings.vad_min_volume is not None:
        vad_params_kwargs["min_volume"] = settings.vad_min_volume

    return VADParams(**vad_params_kwargs)


@dataclass
class AppServices:
    """Container for application services, stored on app.state.

    Note: STT and LLM services are created per-connection in run_pipeline()
    to ensure complete isolation between concurrent clients. Each client
    gets fresh service instances with independent WebSocket connections.

    The available_stt_providers and available_llm_providers lists are
    pre-computed at startup since Settings is immutable after initialization.
    """

    settings: Settings
    active_pipeline_tasks: set[asyncio.Task[None]]
    client_manager: ClientConnectionManager
    available_stt_providers: list[STTProviderId]
    available_llm_providers: list[LLMProviderId]


async def run_pipeline(
    websocket: WebSocket,
    services: AppServices,
    *,
    stt_services: dict[STTProviderId, STTService],
    llm_services: dict[LLMProviderId, LLMService],
    context_manager: DictationContextManager,
    turn_controller: TurnController,
    llm_gate: LLMGateFilter,
    vad_analyzer: SileroVADAnalyzer,
) -> None:
    """Run the Pipecat pipeline for a single WebSocket connection.

    Args:
        websocket: The WebSocket connection for this client
        services: Application services container
        stt_services: Pre-created STT services for this connection
        llm_services: Pre-created LLM services for this connection
        context_manager: Pre-created context manager for this connection
        turn_controller: Pre-created turn controller for this connection
        llm_gate: Pre-created LLM gate filter for this connection
        vad_analyzer: Pre-created VAD analyzer for this connection
    """
    logger.info("Starting pipeline for new WebSocket connection")

    # Create transport using the WebSocket connection
    logger.info(
        "SileroVADAnalyzer configured with "
        f"params={vad_analyzer.params.model_dump(exclude_none=True)}"
    )

    # Initialize Pipecat transport with WebSocket
    # add_wav_header=False: Server doesn't need WAV headers for raw PCM audio streaming.
    # The client sends 16-bit PCM at 16kHz directly, which Pipecat processes natively.
    # WAV headers are only needed when writing to files or passing to non-PCM-aware systems.
    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=False,  # No audio output for dictation
            add_wav_header=False,
            vad_enabled=True,
            vad_analyzer=vad_analyzer,
            vad_audio_passthrough=True,
        ),
    )
    vad_frame_forwarder = VADFrameForwardingProcessor(vad_analyzer=vad_analyzer)

    # Create service switchers for this connection
    from pipecat.pipeline.base_pipeline import FrameProcessor as PipecatFrameProcessor

    stt_service_list = cast(list[PipecatFrameProcessor], list(stt_services.values()))
    llm_service_list = list(llm_services.values())

    stt_switcher = ServiceSwitcher(
        services=stt_service_list,
        strategy_type=ServiceSwitcherStrategyManual,
    )

    llm_switcher = LLMSwitcher(
        llms=llm_service_list,
        strategy_type=ServiceSwitcherStrategyManual,
    )

    # Build pipeline - Pipecat 0.0.101+ handles RTVI automatically via task.rtvi
    # The aggregator pair from context_manager collects transcriptions and LLM responses
    pipeline = Pipeline(
        [
            transport.input(),
            vad_frame_forwarder,
            stt_switcher,
            turn_controller,  # Controls turn boundaries, passes transcriptions through
            llm_gate,  # Gates frames to aggregator based on LLM formatting setting
            context_manager.user_aggregator(),  # Collects transcriptions, emits LLMContextFrame
            llm_switcher,
            context_manager.assistant_aggregator(),  # Collects LLM responses
            transport.output(),
        ]
    )

    user_bot_latency_observer = UserBotLatencyObserver()

    @user_bot_latency_observer.event_handler("on_latency_measured")
    async def on_latency_measured(
        observer: UserBotLatencyObserver,
        latency_seconds: float,
    ) -> None:
        """Log measured user-to-bot latency."""
        _ = observer
        logger.debug(
            f"⏱️ LATENCY FROM USER STOPPED SPEAKING TO BOT STARTED SPEAKING: {latency_seconds:.3f}s"
        )

    # Create pipeline task - RTVI is automatically enabled and accessible via task.rtvi
    # This avoids duplicate RTVIObservers that caused text duplication in 0.0.101
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=False,
            enable_metrics=True,
            enable_usage_metrics=True,
            enable_heartbeats=True,
        ),
        idle_timeout_frames=(HeartbeatFrame,),
        observers=[
            user_bot_latency_observer,
            PipelineLogObserver(),
        ],
    )

    # ConfigurationHandler processes provider switching messages from RTVI client
    # Note: State-only config (prompts, timeouts) is now handled via HTTP API
    config_handler = ConfigurationHandler(
        rtvi_processor=task.rtvi,
        stt_switcher=stt_switcher,
        llm_switcher=llm_switcher,
        stt_services=stt_services,
        llm_services=llm_services,
        settings=services.settings,
    )

    # Register event handler for client messages on the RTVI processor
    @task.rtvi.event_handler("on_client_message")
    async def on_client_message(processor: RTVIProcessor, message: object) -> None:
        """Handle RTVI client messages for configuration and recording control."""
        _ = processor  # Unused, required by event handler signature

        raw_data = parse_rtvi_client_message_payload(message)
        if raw_data is None:
            return

        # Use forward-compatible parser (never returns None)
        parsed = parse_client_message(raw_data)

        # Handle the typed message with exhaustive pattern matching
        match parsed:
            case StartRecordingMessage():
                active_app_context_for_recording = parsed.active_app_context_for_recording()
                logger.info(
                    f"Start-recording received active app context: {active_app_context_for_recording}"
                )
                context_manager.set_active_app_context(active_app_context_for_recording)
                llm_gate.reset_for_recording()
                await context_manager.reset_aggregator()
                await turn_controller.start_recording()
            case StopRecordingMessage():
                await turn_controller.stop_recording()
            case SetSTTProviderMessage() | SetLLMProviderMessage():
                await config_handler.handle_config_message(parsed)
            case UnknownClientMessage():
                pass  # Already logged at debug level in parse_client_message

    # Set up event handlers
    @transport.event_handler("on_client_connected")
    async def on_client_connected(_transport: object, client: object) -> None:
        logger.success(f"Client connected via WebSocket: {client}")

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(_transport: object, client: object) -> None:
        logger.info(f"Client disconnected: {client}")
        await task.cancel()

    # Run the pipeline
    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)


def initialize_services(settings: Settings) -> AppServices | None:
    """Initialize application services container.

    Validates that at least one STT and LLM provider is available.
    Actual service instances are created per-connection in run_pipeline()
    to ensure complete isolation between concurrent clients.

    Args:
        settings: Application settings

    Returns:
        AppServices instance if successful, None otherwise
    """
    available_stt = get_available_stt_providers(settings)
    available_llm = get_available_llm_providers(settings)

    if not available_stt:
        logger.error("No STT providers available. Configure at least one STT API key.")
        return None

    if not available_llm:
        logger.error("No LLM providers available. Configure at least one LLM API key.")
        return None

    logger.info(f"Available STT providers: {[p.value for p in available_stt]}")
    logger.info(f"Available LLM providers: {[p.value for p in available_llm]}")

    return AppServices(
        settings=settings,
        active_pipeline_tasks=set(),
        client_manager=ClientConnectionManager(),
        available_stt_providers=available_stt,
        available_llm_providers=available_llm,
    )


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):  # noqa: ANN201
    """FastAPI lifespan context manager for cleanup."""
    yield
    logger.info("Shutting down server...")

    # Get services from app state (may not exist if startup failed)
    services: AppServices | None = getattr(fastapi_app.state, "services", None)
    if services is None:
        logger.warning("Services not initialized, skipping cleanup")
        return

    # Cancel all active pipeline tasks for graceful shutdown
    if services.active_pipeline_tasks:
        logger.info(f"Cancelling {len(services.active_pipeline_tasks)} active pipeline tasks...")
        for task in list(services.active_pipeline_tasks):
            task.cancel()
        # Wait for all tasks to complete with timeout to avoid hanging
        try:
            async with asyncio.timeout(5.0):
                await asyncio.gather(*services.active_pipeline_tasks, return_exceptions=True)
            logger.info("All pipeline tasks cancelled")
        except TimeoutError:
            logger.warning("Timeout waiting for pipeline tasks to cancel")

    logger.success("All connections cleaned up")


# Create FastAPI app
app = FastAPI(title="Tambourine Server", lifespan=lifespan)

# Add rate limiter to app state
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

app.add_middleware(
    CORSMiddleware,  # type: ignore[invalid-argument-type]
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler to ensure CORS headers are included in error responses.
# FastAPI's CORSMiddleware may not add headers to unhandled exception responses,
# causing misleading "CORS errors".
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Ensure CORS headers are included even in error responses."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
            "Access-Control-Allow-Headers": "*",
        },
    )


# Include config routes
app.include_router(config_router)


@app.get("/health")
@limiter.limit(RATE_LIMIT_HEALTH, key_func=get_ip_only)
async def health_check(request: Request) -> dict[str, str]:
    """Health check endpoint for container orchestration (e.g., Lightsail)."""
    return {"status": "ok"}


# =============================================================================
# Client Registration Endpoints
# =============================================================================


@app.post("/api/client/register")
@limiter.limit(RATE_LIMIT_REGISTRATION, key_func=get_ip_only)
async def register_client(request: Request) -> dict[str, str]:
    """Generate, register, and return a new client UUID.

    This endpoint is called by clients on first connection or when their
    stored UUID is rejected (e.g., after server restart).

    Rate limited by IP to prevent mass UUID registration attacks.

    Returns:
        A dictionary containing the newly generated UUID.
    """
    services: AppServices = request.app.state.services
    client_uuid = services.client_manager.generate_and_register_uuid()
    logger.success(f"Registered new client: {client_uuid}")
    return {"uuid": client_uuid}


@app.get("/api/client/verify/{client_uuid}")
@limiter.limit(RATE_LIMIT_VERIFY, key_func=get_ip_only)
async def verify_client(client_uuid: str, request: Request) -> dict[str, bool]:
    """Verify if a client UUID is registered with the server.

    This endpoint allows clients to check if their stored UUID is still valid
    (e.g., after server restart where in-memory registrations are lost).

    Rate limited by IP to prevent UUID enumeration attacks.

    Returns:
        A dictionary with 'registered' boolean indicating if UUID is valid.
    """
    services: AppServices = request.app.state.services
    is_registered = services.client_manager.is_registered(client_uuid)
    return {"registered": is_registered}


# =============================================================================
# WebSocket Endpoint
# =============================================================================


@app.websocket("/ws")
@limiter.limit(RATE_LIMIT_WEBSOCKET, key_func=get_ip_only)
async def websocket_endpoint(websocket: WebSocket, request: Request) -> None:
    """Handle WebSocket connection from client.

    This endpoint handles the WebSocket connection:
    1. Validates client UUID from query parameters
    2. Accepts the WebSocket connection
    3. Disconnects any existing connection with the same UUID
    4. Spawns the Pipecat pipeline

    Rate limited by IP. Normal client usage won't hit the limit,
    but attackers spamming connection attempts will be blocked.
    """
    services: AppServices = request.app.state.services

    # Extract client UUID from query parameters
    client_uuid = websocket.query_params.get("clientUUID")
    logger.info(f"Incoming client UUID: {client_uuid}")

    # Require UUID - clients must register first
    if not client_uuid:
        logger.warning("Rejected connection without client UUID")
        # Use 1002 (Protocol Error) for missing required query parameter
        await reject_websocket(websocket, 1002, "Client UUID required. Please register first.")
        return

    # Validate UUID is registered
    if not services.client_manager.is_registered(client_uuid):
        logger.warning(f"Rejected unregistered client UUID: {client_uuid}")
        # Use 1008 (Policy Violation) for unregistered/invalid UUID
        await reject_websocket(websocket, 1008, "Unregistered client UUID. Please register first.")
        return

    # Handle existing connection with same UUID (one client = one connection)
    old_connection = services.client_manager.take_existing_connection(client_uuid)
    if old_connection:
        create_background_task(services.client_manager.cleanup_connection(old_connection))
    logger.info(f"Client connecting with UUID: {client_uuid}")

    # Accept the WebSocket connection
    await websocket.accept()

    # Create fresh service instances for this connection to ensure isolation
    vad_params = create_silero_vad_params(services.settings)
    vad_analyzer = SileroVADAnalyzer(params=vad_params)
    context_manager = DictationContextManager()
    logger.info(
        "SileroVADAnalyzer configured with "
        f"params={vad_analyzer.params.model_dump(exclude_none=True)}"
    )
    stt_services = create_all_available_stt_services(
        services.settings,
        services.available_stt_providers,
    )
    llm_services = create_all_available_llm_services(
        services.settings,
        services.available_llm_providers,
    )

    # Create pipeline processors
    turn_controller = TurnController()
    llm_gate = LLMGateFilter()
    # Wire up turn controller to context manager for context reset coordination
    turn_controller.set_context_manager(context_manager)

    # Create and track pipeline task
    task = asyncio.create_task(
        run_pipeline(
            websocket,
            services,
            stt_services=stt_services,
            llm_services=llm_services,
            context_manager=context_manager,
            turn_controller=turn_controller,
            llm_gate=llm_gate,
            vad_analyzer=vad_analyzer,
        )
    )
    services.active_pipeline_tasks.add(task)
    task.add_done_callback(services.active_pipeline_tasks.discard)

    # Track connection by UUID with component references for HTTP API access
    services.client_manager.register_connection(
        client_uuid,
        websocket,
        task,
        context_manager=context_manager,
        turn_controller=turn_controller,
        llm_gate=llm_gate,
        stt_services=stt_services,
        llm_services=llm_services,
    )

    # Wait for the pipeline to complete
    try:
        await task
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        # Unregister the connection if it still exists
        # (it may have been removed during cleanup)
        if services.client_manager.get_connection(client_uuid) is not None:
            services.client_manager.unregister_connection(client_uuid)


def main(
    host: Annotated[str | None, typer.Option(help="Host to bind to")] = None,
    port: Annotated[int | None, typer.Option(help="Port to listen on")] = None,
    verbose: Annotated[
        bool, typer.Option("-v", "--verbose", help="Enable verbose logging")
    ] = False,
) -> None:
    """Tambourine Server - Voice dictation with AI cleanup."""
    # Load settings first so we can use them as defaults
    try:
        settings = Settings()
    except Exception as e:
        print(f"Configuration error: {e}")
        print("Please check your .env file and ensure all required API keys are set.")
        print("See .env.example for reference.")
        raise SystemExit(1) from e

    # Use settings defaults if not provided via CLI
    effective_host = host or settings.host
    effective_port = port or settings.port

    # Configure logging
    log_level = "DEBUG" if verbose else None
    configure_logging(log_level)

    if verbose:
        logger.debug("Verbose logging enabled")

    # Initialize services and store on app.state
    services = initialize_services(settings)
    if services is None:
        raise SystemExit(1)
    app.state.services = services

    logger.info("=" * 60)
    logger.success("Tambourine Server Ready!")
    logger.info("=" * 60)
    logger.info(f"Server endpoint: http://{effective_host}:{effective_port}")
    logger.info(f"WebSocket endpoint: ws://{effective_host}:{effective_port}/ws")
    logger.info(f"Config API endpoint: http://{effective_host}:{effective_port}/api/*")
    logger.info("Waiting for Tauri client connection...")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)

    # Run the server
    uvicorn.run(
        app,
        host=effective_host,
        port=effective_port,
        log_level="warning",
    )


if __name__ == "__main__":
    typer.run(main)
