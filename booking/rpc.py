import logging
import os
import threading
import time
from collections import namedtuple

import grpc

import flights_pb2_grpc


log = logging.getLogger(__name__)
TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
BREAKER_FAILURES = TRANSIENT | {grpc.StatusCode.INTERNAL, grpc.StatusCode.UNKNOWN, grpc.StatusCode.DATA_LOSS}


class CircuitOpen(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "Flight service is temporarily unavailable; try again later"


class CircuitBreaker:
    def __init__(self, threshold, reset_timeout, window):
        self.threshold = threshold
        self.reset_timeout = reset_timeout
        self.window = window
        self.state = "CLOSED"
        self.failures = 0
        self.last_failure = 0
        self.opened_at = 0
        self.generation = 0
        self.lock = threading.Lock()

    def transition(self, state):
        log.warning("circuit breaker %s -> %s", self.state, state)
        self.state = state
        self.generation += 1

    def before_call(self):
        with self.lock:
            if self.state == "OPEN":
                if time.monotonic() - self.opened_at < self.reset_timeout:
                    raise CircuitOpen()
                self.transition("HALF_OPEN")
            elif self.state == "HALF_OPEN":
                raise CircuitOpen()
            return self.generation, self.state == "HALF_OPEN"

    def after_call(self, generation, failed):
        with self.lock:
            if generation != self.generation:
                return
            now = time.monotonic()
            if not failed:
                self.failures = 0
                if self.state == "HALF_OPEN":
                    self.transition("CLOSED")
                return
            if now - self.last_failure > self.window:
                self.failures = 0
            self.last_failure = now
            self.failures += 1
            if self.state == "HALF_OPEN" or self.failures >= self.threshold:
                self.opened_at = now
                self.transition("OPEN")


class CallDetails(namedtuple("CallDetails", "method timeout metadata credentials wait_for_ready compression"), grpc.ClientCallDetails):
    pass


class ResilienceInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, api_key, breaker, timeout=2, backoff=0.1):
        self.api_key = api_key
        self.breaker = breaker
        self.timeout = timeout
        self.backoff = backoff

    def intercept_unary_unary(self, continuation, details, request):
        generation, probe = self.breaker.before_call()
        metadata = tuple(details.metadata or ()) + (("x-api-key", self.api_key),)
        details = CallDetails(details.method, details.timeout or self.timeout, metadata,
                              details.credentials, details.wait_for_ready,
                              getattr(details, "compression", None))
        attempts = 1 if probe else 3
        for attempt in range(attempts):
            try:
                call = continuation(details, request)
                call.result()
            except grpc.RpcError as error:
                transient = error.code() in TRANSIENT
                if transient and attempt + 1 < attempts:
                    delay = self.backoff * 2 ** attempt
                    log.warning("grpc retry method=%s attempt=%s delay=%.2f code=%s",
                                details.method, attempt + 2, delay, error.code().name)
                    time.sleep(delay)
                    continue
                self.breaker.after_call(generation, error.code() in BREAKER_FAILURES)
                raise
            except Exception:
                self.breaker.after_call(generation, True)
                raise
            self.breaker.after_call(generation, False)
            return call


class FlightClient:
    def __init__(self):
        api_key = os.environ["GRPC_API_KEY"]
        if not api_key:
            raise ValueError("GRPC_API_KEY must not be empty")
        threshold = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
        reset_timeout = float(os.getenv("CB_RESET_TIMEOUT_SECONDS", "10"))
        window = float(os.getenv("CB_WINDOW_SECONDS", "30"))
        timeout = float(os.getenv("GRPC_TIMEOUT_SECONDS", "2"))
        backoff = float(os.getenv("GRPC_RETRY_BACKOFF_SECONDS", "0.1"))
        if min(threshold, reset_timeout, window, timeout, backoff) <= 0:
            raise ValueError("gRPC and circuit breaker settings must be positive")
        self.breaker = CircuitBreaker(threshold, reset_timeout, window)
        self.channel = grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight:50051"),
                                             options=(("grpc.enable_retries", 0),
                                                      ("grpc.initial_reconnect_backoff_ms", 100),
                                                      ("grpc.min_reconnect_backoff_ms", 100),
                                                      ("grpc.max_reconnect_backoff_ms", 1000)))
        interceptor = ResilienceInterceptor(api_key, self.breaker, timeout, backoff)
        self.stub = flights_pb2_grpc.FlightServiceStub(grpc.intercept_channel(self.channel, interceptor))

    def close(self):
        self.channel.close()
