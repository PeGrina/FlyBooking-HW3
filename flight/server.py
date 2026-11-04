import hmac
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import psycopg
from google.protobuf.empty_pb2 import Empty
from psycopg_pool import PoolTimeout

import flights_pb2 as pb
import flights_pb2_grpc as rpc
from common.db import create_pool, migrate
from flight.cache import Cache


log = logging.getLogger(__name__)
FLIGHT_SELECT = "SELECT f.*, a.name AS airline FROM flights f JOIN airlines a ON a.id = f.airline_id"


def valid_uuid(value, context):
    try:
        return UUID(value)
    except ValueError:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid UUID")


def flight_message(row):
    result = pb.Flight(
        id=str(row["id"]),
        flight_number=row["flight_number"],
        airline=row["airline"],
        origin=row["origin"],
        destination=row["destination"],
        total_seats=row["total_seats"],
        available_seats=row["available_seats"],
        price=str(row["price"]),
        status=pb.FlightStatus.Value(row["status"]),
    )
    result.departure_at.FromDatetime(row["departure_at"])
    result.arrival_at.FromDatetime(row["arrival_at"])
    return result


def reservation_message(row):
    return pb.SeatReservation(
        id=str(row["id"]),
        flight_id=str(row["flight_id"]),
        booking_id=str(row["booking_id"]),
        seat_count=row["seat_count"],
        status=pb.ReservationStatus.Value(row["status"]),
    )


class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, key):
        if not key:
            raise ValueError("GRPC_API_KEY is required")
        self.key = key.encode()

    def intercept_service(self, continuation, details):
        handler = continuation(details)
        if handler is None:
            return None
        supplied = dict(details.invocation_metadata).get("x-api-key", "").encode()

        def call(request, context):
            if not hmac.compare_digest(supplied, self.key):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid service API key")
            try:
                return handler.unary_unary(request, context)
            except (psycopg.Error, PoolTimeout):
                log.exception("Flight database unavailable")
                context.abort(grpc.StatusCode.UNAVAILABLE, "Flight database unavailable")

        return grpc.unary_unary_rpc_method_handler(
            call,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class FlightService(rpc.FlightServiceServicer):
    def __init__(self, pool, cache):
        self.pool = pool
        self.cache = cache

    def SearchFlights(self, request, context):
        for code in (request.origin, request.destination):
            if len(code) != 3 or not code.isascii() or not code.isalpha() or not code.isupper():
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Use three-letter uppercase IATA codes")
        date = None
        if request.HasField("date"):
            try:
                date = request.date.ToDatetime(tzinfo=timezone.utc).date()
            except (ValueError, OverflowError):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid departure date")
        with self.pool.connection() as conn:
            version = conn.execute("SELECT version FROM cache_state WHERE id = 1").fetchone()["version"]
            key = f"search:{request.origin}:{request.destination}:{date or 'all'}:v{version}"
            cached = self.cache.get(key, pb.SearchFlightsResponse)
            if cached is not None:
                return cached
            query = FLIGHT_SELECT + " WHERE f.origin = %s AND f.destination = %s AND f.status = 'SCHEDULED'"
            params = [request.origin, request.destination]
            if date is not None:
                query += " AND (f.departure_at AT TIME ZONE 'UTC')::date = %s"
                params.append(date)
            rows = conn.execute(query + " ORDER BY f.departure_at, f.id", params).fetchall()
        result = pb.SearchFlightsResponse(flights=[flight_message(row) for row in rows])
        self.cache.put(key, result)
        return result

    def GetFlight(self, request, context):
        flight_id = valid_uuid(request.id, context)
        with self.pool.connection() as conn:
            version = conn.execute("SELECT version FROM cache_state WHERE id = 1").fetchone()["version"]
            key = f"flight:{flight_id}:v{version}"
            cached = self.cache.get(key, pb.Flight)
            if cached is not None:
                return cached
            row = conn.execute(FLIGHT_SELECT + " WHERE f.id = %s", (flight_id,)).fetchone()
        if row is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
        result = flight_message(row)
        self.cache.put(key, result)
        return result

    def ReserveSeats(self, request, context):
        flight_id = valid_uuid(request.flight_id, context)
        booking_id = valid_uuid(request.booking_id, context)
        if request.seat_count <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "seat_count must be positive")
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (str(booking_id),))
            if conn.execute("SELECT 1 FROM reservation_cancellations WHERE booking_id = %s", (booking_id,)).fetchone():
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Reservation already released")
            existing = conn.execute("SELECT * FROM seat_reservations WHERE booking_id = %s", (booking_id,)).fetchone()
            if existing is not None:
                if existing["flight_id"] != flight_id or existing["seat_count"] != request.seat_count:
                    context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id belongs to another reservation")
                if existing["status"] != "ACTIVE":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Reservation is not active")
                return reservation_message(existing)
            flight = conn.execute("SELECT * FROM flights WHERE id = %s FOR UPDATE", (flight_id,)).fetchone()
            if flight is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            if flight["status"] != "SCHEDULED":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Flight is not scheduled")
            if flight["available_seats"] < request.seat_count:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Not enough available seats")
            conn.execute("UPDATE flights SET available_seats = available_seats - %s WHERE id = %s", (request.seat_count, flight_id))
            row = conn.execute(
                "INSERT INTO seat_reservations (id, flight_id, booking_id, seat_count, status) VALUES (%s, %s, %s, %s, 'ACTIVE') RETURNING *",
                (uuid4(), flight_id, booking_id, request.seat_count),
            ).fetchone()
            version = conn.execute("UPDATE cache_state SET version = version + 1 WHERE id = 1 RETURNING version - 1 AS previous").fetchone()["previous"]
        self.cache.invalidate(version)
        return reservation_message(row)

    def ReleaseReservation(self, request, context):
        booking_id = valid_uuid(request.booking_id, context)
        version = None
        with self.pool.connection() as conn, conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (str(booking_id),))
            conn.execute("INSERT INTO reservation_cancellations (booking_id) VALUES (%s) ON CONFLICT DO NOTHING", (booking_id,))
            row = conn.execute("SELECT * FROM seat_reservations WHERE booking_id = %s FOR UPDATE", (booking_id,)).fetchone()
            if row is not None and row["status"] == "ACTIVE":
                conn.execute("SELECT id FROM flights WHERE id = %s FOR UPDATE", (row["flight_id"],))
                conn.execute("UPDATE flights SET available_seats = available_seats + %s WHERE id = %s", (row["seat_count"], row["flight_id"]))
                conn.execute("UPDATE seat_reservations SET status = 'RELEASED' WHERE id = %s", (row["id"],))
                version = conn.execute("UPDATE cache_state SET version = version + 1 WHERE id = 1 RETURNING version - 1 AS previous").fetchone()["previous"]
        if version is not None:
            self.cache.invalidate(version)
        return Empty()

    def UpdateFlight(self, request, context):
        flight_id = valid_uuid(request.id, context)
        price = None
        status = None
        if request.HasField("price"):
            try:
                price = Decimal(request.price)
                if not price.is_finite() or not 0 < price <= Decimal("9999999999.99") or price != price.quantize(Decimal("0.01")):
                    raise ValueError
            except (InvalidOperation, ValueError):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "price must be positive with at most two decimal places")
        if request.HasField("status"):
            if request.status not in (pb.SCHEDULED, pb.DEPARTED, pb.CANCELLED, pb.COMPLETED):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid flight status")
            status = pb.FlightStatus.Name(request.status)
        if price is None and status is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Provide price or status")
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute("SELECT id FROM flights WHERE id = %s FOR UPDATE", (flight_id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            conn.execute("UPDATE flights SET price = COALESCE(%s, price), status = COALESCE(%s, status) WHERE id = %s", (price, status, flight_id))
            version = conn.execute("UPDATE cache_state SET version = version + 1 WHERE id = 1 RETURNING version - 1 AS previous").fetchone()["previous"]
            result = flight_message(conn.execute(FLIGHT_SELECT + " WHERE f.id = %s", (flight_id,)).fetchone())
        self.cache.invalidate(version)
        return result


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    pool = create_pool()
    migrate(pool, Path(__file__).parent / "migrations")
    server = grpc.server(ThreadPoolExecutor(max_workers=16), interceptors=[AuthInterceptor(os.environ["GRPC_API_KEY"])])
    rpc.add_FlightServiceServicer_to_server(FlightService(pool, Cache()), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    log.info("Flight Service listening on 50051")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5).wait()
    finally:
        pool.close()


if __name__ == "__main__":
    main()
