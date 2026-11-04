import logging
import threading
from contextlib import asynccontextmanager, contextmanager
from datetime import date as Date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Annotated
from uuid import UUID, uuid4

import grpc
import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field, StringConstraints
from psycopg_pool import PoolTimeout

import flights_pb2 as pb
from booking.rpc import FlightClient
from common.db import create_pool, migrate


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)


class BookingRequest(BaseModel):
    user_id: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100)]
    flight_id: UUID
    passenger_name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
    passenger_email: EmailStr = Field(max_length=254)
    seat_count: int = Field(gt=0, le=2147483647, strict=True)


def flight_dict(flight):
    return {
        "id": flight.id,
        "flight_number": flight.flight_number,
        "airline": flight.airline,
        "origin": flight.origin,
        "destination": flight.destination,
        "departure_at": flight.departure_at.ToDatetime(tzinfo=timezone.utc).isoformat(),
        "arrival_at": flight.arrival_at.ToDatetime(tzinfo=timezone.utc).isoformat(),
        "total_seats": flight.total_seats,
        "available_seats": flight.available_seats,
        "price": flight.price,
        "status": pb.FlightStatus.Name(flight.status),
    }


def booking_dict(row):
    return {key: str(value) if isinstance(value, (UUID, Decimal)) else value
            for key, value in row.items()}


@contextmanager
def booking_lock(conn, booking_id, wait=True):
    function = "pg_advisory_lock" if wait else "pg_try_advisory_lock"
    result = conn.execute(f"SELECT {function}(hashtextextended(%s, 0)) AS acquired",
                          (str(booking_id),)).fetchone()
    conn.commit()
    acquired = wait or result["acquired"]
    try:
        yield acquired
    finally:
        if acquired and not conn.closed:
            conn.rollback()
            conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (str(booking_id),))
            conn.commit()


def recover_one(conn, rpc, booking_id):
    with conn.transaction():
        job = conn.execute("SELECT * FROM compensation_jobs WHERE booking_id = %s FOR UPDATE",
                           (booking_id,)).fetchone()
        if job is None:
            return
        rpc.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))
        if job["kind"] == "CANCEL":
            conn.execute("UPDATE bookings SET status = 'CANCELLED' WHERE id = %s", (booking_id,))
        conn.execute("DELETE FROM compensation_jobs WHERE booking_id = %s", (booking_id,))
        log.info("booking recovery completed booking_id=%s kind=%s", booking_id, job["kind"])


def recover(pool, rpc, stop):
    while not stop.wait(1):
        try:
            with pool.connection() as conn:
                jobs = conn.execute("SELECT booking_id FROM compensation_jobs ORDER BY created_at LIMIT 50").fetchall()
                conn.commit()
                for job in jobs:
                    if stop.is_set():
                        return
                    try:
                        with booking_lock(conn, job["booking_id"], wait=False) as acquired:
                            if acquired:
                                recover_one(conn, rpc, job["booking_id"])
                    except Exception:
                        log.exception("booking recovery failed booking_id=%s", job["booking_id"])
                        conn.rollback()
        except Exception:
            log.exception("booking recovery unavailable")


@asynccontextmanager
async def lifespan(app):
    pool = create_pool()
    migrate(pool, Path(__file__).parent / "migrations")
    client = FlightClient()
    app.state.pool = pool
    app.state.rpc = client.stub
    stop = threading.Event()
    worker = threading.Thread(target=recover, args=(pool, client.stub, stop), daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop.set()
        client.close()
        worker.join(timeout=10)
        pool.close()


app = FastAPI(title="Flight Booking", lifespan=lifespan)


@app.exception_handler(grpc.RpcError)
async def grpc_error(request, error):
    codes = {
        grpc.StatusCode.INVALID_ARGUMENT: 422,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
        grpc.StatusCode.UNAUTHENTICATED: 502,
        grpc.StatusCode.PERMISSION_DENIED: 502,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 503,
    }
    return JSONResponse(status_code=codes.get(error.code(), 502),
                        content={"detail": error.details(), "code": error.code().name})


@app.exception_handler(psycopg.Error)
@app.exception_handler(PoolTimeout)
async def database_error(request, error):
    log.error("database request failed", exc_info=error)
    return JSONResponse(status_code=503, content={"detail": "Booking database is temporarily unavailable"})


@app.get("/health")
def health(request: Request):
    with request.app.state.pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/flights")
def search_flights(request: Request,
                   origin: str = Query(pattern="^[A-Za-z]{3}$"),
                   destination: str = Query(pattern="^[A-Za-z]{3}$"),
                   date: Date | None = None):
    query = pb.SearchFlightsRequest(origin=origin.upper(), destination=destination.upper())
    if date:
        query.date.FromDatetime(datetime.combine(date, time.min, timezone.utc))
    response = request.app.state.rpc.SearchFlights(query)
    return [flight_dict(flight) for flight in response.flights]


@app.get("/flights/{flight_id}")
def get_flight(flight_id: UUID, request: Request):
    flight = request.app.state.rpc.GetFlight(pb.GetFlightRequest(id=str(flight_id)))
    return flight_dict(flight)


@app.post("/bookings", status_code=201)
def create_booking(data: BookingRequest, request: Request):
    rpc = request.app.state.rpc
    flight = rpc.GetFlight(pb.GetFlightRequest(id=str(data.flight_id)))
    booking_id = uuid4()
    total_price = Decimal(flight.price) * data.seat_count
    with request.app.state.pool.connection() as conn, booking_lock(conn, booking_id):
        with conn.transaction():
            conn.execute("INSERT INTO compensation_jobs (booking_id, kind) VALUES (%s, 'CREATE')", (booking_id,))
        try:
            with conn.transaction():
                conn.execute("SELECT booking_id FROM compensation_jobs WHERE booking_id = %s FOR UPDATE", (booking_id,))
                rpc.ReserveSeats(pb.ReserveSeatsRequest(flight_id=str(data.flight_id),
                                                       booking_id=str(booking_id), seat_count=data.seat_count))
                row = conn.execute(
                    "INSERT INTO bookings (id, user_id, flight_id, passenger_name, passenger_email, seat_count, total_price, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMED') RETURNING *",
                    (booking_id, data.user_id, data.flight_id, data.passenger_name,
                     str(data.passenger_email), data.seat_count, total_price)).fetchone()
                conn.execute("DELETE FROM compensation_jobs WHERE booking_id = %s", (booking_id,))
            return booking_dict(row)
        except Exception:
            try:
                recover_one(conn, rpc, booking_id)
            except Exception:
                log.exception("booking compensation pending booking_id=%s", booking_id)
            raise


@app.get("/bookings")
def list_bookings(request: Request, user_id: str = Query(min_length=1, max_length=100)):
    with request.app.state.pool.connection() as conn:
        rows = conn.execute("SELECT * FROM bookings WHERE user_id = %s ORDER BY created_at DESC, id",
                            (user_id,)).fetchall()
    return [booking_dict(row) for row in rows]


@app.get("/bookings/{booking_id}")
def get_booking(booking_id: UUID, request: Request):
    with request.app.state.pool.connection() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id = %s", (booking_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Booking not found")
    return booking_dict(row)


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(booking_id: UUID, request: Request):
    with request.app.state.pool.connection() as conn, booking_lock(conn, booking_id):
        with conn.transaction():
            row = conn.execute("SELECT * FROM bookings WHERE id = %s FOR UPDATE", (booking_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "Booking not found")
            if row["status"] == "CANCELLED":
                return booking_dict(row)
            conn.execute("INSERT INTO compensation_jobs (booking_id, kind) VALUES (%s, 'CANCEL') "
                         "ON CONFLICT (booking_id) DO NOTHING", (booking_id,))
        with conn.transaction():
            conn.execute("SELECT id FROM bookings WHERE id = %s FOR UPDATE", (booking_id,))
            request.app.state.rpc.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))
            row = conn.execute("UPDATE bookings SET status = 'CANCELLED' WHERE id = %s RETURNING *",
                               (booking_id,)).fetchone()
            conn.execute("DELETE FROM compensation_jobs WHERE booking_id = %s", (booking_id,))
        return booking_dict(row)
