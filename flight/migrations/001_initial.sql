CREATE TABLE airlines (
    id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code varchar(3) NOT NULL UNIQUE CHECK (code ~ '^[A-Z0-9]{2,3}$'),
    name text NOT NULL CHECK (length(trim(name)) > 0)
);

CREATE TABLE flights (
    id uuid PRIMARY KEY,
    airline_id smallint NOT NULL REFERENCES airlines(id),
    flight_number varchar(10) NOT NULL CHECK (flight_number ~ '^[A-Z0-9]{2,3}[0-9]{1,4}$'),
    origin char(3) NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination char(3) NOT NULL CHECK (destination ~ '^[A-Z]{3}$' AND destination <> origin),
    departure_at timestamptz NOT NULL,
    arrival_at timestamptz NOT NULL CHECK (arrival_at > departure_at),
    total_seats integer NOT NULL CHECK (total_seats > 0),
    available_seats integer NOT NULL CHECK (available_seats >= 0 AND available_seats <= total_seats),
    price numeric(12, 2) NOT NULL CHECK (price > 0 AND price <> 'NaN'::numeric),
    status text NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED'))
);

CREATE UNIQUE INDEX flights_number_date ON flights (flight_number, ((departure_at AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_search ON flights (origin, destination, departure_at) WHERE status = 'SCHEDULED';

CREATE TABLE seat_reservations (
    id uuid PRIMARY KEY,
    flight_id uuid NOT NULL REFERENCES flights(id),
    booking_id uuid NOT NULL UNIQUE,
    seat_count integer NOT NULL CHECK (seat_count > 0),
    status text NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reservation_cancellations (
    booking_id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cache_state (
    id integer PRIMARY KEY CHECK (id = 1),
    version bigint NOT NULL CHECK (version > 0)
);

INSERT INTO cache_state VALUES (1, 1);
INSERT INTO airlines (code, name) VALUES ('SU', 'Aeroflot'), ('DP', 'Pobeda');

INSERT INTO flights (id, airline_id, flight_number, origin, destination, departure_at, arrival_at, total_seats, available_seats, price, status) VALUES
('11111111-1111-4111-8111-111111111111', 1, 'SU100', 'SVO', 'LED', '2026-12-01 08:00:00+00', '2026-12-01 09:30:00+00', 20, 20, 7500.00, 'SCHEDULED'),
('22222222-2222-4222-8222-222222222222', 1, 'SU102', 'SVO', 'LED', '2026-12-02 12:00:00+00', '2026-12-02 13:30:00+00', 20, 20, 8200.00, 'SCHEDULED'),
('33333333-3333-4333-8333-333333333333', 2, 'DP200', 'VKO', 'AER', '2026-12-01 10:00:00+00', '2026-12-01 13:30:00+00', 20, 20, 11000.00, 'SCHEDULED');
