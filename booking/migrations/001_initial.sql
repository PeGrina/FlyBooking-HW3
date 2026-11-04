CREATE TABLE bookings (
    id uuid PRIMARY KEY,
    user_id varchar(100) NOT NULL CHECK (length(trim(user_id)) > 0),
    flight_id uuid NOT NULL,
    passenger_name varchar(200) NOT NULL CHECK (length(trim(passenger_name)) > 0),
    passenger_email varchar(254) NOT NULL CHECK (position('@' IN passenger_email) > 1),
    seat_count integer NOT NULL CHECK (seat_count > 0),
    total_price numeric(22,2) NOT NULL CHECK (total_price > 0 AND total_price <> 'NaN'::numeric),
    status varchar(20) NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX bookings_user_id_idx ON bookings (user_id, created_at);

CREATE TABLE compensation_jobs (
    booking_id uuid PRIMARY KEY,
    kind varchar(10) NOT NULL CHECK (kind IN ('CREATE', 'CANCEL')),
    created_at timestamptz NOT NULL DEFAULT now()
);
