FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto proto
RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/flights.proto
COPY common common
COPY flight flight
COPY booking booking
RUN useradd -m app
USER app
CMD ["uvicorn", "booking.app:app", "--host", "0.0.0.0", "--port", "8000"]
