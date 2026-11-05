# Order Tracker MVP

A small, runnable order-status service built with FastAPI, Kafka, and Redis. Kafka is the event log; a separate worker consumes lifecycle events and writes the latest status to Redis for fast reads.

## Run locally with Docker Compose

Requirements: Docker Desktop with the Compose plugin.

```bash
docker compose up --build
```

The API is available at <http://localhost:8000>. Interactive API docs are at <http://localhost:8000/docs>. Stop the services with `Ctrl+C` and remove them with `docker compose down`.

## Architecture

```text
Client -> FastAPI -> Kafka (order-events) -> status worker -> Redis
                         Events                       latest status
Client ----------------------------------------------> FastAPI -> Redis
```

- `POST /orders` validates an order and publishes its initial `pending` event.
- `PATCH /orders/{order_id}/status` validates the lifecycle transition against the latest status in Redis, then publishes the new status event.
- `GET /orders/{order_id}` reads the latest status from Redis.
- The worker is a separate Compose service. It consumes `order-events` and updates Redis.

The API acknowledges a write after Kafka accepts the event, not after Redis is updated. The worker updates Redis asynchronously, so reads are eventually consistent: a newly accepted order can briefly return `404` until its event is processed. A status update during that window also returns `404`; retry after the worker catches up.

Allowed transitions are `pending -> confirmed -> processing -> shipped -> delivered`; an order can be cancelled from `pending`, `confirmed`, or `processing`. `delivered` and `cancelled` are terminal. Invalid transitions return `409`.

## Example requests

Create an order:

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"customer-42","items":[{"sku":"book-1","quantity":2}]}'
```

Use the returned `order_id` to check status:

```bash
curl http://localhost:8000/orders/<order_id>
```

After the worker has handled the initial event, transition the order:

```bash
curl -X PATCH http://localhost:8000/orders/<order_id>/status \
  -H "Content-Type: application/json" \
  -d '{"status":"confirmed"}'
```

## Run tests

The focused tests mock Kafka and Redis for API behavior and test the worker's Redis update without requiring running services.

```bash
python -m pip install -r requirements.txt
python -m pytest
```
