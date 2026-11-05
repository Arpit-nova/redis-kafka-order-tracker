from uuid import UUID


def order_status_key(order_id: UUID) -> str:
    return f"order:{order_id}:status"
