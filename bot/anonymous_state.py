_pending_targets: dict[int, int] = {}


def set_pending_target(user_id: int, chat_id: int) -> None:
    _pending_targets[user_id] = chat_id


def pop_pending_target(user_id: int) -> int | None:
    return _pending_targets.pop(user_id, None)


def clear_pending_target(user_id: int) -> None:
    _pending_targets.pop(user_id, None)
