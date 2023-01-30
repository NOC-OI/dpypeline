from .actions import subscribe_action, Action


def handle_extract_from_server(*args, **kwargs):
    print("Extracting from server.")


def setup_extract_from_server(*args, **kwargs):
    subscribe_action(Action.EXTRACT, handle_extract_from_server, *args, **kwargs)
