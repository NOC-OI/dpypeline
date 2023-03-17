"""Test suite for the EventsQueue class."""
import os
import random

from dpypeline.akita.queue_events import EventsQueue

os.environ["CACHE_DIR"] = "tests/"


def delete_cache_file() -> None:
    """Delete the cache file containg the queue."""
    state_file = os.path.join(os.getenv("CACHE_DIR"), "queue_state.pickle")
    if os.path.isfile(state_file):
        os.remove(state_file)


def add_elements_to_queue(
    queue: EventsQueue, n: int = None
) -> tuple[EventsQueue, list[int]]:
    """
    Add elements to the queue.

    Parameters
    ----------
    queue
        Queue to add elements to.
    n, optional
        Number of elements to add, by default None
    """
    if n is None:
        n = random.randint(1, 1000)

    elements_to_add = [x for x in range(n)]
    for elem in elements_to_add:
        queue.enqueue(elem)

    return queue, elements_to_add


def test_queue_size() -> None:
    """Test the size of the queue."""
    delete_cache_file()

    # Create dummy queue
    queue = EventsQueue()

    # Add events to the queue
    n = random.randint(1, 1000)
    queue, _ = add_elements_to_queue(queue, n)

    assert queue.qsize() == n
    assert queue.get_queue_size() == n

    queue.clear_instance()


def test_enqueue_dequeue() -> None:
    """Test the enqueue method."""
    delete_cache_file()

    # Create dummy queue
    queue = EventsQueue()

    # Add events to the queue
    queue, added_elements = add_elements_to_queue(queue, 3)

    assert list(queue.queue) == added_elements

    # Remove two events from the queue
    queue.dequeue()
    queue.dequeue()

    assert list(queue.queue) == added_elements[2:]

    queue.clear_instance()


def test_peek() -> None:
    """Test the peek method."""
    delete_cache_file()

    # Create dummy queue
    queue = EventsQueue()

    # Peek when queue is Empty
    assert queue.peek() is None

    # Add events to the queue
    queue, added_elements = add_elements_to_queue(queue)

    # Peek the queue
    peeked_element = queue.peek()
    assert peeked_element == added_elements[0]
    assert list(queue.queue) == added_elements

    queue.clear_instance()


def test_singleton() -> None:
    """Test the singleton pattern."""
    delete_cache_file()

    # Create dummy queue
    queue = EventsQueue()

    # Add events to the queue
    _, _ = add_elements_to_queue(queue)

    queue_other = EventsQueue()

    assert queue is queue_other
    assert list(queue.queue) == list(queue_other.queue)

    queue.clear_instance()


def test_save_load_state() -> None:
    """Test the save and load of the state of the queue."""
    delete_cache_file()

    # Create dummy queue
    old_queue = EventsQueue()

    # Add events to the queue
    old_queue, added_elements = add_elements_to_queue(old_queue)

    # Destroy the queue
    old_queue.clear_instance()

    new_queue = EventsQueue()

    assert list(new_queue.queue) == added_elements

    # Add more events to the new queue
    new_queue, new_added_elements = add_elements_to_queue(new_queue)

    # Destroy the queue
    new_queue.clear_instance()

    new_new_queue = EventsQueue()

    assert list(new_new_queue.queue) == added_elements + new_added_elements

    new_new_queue.clear_instance()


def test_clear_instance() -> None:
    """Test the clear_instance method."""
    delete_cache_file()
    queue = EventsQueue()
    queue.clear_instance()
    queue_new = EventsQueue()

    assert queue is not queue_new
