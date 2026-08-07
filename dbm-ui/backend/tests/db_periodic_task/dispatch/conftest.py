import pytest


@pytest.fixture(autouse=True)
def clear_dispatch_route_cache():
    """Keep the routing memo/cache from leaking across tests.

    With 50+ patch targets repointed at ``routing.conn_for_namespace``, a stale
    memo entry would silently cross-wire namespaces between tests.
    """
    from backend.db_periodic_task.dispatch import routing

    routing.reset_route_cache()
    yield
    routing.reset_route_cache()
