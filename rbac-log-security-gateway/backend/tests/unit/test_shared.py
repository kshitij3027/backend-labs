"""Identity tests for shared singletons. Two imports MUST return the same instance."""
import importlib


def test_auth_service_is_singleton() -> None:
    from src.shared import auth_service as a1
    mod = importlib.import_module("src.shared")
    assert a1 is mod.auth_service


def test_rbac_engine_is_singleton() -> None:
    from src.shared import rbac_engine as r1
    mod = importlib.import_module("src.shared")
    assert r1 is mod.rbac_engine


def test_audit_service_is_singleton() -> None:
    from src.shared import audit_service as a1
    mod = importlib.import_module("src.shared")
    assert a1 is mod.audit_service


def test_audit_stub_append_is_noop() -> None:
    from src.shared import audit_service
    # Should not raise, returns None.
    assert audit_service.append({"any": "entry"}) is None


def test_dependencies_return_shared_instances() -> None:
    from src.auth.dependencies import get_auth, get_rbac, get_audit
    from src.shared import auth_service, rbac_engine, audit_service

    assert get_auth() is auth_service
    assert get_rbac() is rbac_engine
    assert get_audit() is audit_service


def test_auth_router_uses_shared_auth_service() -> None:
    """The auth router's _auth_service alias must be the shared singleton."""
    from src.api.auth import _auth_service as router_local
    from src.shared import auth_service as canonical
    assert router_local is canonical
