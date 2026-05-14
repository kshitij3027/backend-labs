"""Unit tests for the in-memory user store and the 4 seeded demo users."""
import pytest

from src.auth.users import User, UserStore, default_store, hash_password


def test_default_store_has_four_users() -> None:
    assert len(default_store) == 4
    assert default_store.all_usernames() == ("alice", "bob", "carol", "dave")


def test_default_store_users_have_expected_roles() -> None:
    assert default_store.get("alice").roles == ("administrator",)
    assert default_store.get("bob").roles == ("developer",)
    assert default_store.get("carol").roles == ("analyst",)
    assert default_store.get("dave").roles == ("support",)


def test_get_returns_none_for_unknown_user() -> None:
    assert default_store.get("eve") is None


@pytest.mark.parametrize("username,password", [
    ("alice", "admin123"),
    ("bob", "dev123"),
    ("carol", "analyst123"),
    ("dave", "support123"),
])
def test_authenticate_accepts_seeded_credentials(username: str, password: str) -> None:
    user = default_store.authenticate(username, password)
    assert user is not None
    assert user.username == username


def test_authenticate_rejects_wrong_password() -> None:
    assert default_store.authenticate("alice", "wrong") is None


def test_authenticate_rejects_unknown_username() -> None:
    assert default_store.authenticate("eve", "admin123") is None


def test_user_store_stores_arbitrary_users() -> None:
    """Constructing a fresh UserStore works for tests / future code."""
    custom = UserStore([
        User(user_id="u1", username="x", password_hash=hash_password("p1"), roles=("administrator",))
    ])
    assert len(custom) == 1
    assert custom.authenticate("x", "p1") is not None
    assert custom.authenticate("x", "p2") is None


def test_user_dataclass_is_frozen() -> None:
    """User instances must be immutable to prevent accidental mutation of the seeded store."""
    u = default_store.get("alice")
    with pytest.raises(Exception):  # FrozenInstanceError subclasses Exception
        u.username = "mallory"  # type: ignore[misc]
