from __future__ import annotations

import logging
import sys

from passlib.context import CryptContext

logger = logging.getLogger(__name__)

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        logger.error("usage: python scripts/seed_password.py <plaintext-password>")
        return 2
    plain = argv[1]
    sys.stdout.write(_pwd_context.hash(plain))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main(sys.argv))
