import os
from pathlib import Path


def load_dotenv() -> None:
    """Load ``test/.env`` into ``os.environ`` (without overriding existing
    vars). Shared by the compliance + integration conftests."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())
