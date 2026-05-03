from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

MASSIVE_API_KEY: str = (os.environ.get("MASSIVE_API_KEY") or "").strip()
MASSIVE_TIER: str = os.environ.get("MASSIVE_TIER", "free").lower()
SIM_STEP_SECONDS: float = float(os.environ.get("SIM_STEP_SECONDS", "0.5"))
SIM_RANDOM_SEED: int | None = (
    int(os.environ["SIM_RANDOM_SEED"])
    if os.environ.get("SIM_RANDOM_SEED")
    else None
)
LLM_MOCK: bool = os.environ.get("LLM_MOCK", "false").lower() == "true"
OPENROUTER_API_KEY: str = os.environ.get("OPENROUTER_API_KEY", "")
