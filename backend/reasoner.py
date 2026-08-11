"""
LLM confluence/bias reasoning layer.

Takes a MarketSnapshot's compact JSON payload, asks Claude to reason about
top-down bias, and gets back strict structured JSON — not prose. Schema is
defined up front, same discipline as the confluence-engine spec: define the
shape before you write the prompt.
"""

from __future__ import annotations

import json
import os

from anthropic import Anthropic
from pydantic import BaseModel, Field, ValidationError

from models import MarketSnapshot

MODEL = "claude-sonnet-5"


class TimeframeRead(BaseModel):
    timeframe: str
    read: str = Field(description="one short phrase, e.g. 'bullish structure', 'ranging'")


class BiasVerdict(BaseModel):
    symbol: str
    bias: str = Field(description="bullish | bearish | neutral")
    confidence: int = Field(ge=1, le=5)
    timeframe_reads: list[TimeframeRead]
    reasoning: str
    invalidation_note: str


SYSTEM_PROMPT = """You are a top-down technical analysis assistant for a forex trader.

You will receive structured JSON summarizing recent price action across H4, H1, M15, M5, and M1 timeframes for one symbol. No chart images — only numeric summaries (period high/low, net change, last five closes).

Apply standard top-down analysis: higher timeframes (H4, H1) set the directional bias; lower timeframes (M15, M5, M1) are checked for alignment or conflict with that bias.

Respond with ONLY a single JSON object, no prose before or after, matching exactly this shape:
{
  "symbol": "<string>",
  "bias": "bullish" | "bearish" | "neutral",
  "confidence": <integer 1-5>,
  "timeframe_reads": [
    {"timeframe": "H4", "read": "<short phrase>"},
    {"timeframe": "H1", "read": "<short phrase>"},
    {"timeframe": "M15", "read": "<short phrase>"},
    {"timeframe": "M5", "read": "<short phrase>"},
    {"timeframe": "M1", "read": "<short phrase>"}
  ],
  "reasoning": "<2-3 sentences, referencing specific numbers from the data>",
  "invalidation_note": "<what would invalidate this bias>"
}

If a timeframe's data is missing (status: no_data), say so in its "read" field rather than inventing a value. Never output anything except that JSON object."""


def get_bias(snapshot: MarketSnapshot, api_key: str | None = None) -> BiasVerdict:
    client = Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))

    payload = snapshot.to_llm_payload()

    response = client.messages.create(
        model=MODEL,
        max_tokens=800,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": json.dumps(payload)}],
    )

    raw_text = "".join(block.text for block in response.content if block.type == "text")

    try:
        parsed = json.loads(raw_text)
        return BiasVerdict(**parsed)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise RuntimeError(
            f"LLM returned unparseable output, pipeline should not crash the caller: {exc}\n"
            f"Raw response: {raw_text[:500]}"
        ) from exc
