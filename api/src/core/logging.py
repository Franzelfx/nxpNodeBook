#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/core/logging.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Centralized logging configuration.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Configure application-wide logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)
