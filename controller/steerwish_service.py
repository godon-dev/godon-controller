import uuid

from f.controller.database import MetadataDatabaseRepository
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)

VALID_EVENT_TYPES = [
    'declared', 'planned', 'refused', 'acted',
    'landed', 'missed', 're_opened', 'closed',
]


class SteerwishValidationError(Exception):
    """Raised when a declare payload fails entry validation.

    Malformed wishes are rejected before anything plans on them.
    """


class SteerwishService:
    """Registry + lifecycle for steerwishes.

    Division of labor (sealed 2026-09-10): the controller owns the wish
    OBJECT and its lifecycle - identity, coordinates, events, timestamps,
    adoption lookup, purge cascade. causal owns the steering CALCULATION
    keyed by wish id (plan, bars, refusal, verdict stamps, vigilance).
    This service asks, never computes: state is the latest lifecycle
    event, derived on read, never stored.
    """

    def __init__(self, meta_db_config):
        self.repo = MetadataDatabaseRepository(meta_db_config)

    def _ensure_registry(self):
        """Idempotent: registry tables exist before the first DB touch.

        Mirrors the targets/credentials pattern - schema is ensured at
        the point of use (CREATE TABLE IF NOT EXISTS), after entry
        validation, so malformed wishes never reach the database.
        """
        self.repo.create_steerwish_tables()

    def create_steerwish(self, payload):
        """Validate at the door, insert, stamp 'declared', return the wish."""
        payload = payload or {}
        outcome = payload.get('outcome')
        band = payload.get('band')

        if not isinstance(outcome, str) or not outcome.strip():
            raise SteerwishValidationError(
                'outcome is required: the plain name of one measured value')
        if not isinstance(band, dict) or 'lo' not in band or 'hi' not in band:
            raise SteerwishValidationError(
                'band requires lo and hi (in the outcome measurement units)')
        try:
            lo, hi = float(band['lo']), float(band['hi'])
        except (TypeError, ValueError):
            raise SteerwishValidationError('band lo and hi must be numbers')
        if not lo < hi:
            raise SteerwishValidationError('band lo must be below hi')

        budget = payload.get('budget')
        if budget is not None:
            if isinstance(budget, bool) or not isinstance(budget, int) or budget < 0:
                raise SteerwishValidationError(
                    'budget must be a non-negative integer (omitted = upkeep indefinitely)')

        regime = payload.get('regime') or 'standing'
        if regime != 'standing':
            raise SteerwishValidationError(
                f"unknown regime: {regime} (only 'standing' exists today)")

        limits = payload.get('limits')
        if limits is not None and not isinstance(limits, dict):
            raise SteerwishValidationError('limits must be an object (exclude, maxChange)')

        wish_id = str(uuid.uuid4())
        self._ensure_registry()
        self.repo.insert_steerwish(
            wish_id=wish_id,
            outcome=outcome.strip(),
            band=band,
            limits=limits,
            budget=budget,
            regime=regime,
        )
        self.repo.insert_steerwish_event(wish_id, 'declared', None)
        logger.info(f"Steerwish declared: {wish_id} (outcome: {outcome.strip()})")

        wish = self.get_steerwish(wish_id)
        if wish is None:
            raise RuntimeError(f'inserted steerwish vanished: {wish_id}')
        return wish

    def get_steerwish(self, wish_id):
        """Fetch one wish with full event history; None if unknown."""
        self._ensure_registry()
        row = self.repo.fetch_steerwish_by_id(wish_id)
        if row is None:
            return None
        events = self.repo.fetch_steerwish_events(wish_id)
        return self._format_wish(row, events)

    def list_steerwishes(self):
        """Summaries (no event history), newest first."""
        self._ensure_registry()
        rows = self.repo.fetch_steerwishes_list()
        return [self._format_summary(row) for row in rows]

    def close_steerwish(self, wish_id):
        """Append the 'closed' event; idempotent on already-closed wishes.

        Closing is the owner's act - it is what releases the held outcome
        (release-to-neutral is the tender's exit behavior, not stored here).
        """
        wish = self.get_steerwish(wish_id)
        if wish is None:
            return None
        if wish['state'] == 'closed':
            return wish
        self.repo.insert_steerwish_event(wish_id, 'closed', None)
        return self.get_steerwish(wish_id)

    # ── formatting ──────────────────────────────────────────────────

    def _format_summary(self, row):
        return {
            'id': str(row[0]),
            'outcome': row[1],
            'state': row[2],
            'createdAt': row[3].isoformat() if row[3] else None,
        }

    def _format_wish(self, row, events):
        # row: id, outcome, band, limits, budget, regime, created_at, state
        return {
            'id': str(row[0]),
            'outcome': row[1],
            'band': row[2],
            'limits': row[3],
            'budget': row[4],
            'regime': row[5],
            'createdAt': row[6].isoformat() if row[6] else None,
            'state': row[7],
            'events': [
                {
                    'type': event[0],
                    'at': event[1].isoformat() if event[1] else None,
                    'detail': event[2],
                }
                for event in events
            ],
        }
