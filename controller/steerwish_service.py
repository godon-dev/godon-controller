import json
import os
import urllib.request
import uuid

from f.controller.database import ArchiveDatabaseRepository, MetadataDatabaseRepository
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)

VALID_EVENT_TYPES = [
    'declared', 'planned', 'refused', 'assigned', 'plan_error', 'acted',
    'landed', 'missed', 're_opened', 'closed',
    # mirrored from causal's book by the lazy fold-in
    'undecidable', 'replanned', 'released',
    'walk_opened', 'walk_probe', 'walk_closed',
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

    def __init__(self, meta_db_config, archive_db_config=None):
        self.repo = MetadataDatabaseRepository(meta_db_config)
        # Archive access is optional at construction: get/list never need
        # it; create (plan ask -> assignment row) and close (release)
        # degrade to logged warnings when it is absent.
        self.archive_repo = (
            ArchiveDatabaseRepository(archive_db_config) if archive_db_config else None)

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

        # ── the ask: causal learns the wish and plans it ────────────────
        # One HTTP ask seeds causal's book (the terms ride the request).
        # A planned answer names the dial - the controller then plants the
        # assignment row in the serving tender's archive DB, and the
        # tender's pulse does the rest. The budget rides along: causal
        # enforces the outer round total, the controller only declares it.
        self._ask_causal_to_plan(wish_id, outcome.strip(), band, limits, budget)

        wish = self.get_steerwish(wish_id)
        if wish is None:
            raise RuntimeError(f'inserted steerwish vanished: {wish_id}')
        return wish

    def get_steerwish(self, wish_id):
        """Fetch one wish with full event history; None if unknown.

        Reading is asking: the lazy fold-in pulls causal's page for this
        wish once and mirrors every book event the registry has not yet
        seen. The book is the truth; the registry is a late-but-complete
        mirror — whoever looks triggers the catch-up.
        """
        self._ensure_registry()
        row = self.repo.fetch_steerwish_by_id(wish_id)
        if row is None:
            return None
        self._fold_in_book(wish_id)
        row = self.repo.fetch_steerwish_by_id(wish_id)
        events = self.repo.fetch_steerwish_events(wish_id)
        return self._format_wish(row, events)

    def list_steerwishes(self):
        """Summaries (no event history), newest first. The fold-in runs
        for every open wish — a handful of wishes, a handful of cheap
        GETs, no timers."""
        self._ensure_registry()
        for row in self.repo.fetch_steerwishes_list():
            if row[2] != 'closed':
                self._fold_in_book(str(row[0]))
        rows = self.repo.fetch_steerwishes_list()
        return [self._format_summary(row) for row in rows]

    def close_steerwish(self, wish_id):
        """Append the 'closed' event; idempotent on already-closed wishes.

        Closing is the owner's act - it is what releases the held outcome:
        the assignment row is removed, and the serving tender's pulse
        reverts the dial to neutral on its next boundary.
        """
        wish = self.get_steerwish(wish_id)
        if wish is None:
            return None
        if wish['state'] == 'closed':
            return wish
        self.repo.insert_steerwish_event(wish_id, 'closed', None)
        self._unassign_wish(wish)
        return self.get_steerwish(wish_id)

    # ── the plan ask + assignment (the controller asks, never computes) ──

    def _causal_url(self):
        return os.environ.get(
            'GODON_CAUSAL_URL', 'http://godon-godon-causal:9091')

    # ── the lazy fold-in: reading is asking ───────────────────────────

    def _fetch_causal_page(self, wish_id):
        """GET the wish's page from causal's book (status, instruction,
        event tail). Best-effort: None on any failure — the registry
        then simply stays as stale as it was."""
        import urllib.request
        url = f"{self._causal_url()}/steer/plan/{wish_id}"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            logger.debug(f"Wish {wish_id}: book page fetch skipped: {e}")
            return None

    def _fold_in_book(self, wish_id):
        """Mirror every book event the registry has not seen, stamped at
        the book's own time. The book is the truth; this copy is late
        but complete. Idempotent per event (type + timestamp dedupe)."""
        page = self._fetch_causal_page(wish_id)
        if not page:
            return
        for e in page.get('events') or []:
            tsz = e.get('tsz')
            event = e.get('event')
            if tsz is None or not event:
                continue
            try:
                if self.repo.has_steerwish_event_at(wish_id, event, tsz):
                    continue
                detail = json.dumps({'at': tsz, 'book': e.get('detail')})
                self.repo.insert_steerwish_event_at(wish_id, event, detail, tsz)
            except Exception as ex:
                logger.debug(f"Wish {wish_id}: fold-in of {event} skipped: {ex}")

    def _ask_causal_to_plan(self, wish_id, outcome, band, limits, budget=None):
        plan_request = {
            'wish_id': wish_id,
            'outcome': outcome,
            'band': {
                'lo': float(band['lo']),
                'hi': float(band['hi']),
                **({'target': float(band['target'])}
                   if band.get('target') is not None else {}),
            },
        }
        if budget is not None:
            plan_request['budget'] = int(budget)
        if limits:
            plan_request['limits'] = limits
        try:
            req = urllib.request.Request(
                f"{self._causal_url()}/steer/plan",
                data=json.dumps(plan_request).encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method='POST',
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                plan_response = json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            logger.warning(f"Wish {wish_id}: plan ask failed: {e}")
            self.repo.insert_steerwish_event(
                wish_id, 'plan_error', json.dumps({'error': str(e)}))
            return

        if plan_response.get('status') == 'planned':
            self.repo.insert_steerwish_event(
                wish_id, 'planned', json.dumps(plan_response))
            moves = plan_response.get('moves') or []
            sender = (moves[0] or {}).get('sender') if moves else None
            if sender and self.archive_repo is not None:
                db_name = f"systemtender_{str(sender).replace('-', '_')}"
                try:
                    self.archive_repo.write_wish_assignment(db_name, wish_id)
                    self.repo.insert_steerwish_event(
                        wish_id, 'assigned',
                        json.dumps({'sender': sender, 'db': db_name}))
                    logger.info(f"Wish {wish_id} assigned to {db_name}")
                except Exception as e:
                    logger.warning(
                        f"Wish {wish_id}: assignment write failed: {e}")
                    self.repo.insert_steerwish_event(
                        wish_id, 'plan_error',
                        json.dumps({'error': f'assignment write failed: {e}'}))
            elif sender is None:
                logger.warning(f"Wish {wish_id}: planned but no move named")
            else:
                logger.warning(
                    f"Wish {wish_id}: planned on {sender} but no archive "
                    f"config - assignment not written")
        else:
            self.repo.insert_steerwish_event(wish_id, 'refused', json.dumps({
                'reason': plan_response.get('reason'),
                'detail': plan_response.get('detail'),
            }))
            logger.info(
                f"Wish {wish_id} refused: {plan_response.get('reason')}")

    def _unassign_wish(self, wish):
        """Close is the owner's release: remove the assignment row so the
        serving tender's pulse reverts the dial to neutral."""
        if self.archive_repo is None:
            return
        for event in wish.get('events', []):
            if event.get('type') != 'assigned':
                continue
            detail = event.get('detail')
            try:
                d = json.loads(detail) if isinstance(detail, str) else detail
                db_name = (d or {}).get('db')
            except Exception:
                db_name = None
            if db_name:
                try:
                    self.archive_repo.delete_wish_assignment(db_name, wish['id'])
                except Exception as e:
                    logger.warning(
                        f"Wish {wish['id']}: assignment removal failed: {e}")

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
