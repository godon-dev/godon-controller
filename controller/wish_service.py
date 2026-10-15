import json
import math
import os
import urllib.request
import uuid

from f.controller.database import ArchiveDatabaseRepository, MetadataDatabaseRepository
try:
    from f.controller import steer_grammar, seek_grammar
    from f.controller.wish_errors import WishValidationError
except ImportError:  # test envs
    from controller import steer_grammar, seek_grammar
    from controller.wish_errors import WishValidationError

from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)

# The stale window derives from the tender's DECLARED beat interval:
# alive iff age <= BEAT_MULTIPLIER * interval. Three beats of margin
# covers one slow trial without pretending immortality. The fallback
# below serves only a tender's first seconds (before its second beat
# reports the interval); it errs loud — a false "dead" is a visible
# refusal, a false "alive" is the old silent void.
BEAT_MULTIPLIER = 3
HEARTBEAT_FALLBACK_TTL_SECS = float(
    os.environ.get('GODON_HEARTBEAT_FALLBACK_TTL_SECS', '60'))

VALID_EVENT_TYPES = [
    'declared', 'planned', 'refused', 'assigned', 'plan_error', 'acted',
    'landed', 'missed', 're_opened', 'closed', 'corrected',
    # mirrored from causal's book by the lazy fold-in
    'undecidable', 'replanned', 'released',
    'walk_opened', 'walk_probe', 'walk_closed',
]

class WishService:
    """Registry + lifecycle for steerwishes.

    Division of labor: the controller owns the wish
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
        """Validate at the door, insert, stamp 'declared', return the wish.

        The wish speaks the stack (sealed 2026-09-21): claims N>=1 -
        the aims the gavel judges - and terms M>=0, claims that carry
        no dial: the price the wish pays. One verdict: kept = every
        claim in band AND every term honored.
        """
        payload = payload or {}

        wish_type = payload.get('type')
        if wish_type is None:
            raise WishValidationError(
                'the wish speaks its type: steer or seek (no omission)')
        if wish_type == 'seek':
            # The walk wishes: a commissioned walk, not a hold. Their
            # door is their own grammar (form, region, criterion,
            # budget, candidates); the registry row carries the body
            # in its own column — never inside claims, whose meaning
            # stays pinned.
            return self._create_seekwish(payload)
        if wish_type not in (None, 'steer'):
            raise WishValidationError(
                f"unknown wish type: {wish_type} (steer or seek)")
        if payload.get('form') is not None:
            raise WishValidationError(
                "form belongs to seek wishes - a hold speaks claims "
                "and terms")

        if payload.get('outcome') is not None or payload.get('band') is not None:
            raise WishValidationError(
                'the sugar shape is retired - the wish speaks claims: '
                '[{outcome, band}], terms: [{outcome, band}]')
        claims_field = payload.get('claims')
        if not isinstance(claims_field, list) or not claims_field:
            raise WishValidationError(
                'claims is required: a non-empty list of {outcome, band} '
                'claims - the atom the gavel judges')
        terms_field = payload.get('terms') or []

        claims = [steer_grammar.checked_claim(c, f'claims[{i}]')
                  for i, c in enumerate(claims_field)]
        terms = [steer_grammar.checked_claim(t, f'terms[{i}]')
                 for i, t in enumerate(terms_field)]
        names = [c['outcome'] for c in claims + terms]
        if len(set(names)) != len(names):
            duplicated = sorted({n for n in names if names.count(n) > 1})[0]
            raise WishValidationError(
                f'one claim per outcome per wish: {duplicated} appears '
                'twice across claims and terms')

        outcome = claims[0]['outcome']
        band = claims[0]['band']

        budget = payload.get('budget')
        if budget is not None:
            if isinstance(budget, bool) or not isinstance(budget, int) or budget < 0:
                raise WishValidationError(
                    'budget must be a non-negative integer (omitted = upkeep indefinitely)')

        regime = payload.get('regime') or 'standing'
        if regime != 'standing':
            raise WishValidationError(
                f"unknown regime: {regime} (only 'standing' exists today)")

        # The two caller-stated declaration options (§XIX): answer
        # timing (default later) and divisibility (holds default
        # joint - a half-kept state is a lying state).
        options = steer_grammar.checked_declaration_options(payload, 'joint')

        limits = payload.get('limits')
        if limits is not None:
            if not isinstance(limits, dict):
                raise WishValidationError(
                    'limits must be an object (exclude, maxChange)')
            exclude = limits.get('exclude')
            if exclude is not None and (
                    not isinstance(exclude, list)
                    or not all(isinstance(p, str) for p in exclude)):
                raise WishValidationError(
                    'limits.exclude must be a list of param names')
            max_change = limits.get('maxChange')
            if max_change is not None:
                # (0, 1) exclusive - aligned with the planner's own
                # plan-time rule, the downstream truth this door mirrors
                if isinstance(max_change, bool) \
                        or not isinstance(max_change, (int, float)) \
                        or not 0.0 < float(max_change) < 1.0:
                    raise WishValidationError(
                        'limits.maxChange must be a fraction of a param\'s '
                        'range from neutral, in (0, 1)')

        wish_id = str(uuid.uuid4())
        self._ensure_registry()
        self.repo.insert_steerwish(
            wish_id=wish_id,
            outcome=outcome.strip(),
            band=band,
            limits=limits,
            budget=budget,
            regime=regime,
            claims=claims,
            terms=terms,
            answer=options['answer'],
            divisibility=options['divisibility'],
        )
        self.repo.insert_steerwish_event(wish_id, 'declared', None)
        logger.info(f"Steerwish declared: {wish_id} (outcome: {outcome.strip()})")

        # ── the ask: causal learns the wish and plans it ────────────────
        # One HTTP ask seeds causal's book (the terms ride the request).
        # A planned answer names the dial - the controller then plants the
        # assignment row in the serving tender's archive DB, and the
        # tender's pulse does the rest. The budget rides along: causal
        # enforces the outer round total, the controller only declares it.
        # Answer timing (§XIX): 'now' asks in the same breath - the
        # consult's answer (planned, or a refusal with numbers) is in
        # this very response; 'later' (the default) dispatches the ask
        # as its own job - the wish stands declared, the answers land
        # as events and surface at serving time and in status.
        if options['answer'] == 'now':
            self._ask_causal_to_plan(wish_id, claims=claims, terms=terms,
                                     limits=limits, budget=budget,
                                     divisibility=options['divisibility'])
        else:
            self._dispatch_plan_ask(wish_id)

        wish = self.get_steerwish(wish_id)
        if wish is None:
            raise RuntimeError(f'inserted steerwish vanished: {wish_id}')
        return wish

    def _create_seekwish(self, payload):
        """Declare a seek wish: validate its body at the door, insert
        with regime 'seek' (the body rides its own column — claims keep
        their pinned meaning), stamp 'declared', ask causal to plan.

        The ask's refusal is a valid answer: causal's map_reads_it
        carries the reading INSIDE the refusal detail, and the wish's
        events carry it to the declarer — the refusal is the answer.
        """
        walk = seek_grammar.checked_wish_body(payload)
        outcome = walk['criterion']['outcome'] if walk['criterion'] \
            else 'cover_now'
        # Walk wishes default to alternatives (§XIX): a seek's
        # candidates are partial service by nature.
        options = steer_grammar.checked_declaration_options(payload, 'alternatives')

        wish_id = str(uuid.uuid4())
        self._ensure_registry()
        self.repo.insert_steerwish(
            wish_id=wish_id,
            outcome=outcome,
            band=None,
            limits=None,
            budget=walk['budget'],
            regime='seek',
            claims=None,
            terms=None,
            seek=walk,
            answer=options['answer'],
            divisibility=options['divisibility'],
        )
        self.repo.insert_steerwish_event(wish_id, 'declared', None)
        logger.info(
            f"Seek wish declared: {wish_id} (form: {walk['form']}, "
            f"outcome: {outcome})")

        if options['answer'] == 'now':
            self._ask_causal_to_plan_seek(wish_id, walk,
                                          divisibility=options['divisibility'])
        else:
            self._dispatch_plan_ask(wish_id)

        wish = self.get_steerwish(wish_id)
        if wish is None:
            raise RuntimeError(f'inserted seek wish vanished: {wish_id}')
        return wish

    def _ask_causal_to_plan_seek(self, wish_id, walk, divisibility=None):
        """One HTTP ask seeds causal's book with the seek. The wire is
        re-pinned (§XVII, 2026-10-13): the ask carries type 'seek' and
        the admitted walk body under 'seek' — one dialect end to end,
        form-keyed (the word 'species' is retired); a planned answer
        names the serving tender (its dials are the region's); any
        other answer is a refusal — map_reads_it arrives with the
        reading in its detail, and the event carries it to the
        declarer verbatim."""
        plan_request = {
            'wish_id': wish_id,
            'type': 'seek',
            'seek': walk,
        }
        if divisibility:
            plan_request['divisibility'] = divisibility
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
            logger.warning(f"Wish {wish_id}: seek plan ask failed: {e}")
            self.repo.insert_steerwish_event(
                wish_id, 'plan_error', json.dumps({'error': str(e)}))
            return

        if plan_response.get('status') == 'planned':
            self.repo.insert_steerwish_event(
                wish_id, 'planned', json.dumps(plan_response))
            sender = plan_response.get('sender')
            if not sender:
                logger.warning(
                    f"Wish {wish_id}: planned but no serving tender named")
                return
            if self.archive_repo is None:
                logger.warning(
                    f"Wish {wish_id}: planned on {sender} but no archive "
                    f"config - assignment not written")
                return
            alive, reason = self._sender_alive(sender)
            if not alive:
                detail = {'error': f'sender {sender} not alive: {reason} '
                                   '- assignment not planted'}
                self.repo.insert_steerwish_event(
                    wish_id, 'plan_error', json.dumps(detail))
                logger.warning(
                    f"Wish {wish_id}: sender {sender} not alive ({reason})")
                return
            db_name = f"systemtender_{str(sender).replace('-', '_')}"
            try:
                self.archive_repo.write_wish_assignment(
                    db_name, wish_id, role='sender')
                self.repo.insert_steerwish_event(
                    wish_id, 'assigned',
                    json.dumps({'sender': sender, 'db': db_name}))
                logger.info(f"Seek wish {wish_id} assigned to {db_name}")
            except Exception as e:
                logger.warning(f"Wish {wish_id}: assignment write failed: {e}")
                self.repo.insert_steerwish_event(
                    wish_id, 'plan_error',
                    json.dumps({'error': f'assignment write failed: {e}'}))
        else:
            self.repo.insert_steerwish_event(wish_id, 'refused', json.dumps({
                'reason': plan_response.get('reason'),
                'detail': plan_response.get('detail'),
            }))
            logger.info(
                f"Wish {wish_id} refused: {plan_response.get('reason')}")

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
        if self._fold_in_book(wish_id) > 0:
            # the mirror moved: the derived state may have moved with it
            row = self.repo.fetch_steerwish_by_id(wish_id)

        # The seek's return mirror: the worker banks its RETURN in the
        # serving tender's own ledger (seek_return::<wish_id>); whoever
        # reads the wish pulls it into the event trail ONCE — the same
        # lazy-mirror discipline the book fold-in uses. A dead tender
        # reads as no return, never an error.
        if (row is not None and len(row) > 10 and row[10]
                and self.archive_repo is not None):
            events_so_far = self.repo.fetch_steerwish_events(wish_id)
            if not any(e[0] == 'returned' for e in events_so_far):
                for e in events_so_far:
                    if e[0] != 'assigned':
                        continue
                    detail = e[2]
                    try:
                        d = json.loads(detail) if isinstance(detail, str) else detail
                    except Exception:
                        d = None
                    db_name = d.get('db') if isinstance(d, dict) else None
                    if not db_name:
                        continue
                    payload = self.archive_repo.read_seek_return(db_name, wish_id)
                    if payload:
                        self.repo.insert_steerwish_event(wish_id, 'returned', payload)
                        row = self.repo.fetch_steerwish_by_id(wish_id)
                        break

        events = self.repo.fetch_steerwish_events(wish_id)
        wish = self._format_wish(row, events)

        # Deletion state machine overrides the derived state (the same
        # lift the tenders run): while the marker stands, the wish IS
        # deleting (or deletion-failed with its reason). Poll until 404.
        lifecycle = self.repo.get_wish_lifecycle(wish_id)
        if lifecycle:
            wish['state'] = lifecycle['state']
            wish['deletion_reason'] = lifecycle.get('reason')
        return wish

    def list_steerwishes(self):
        """Summaries (no event history), newest first. The fold-in runs
        for every open wish — a handful of wishes, a handful of cheap
        GETs, no timers."""
        self._ensure_registry()
        for row in self.repo.fetch_steerwishes_list():
            if row[2] != 'closed':
                self._fold_in_book(str(row[0]))
        rows = self.repo.fetch_steerwishes_list()
        # Deletion markers ride on top of the derived state.
        markers = self.repo.get_all_wish_lifecycle()
        summaries = [self._format_summary(row) for row in rows]
        for summary in summaries:
            if summary.get('id') in markers:
                summary['state'] = markers[summary['id']]
        return summaries

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

    def update_steerwish(self, wish_id, payload):
        """The holder corrects the wish: new terms on the same identity.

        A deliberate holder act - the machinery never rewrites terms on
        its own. Stamps 'corrected' with the previous band, removes the
        stale assignment, and re-asks causal to plan under the same
        wish id; the plan ask rides the same door as declare (plan,
        door check, assignment plant).
        """
        payload = payload or {}
        wish = self.get_steerwish(wish_id)
        if wish is None:
            raise WishValidationError(f'unknown wish: {wish_id}')
        if wish['state'] == 'closed':
            raise WishValidationError(
                'wish is closed - corrections apply to living wishes')
        if payload.get('type') != 'steer':
            raise WishValidationError(
                'corrections speak their type: steer - holds only today')
        if payload.get('band') is not None:
            raise WishValidationError(
                'the sugar shape is retired - correct via claims: '
                '[{outcome, band}], terms: [{outcome, band}]')
        claims_field = payload.get('claims')
        if not isinstance(claims_field, list) or not claims_field:
            raise WishValidationError(
                'corrections speak claims: a non-empty list of '
                '{outcome, band} on the same identity')
        claims = [steer_grammar.checked_claim(c, f'claims[{i}]')
                  for i, c in enumerate(claims_field)]
        terms = [steer_grammar.checked_claim(t, f'terms[{i}]')
                 for i, t in enumerate(payload.get('terms') or [])]
        names = [c['outcome'] for c in claims + terms]
        if len(set(names)) != len(names):
            duplicated = sorted({n for n in names if names.count(n) > 1})[0]
            raise WishValidationError(
                f'one claim per outcome per wish: {duplicated} appears '
                'twice across claims and terms')

        limits = payload.get('limits')
        if limits is not None and not isinstance(limits, dict):
            raise WishValidationError(
                'limits must be an object (exclude, maxChange)')

        budget = payload.get('budget')
        if budget is None:
            budget = wish.get('budget')

        old_band = wish.get('band')
        reason = payload.get('reason') or 'holder correction'

        self._ensure_registry()
        self.repo.update_steerwish_terms(
            wish_id=wish_id, band=claims[0]['band'], limits=limits,
            budget=budget, claims=claims, terms=terms)
        self.repo.insert_steerwish_event(
            wish_id, 'corrected',
            json.dumps({'reason': reason, 'previous_band': old_band}))
        logger.info(
            f"Steerwish corrected: {wish_id} "
            f"-> {len(claims)} claim(s), {len(terms)} term(s)")

        # the world moved. The plan ask decides the assignment's fate:
        # a plan with moves re-plants the note (new dial); a zero-move
        # hold keeps the existing note and dial untouched. Tearing the
        # note out before the ask would release a wish the corrected
        # terms already satisfy. A correction is a deliberate holder
        # act - the mover's ask answers in the same breath (§XIX).
        stored_divisibility = wish.get('divisibility')
        self._ask_causal_to_plan(wish_id, claims=claims, terms=terms,
                                 limits=limits, budget=budget,
                                 divisibility=stored_divisibility)
        return self.get_steerwish(wish_id)

    def delete_steerwish(self, wish_id):
        """Mark-and-forget: the async executor behind DELETE's 202.

        The api enqueues this script and answers 202 immediately; THIS
        run is the deletion executor. Idempotent, single-flight — the
        same state machine the tenders run (designs/2026-10-03, lifted
        to wishes 10-06):

        - claim the executor slot; a live claim means another executor
          is mid-flight — re-DELETE rides along, the client polls
        - close first: the owner's act (unassign + dial revert) —
          idempotent on already-closed wishes, so retries are free;
          causal unreachable -> deletion-failed, the client retries
        - remove the registry rows LAST (events cascade): the row is
          the finalizer, the wish stays visible in "deleting" until
          it goes. The book page is purged by causal's own paired
          delete path.

        GET answers the state machine ("deleting" / "deletion-failed"
        with the reason); 404 is the deletion-done receipt.
        """
        try:
            self._ensure_registry()
            if self.repo.fetch_steerwish_by_id(wish_id) is None:
                # gone — purged by an earlier executor, or never was
                self.repo.remove_wish_lifecycle(wish_id)
                logger.info(f"Delete of steerwish {wish_id}: already gone")
                return {
                    "result": "SUCCESS",
                    "data": {"wish_id": wish_id, "delete_type": "already-gone"},
                }

            # Single-flight claim: exactly one executor per wish.
            if not self.repo.claim_wish_deletion(wish_id):
                lifecycle = self.repo.get_wish_lifecycle(wish_id) or {}
                return {
                    "result": "SUCCESS",
                    "data": {
                        "wish_id": wish_id,
                        "status": lifecycle.get("state", "deleting"),
                        "reason": lifecycle.get("reason"),
                        "note": "deletion already in progress",
                    }
                }

            def _fail(reason):
                logger.error(f"Steerwish deletion failed for {wish_id}: {reason}")
                self.repo.set_wish_lifecycle(wish_id, 'deletion-failed', reason)
                return {
                    "result": "FAILURE",
                    "error": reason,
                    "data": {"wish_id": wish_id, "status": "deletion-failed"},
                }

            # Close first — the owner's act, idempotent on already-closed.
            try:
                closed = self.close_steerwish(wish_id)
                if closed is None:
                    return _fail("wish vanished mid-deletion; retry the delete")
            except Exception as e:
                return _fail(
                    f"causal close failed: {e} - nothing forgotten; retry the delete"
                )

            # The registry row is the finalizer: removed last, so the
            # wish stays visible in "deleting" until everything has
            # landed. From the client's side, GET answering 404 is the
            # deletion-done receipt.
            self.repo.delete_steerwish(wish_id)
            self.repo.remove_wish_lifecycle(wish_id)
            logger.info(f"Steerwish purged: {wish_id}")
            return {
                "result": "SUCCESS",
                "data": {"wish_id": wish_id, "delete_type": "executed"},
            }
        except Exception as e:
            logger.error(f"Failed to delete steerwish {wish_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    # ── the plan ask + assignment (the controller asks, never computes) ──

    def _causal_url(self):
        return os.environ.get(
            'GODON_CAUSAL_URL', 'http://godon-godon-causal:9091')

    def _sender_alive(self, sender_uuid):
        """(alive, reason) — is the named sender a breathing house?

        Decided from the tender's OWN state, never the platform: the
        tender touches its state row on every pulse and writes its
        measured beat interval; the controller derives the window from
        that cadence (age <= multiplier * interval), falling back to a
        fixed window only before the tender's second beat. The registry
        row alone proves existence, not breath (cell teardown kills
        workers without walking through delete — flight 9 planted into
        exactly such a void). The heartbeat read rides the same archive
        connection the delivery itself uses: one door, one clock (the
        database's own now()).
        """
        db_name = f"systemtender_{str(sender_uuid).replace('-', '_')}"
        try:
            rows = self.repo.fetch_meta_data(str(sender_uuid))
            if not rows:
                return False, 'unknown tender: not in registry'
        except Exception as e:
            return False, f'registry check failed: {e}'
        try:
            beat = self.archive_repo.read_heartbeat(db_name)
        except Exception as e:
            return False, f'archive db unreachable ({db_name}): {e}'
        if beat is None:
            return False, f'no state row in {db_name} (db missing or empty)'
        age, interval = beat
        window = (BEAT_MULTIPLIER * interval if interval is not None
                  else HEARTBEAT_FALLBACK_TTL_SECS)
        if age <= window:
            return True, (f'heartbeat {age:.0f}s old '
                          f'(window {window:.0f}s)')
        return False, (f'heartbeat stale: last beat {age:.0f}s ago '
                       f'(window {window:.0f}s)')

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
        but complete. Idempotent per event (type + timestamp dedupe).
        Returns how many events were mirrored."""
        page = self._fetch_causal_page(wish_id)
        if not page:
            return 0
        mirrored = 0
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
                mirrored += 1
            except Exception as ex:
                logger.debug(f"Wish {wish_id}: fold-in of {event} skipped: {ex}")
        return mirrored

    # ── the plan ask as its own motion (answer 'later') ──────────────

    def _dispatch_plan_ask(self, wish_id):
        """Answer 'later' (§XIX): the consult rides its own job and the
        door returns at 'declared'. Every answer - planned, refused
        with numbers, plan_error - lands on the wish's event record
        when the job runs; status and serving time surface it. A
        dispatch that cannot fly (no windmill in this context) asks
        inline instead: the wish is never left unasked."""
        try:
            import wmill
            wmill.run_script_by_path_async(
                path='f/controller/steerwish_plan',
                args={'wish_id': wish_id},
            )
            logger.info(
                f"Wish {wish_id}: plan ask dispatched (answer: later)")
        except Exception as e:
            logger.warning(
                f"Wish {wish_id}: plan ask dispatch failed ({e}) - asking inline")
            self.plan_wish(wish_id)

    def plan_wish(self, wish_id):
        """The plan ask from stored terms (the dispatched job's body, and
        any re-ask): identical answers and event stamps as the inline
        path - one ask discipline, two timings. Returns the wish, or
        None when the id is unknown."""
        self._ensure_registry()
        row = self.repo.fetch_steerwish_by_id(wish_id)
        if row is None:
            logger.warning(f"Plan ask for unknown wish: {wish_id}")
            return None
        divisibility = row[12] if len(row) > 12 else None
        if row[5] == 'seek':
            walk = row[10] if len(row) > 10 else None
            if not walk:
                logger.warning(f"Wish {wish_id}: seek row carries no walk body")
                return None
            self._ask_causal_to_plan_seek(wish_id, walk,
                                          divisibility=divisibility)
        else:
            claims = row[8] or ([{'outcome': row[1], 'band': row[2]}]
                                if row[1] and row[2] else [])
            self._ask_causal_to_plan(wish_id, claims=claims,
                                     terms=row[9] or [], limits=row[3],
                                     budget=row[4],
                                     divisibility=divisibility)
        return self.get_steerwish(wish_id)

    def _ask_causal_to_plan(self, wish_id, outcome=None, band=None, limits=None,
                            budget=None, claims=None, terms=None,
                            divisibility=None):
        """One HTTP ask seeds causal's book. A single claim with no
        terms rides the engine's current wire shape (outcome + band);
        a multi-claim or terms-carrying wish rides the stack shape
        (claims + terms) - the engine compiles it jointly. Paired
        engine change; the controller only declares, never computes."""
        if claims is None:
            claims = ([{'outcome': outcome, 'band': band}]
                      if outcome and band else [])
        if len(claims) == 1 and not terms:
            first = claims[0]
            first_band = first['band']
            plan_request = {
                'wish_id': wish_id,
                'outcome': first['outcome'],
                'band': {
                    'lo': float(first_band['lo']),
                    'hi': float(first_band['hi']),
                    **({'target': float(first_band['target'])}
                       if first_band.get('target') is not None else {}),
                },
            }
        else:
            plan_request = {
                'wish_id': wish_id,
                'claims': claims,
                'terms': terms or [],
            }
        if divisibility:
            plan_request['divisibility'] = divisibility
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
            path = plan_response.get('path') or []
            receiver = path[-1] if path else None
            if sender and self.archive_repo is not None:
                # The door check: never plant into a house that is not
                # breathing. Flight 9 planted into a torn-down cell's db
                # and the wish died silently; a dead sender now refuses
                # loudly on the wish instead of vanishing into a void.
                alive, reason = self._sender_alive(sender)
                if not alive:
                    detail = {'error': f'sender {sender} not alive: {reason} '
                                       '- assignment not planted'}
                    self.repo.insert_steerwish_event(wish_id, 'plan_error', json.dumps(detail))
                    logger.warning(f"Wish {wish_id}: sender {sender} not alive "
                                   f"({reason}) - no note planted")
                else:
                    db_name = f"systemtender_{str(sender).replace('-', '_')}"
                    try:
                        self.archive_repo.write_wish_assignment(db_name, wish_id, role='sender')
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
                    # The receiver is told too: its reading is the wish.
                    # Without its own note the receiver never parks - it
                    # keeps walking, swinging the promised reading, and
                    # the judge watches a moving target forever (found
                    # live Sep 27: every wish froze at planned because
                    # only the sender's house ever got a note).
                    if receiver and receiver != sender:
                        receiver_db = f"systemtender_{str(receiver).replace('-', '_')}"
                        try:
                            self.archive_repo.write_wish_assignment(
                                receiver_db, wish_id, role='receiver')
                            self.repo.insert_steerwish_event(
                                wish_id, 'assigned',
                                json.dumps({'receiver': receiver, 'db': receiver_db,
                                            'role': 'receiver'}))
                            logger.info(f"Wish {wish_id} receiver note in {receiver_db}")
                        except Exception as e:
                            logger.warning(
                                f"Wish {wish_id}: receiver note failed: {e}")
                            self.repo.insert_steerwish_event(
                                wish_id, 'plan_error',
                                json.dumps({'error': f'receiver note failed: {e}'}))
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
        # row: id, outcome, band, limits, budget, regime, created_at,
        # state, claims, terms (stored; NULL on rows older than the
        # grammar - legacy rows read back as their single claim)
        stored_claims = row[8] if len(row) > 8 else None
        stored_terms = row[9] if len(row) > 9 else None
        claims = stored_claims or [{'outcome': row[1], 'band': row[2]}]
        terms = stored_terms or []
        return {
            'id': str(row[0]),
            'outcome': row[1],
            'band': row[2],
            'claims': claims,
            'terms': terms,
            'limits': row[3],
            'budget': row[4],
            'regime': row[5],
            'createdAt': row[6].isoformat() if row[6] else None,
            'state': row[7],
            'seek': row[10] if len(row) > 10 else None,
            'answer': row[11] if len(row) > 11 else None,
            'divisibility': row[12] if len(row) > 12 else None,
            'events': [
                {
                    'type': event[0],
                    'at': event[1].isoformat() if event[1] else None,
                    'detail': event[2],
                }
                for event in events
            ],
        }
