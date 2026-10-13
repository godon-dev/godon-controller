"""The seek species' grammar — the commissioned walk's body at the door.

Mirrors the tender's own door (robots) and the planner's (causal):
one vocabulary across all three checks; the wire is never trusted.
The steer species' grammar lives beside it in steer_grammar.
"""

try:
    from f.controller.wish_errors import WishValidationError
except ImportError:  # test envs
    from controller.wish_errors import WishValidationError


SEEK_SPECIES = ('cover_now', 'find_best')
DIRECTION_WORDS = ('maximize', 'max', 'minimize', 'min')


def checked_seek_body(body):
    """The seek wish's body at the door (design §XIV; wire pinned in
    the chime 2026-10-12): species, region, criterion, budget,
    candidates. This door mirrors the tender's own door — a wish that
    cannot run refuses here, before anything plans on it; the tender's
    door re-checks (never trust the wire)."""
    if not isinstance(body, dict):
        raise WishValidationError(
            'seek must be an object: {species, region, criterion, budget, candidates}')

    species = body.get('species') or 'find_best'
    if species not in SEEK_SPECIES:
        raise WishValidationError(
            f"unknown seek species: {species} (cover_now or find_best)")

    region = body.get('region')
    if not isinstance(region, dict) or not region:
        raise WishValidationError(
            'seek.region is required: at least one dial, each '
            '[lo, hi, is_int]')
    checked_region = {}
    for dial, spec in region.items():
        if not isinstance(spec, (list, tuple)) or len(spec) < 2:
            raise WishValidationError(
                f'seek.region.{dial} must be [lo, hi, is_int]')
        lo, hi = spec[0], spec[1]
        is_int = bool(spec[2]) if len(spec) > 2 else False
        if isinstance(lo, bool) or isinstance(hi, bool) \
                or not isinstance(lo, (int, float)) \
                or not isinstance(hi, (int, float)) \
                or float(hi) <= float(lo):
            raise WishValidationError(
                f'seek.region.{dial}: hi must exceed lo')
        if is_int and (int(lo) != lo or int(hi) != hi):
            raise WishValidationError(
                f'seek.region.{dial}: integer dial needs integer bounds')
        checked_region[dial] = [float(lo) if not is_int else int(lo),
                                float(hi) if not is_int else int(hi),
                                is_int]

    criterion = None
    if species == 'find_best':
        criterion = body.get('criterion')
        if not isinstance(criterion, dict) \
                or not criterion.get('outcome'):
            raise WishValidationError(
                'find_best declares a criterion: {outcome, direction}')
        direction = criterion.get('direction')
        if direction not in DIRECTION_WORDS:
            raise WishValidationError(
                f"criterion.direction must be one of {DIRECTION_WORDS}")
        criterion = {'outcome': str(criterion['outcome']).strip(),
                     'direction': direction}

    budget = body.get('budget')
    if species == 'find_best':
        if isinstance(budget, bool) or not isinstance(budget, int) or budget <= 0:
            raise WishValidationError(
                'find_best declares a budget: a positive integer '
                '(the feelers it may spend)')
    else:
        if budget is not None:
            raise WishValidationError(
                'cover_now walks on standing duty - it carries no budget')

    candidates = body.get('candidates') or 0
    if isinstance(candidates, bool) or not isinstance(candidates, int) \
            or candidates < 0:
        raise WishValidationError(
            'seek.candidates must be a non-negative integer (omitted = none)')
    if species == 'find_best' and isinstance(budget, int) \
            and candidates > budget:
        raise WishValidationError(
            f'seek.candidates ({candidates}) exceeds the budget '
            f'({budget}) - an impossible wish never starts')

    return {
        'species': species,
        'region': checked_region,
        'criterion': criterion,
        'budget': budget,
        'candidates': candidates,
    }
