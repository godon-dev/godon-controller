"""The hold wish's grammar — claims and terms at the door.

The wish registry is grammar-agnostic (wish_service); this module
carries the STEER vocabulary: bands, claims, terms — the atoms the
gavel judges. The walk wish's grammar lives beside it in
seek_grammar.
"""

import math

try:
    from f.controller.wish_errors import WishValidationError
except ImportError:  # test envs
    from controller.wish_errors import WishValidationError


def checked_band(band, where='band'):
    """One band, door-checked - the same rules for sugar and claims.

    The wish-shape stays free; the door keeps one law for every band
    it accepts (sealed with wish-v1: lo below hi, finite, target
    finite when present).
    """
    if not isinstance(band, dict) or 'lo' not in band or 'hi' not in band:
        raise WishValidationError(
            f'{where} requires lo and hi (in the outcome measurement units)')
    try:
        lo, hi = float(band['lo']), float(band['hi'])
    except (TypeError, ValueError):
        raise WishValidationError(f'{where} lo and hi must be numbers')
    if not lo < hi:
        raise WishValidationError(f'{where} lo must be below hi')
    if not (math.isfinite(lo) and math.isfinite(hi)):
        raise WishValidationError(f'{where} lo and hi must be finite')
    target = band.get('target')
    if target is not None:
        try:
            t = float(target)
        except (TypeError, ValueError):
            raise WishValidationError(f'{where} target must be a number')
        if not math.isfinite(t):
            raise WishValidationError(f'{where} target must be finite')
    return band


def checked_claim(claim, where='claim'):
    """One claim or term at the door: the atom the gavel judges.

    The shape is identical for aims and price - the role (dial or
    never-actuate) is a compile-time fact, not a grammar one.
    """
    if not isinstance(claim, dict):
        raise WishValidationError(
            f'{where} must be an object (outcome, band)')
    if claim.get('direction') is not None:
        raise WishValidationError(
            f'{where}.direction is refused: the direction rung is not '
            'built - declare a band')
    name = claim.get('outcome')
    if not isinstance(name, str) or not name.strip():
        raise WishValidationError(
            f'{where}.outcome is required: the plain name of one '
            'measured value')
    return {
        'outcome': name.strip(),
        'band': checked_band(claim.get('band'), f'{where}.band'),
    }


ANSWER_WORDS = ('now', 'later')
DIVISIBILITY_WORDS = ('joint', 'alternatives')


def checked_declaration_options(body, default_divisibility):
    """The two caller-stated declaration options (design §XIX) - family
    grammar, both wish types: answer timing and divisibility, intent
    only the wisher holds. Both optional with sealed defaults: answer
    'later' (the consult's answers surface at serving time and in
    status), divisibility 'joint' for holds (a half-kept state is a
    lying state) and 'alternatives' for walks (a seek's candidates by
    nature).
    """
    answer = body.get('answer')
    if answer is None:
        answer = 'later'
    if answer not in ANSWER_WORDS:
        raise WishValidationError(
            f"answer must be 'now' or 'later' (got: {answer}) - when the "
            'reachability consult answers: in the same breath, or at '
            'serving time')
    divisibility = body.get('divisibility')
    if divisibility is None:
        divisibility = default_divisibility
    if divisibility not in DIVISIBILITY_WORDS:
        raise WishValidationError(
            f'divisibility must be joint or alternatives '
            f'(got: {divisibility}) - one kept state, or the best of '
            'several')
    return {'answer': answer, 'divisibility': divisibility}
