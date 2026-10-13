"""The wish family's door error — shared by the registry (wish_service)
and both species grammars, in its own module so nothing circles."""


class WishValidationError(Exception):
    """A wish refused at the door — the message names what was wrong."""
