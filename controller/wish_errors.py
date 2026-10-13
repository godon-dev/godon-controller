"""The wish family's door error — shared by the registry (wish_service)
and both wish grammars (the hold's and the walk's), in its own module so nothing circles."""


class WishValidationError(Exception):
    """A wish refused at the door — the message names what was wrong."""
