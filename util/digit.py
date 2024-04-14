from decimal import Decimal


def remove_exponent(d: Decimal) -> Decimal:
    # From https://docs.python.org/3/library/decimal.html#decimal-faq
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()