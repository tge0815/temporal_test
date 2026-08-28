"""Kleine Formathilfen fuer deutsche Zahlendarstellung."""


def euro(betrag: float) -> str:
    """1234567.5 -> '1.234.567,50'"""
    return f"{betrag:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
