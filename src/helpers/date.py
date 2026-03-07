from datetime import datetime, timedelta
import holidays


def get_next_trading_day(current_date):
    """Skips weekends and Greek national holidays."""
    gr_holidays = holidays.GR()
    next_date = current_date + timedelta(days=1)

    while next_date.weekday() >= 5 or next_date in gr_holidays:
        next_date += timedelta(days=1)

    return next_date