from datetime import datetime, timedelta
import holidays

_gr_holidays = holidays.GR()  # module-level, created once

def get_next_trading_day(current_date):
    """Skips weekends and Greek national holidays (Orthodox calendar)."""
    next_date = current_date + timedelta(days=1)
    while next_date.weekday() >= 5 or next_date in _gr_holidays:
        next_date += timedelta(days=1)
    return next_date