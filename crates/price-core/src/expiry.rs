use chrono::{NaiveDate, Weekday, Datelike};

/// Returns the next upcoming Tuesday on or after `date`.
pub fn get_next_tuesday(date: NaiveDate) -> NaiveDate {
    let mut current = date;
    while current.weekday() != Weekday::Tue {
        if let Some(next) = current.succ_opt() {
            current = next;
        } else {
            break;
        }
    }
    current
}

/// Formats a NaiveDate to the Fyers weekly expiry code format (e.g. "26730" for 30-Jul-2026).
/// - YY: 2-digit year (e.g. "26")
/// - M: Month (1-9, O, N, D)
/// - DD: 2-digit day of month
pub fn format_fyers_expiry_suffix(date: NaiveDate) -> String {
    let yy = format!("{:02}", date.year() % 100);
    let m = match date.month() {
        1..=9 => date.month().to_string(),
        10 => "O".to_string(),
        11 => "N".to_string(),
        12 => "D".to_string(),
        _ => unreachable!(),
    };
    let dd = format!("{:02}", date.day());
    format!("{}{}{}", yy, m, dd)
}

/// Calculate weekly options expiry for NIFTY.
/// Standard weekly contracts expire on Tuesday.
/// If Tuesday is a trading holiday, the expiry shifts to Monday (t-1),
/// and recursively shifts back if successive days are also holidays.
pub fn calculate_nifty_expiry(date: NaiveDate, holidays: &[NaiveDate]) -> NaiveDate {
    let mut expiry = get_next_tuesday(date);
    while holidays.contains(&expiry) || expiry.weekday() == Weekday::Sat || expiry.weekday() == Weekday::Sun {
        if let Some(prev) = expiry.pred_opt() {
            expiry = prev;
        } else {
            break;
        }
    }
    expiry
}

/// Returns the standard list of NSE trading holidays for 2026.
pub fn get_nse_holidays_2026() -> Vec<NaiveDate> {
    vec![
        NaiveDate::from_ymd_opt(2026, 1, 26).unwrap(),  // Republic Day (Mon)
        NaiveDate::from_ymd_opt(2026, 2, 17).unwrap(),  // Mahashivratri (Tue)
        NaiveDate::from_ymd_opt(2026, 3, 4).unwrap(),   // Holi (Wed)
        NaiveDate::from_ymd_opt(2026, 3, 20).unwrap(),  // Eid-ul-Fitr (Fri)
        NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),   // Good Friday (Fri)
        NaiveDate::from_ymd_opt(2026, 4, 14).unwrap(),  // Ambedkar Jayanti (Tue)
        NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),   // Maharashtra Day (Fri)
        NaiveDate::from_ymd_opt(2026, 5, 27).unwrap(),  // Eid-al-Adha (Wed)
        NaiveDate::from_ymd_opt(2026, 9, 15).unwrap(),  // Ganesh Chaturthi (Tue)
        NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(),  // Gandhi Jayanti (Fri)
        NaiveDate::from_ymd_opt(2026, 11, 24).unwrap(), // Guru Nanak Jayanti (Tue)
        NaiveDate::from_ymd_opt(2026, 12, 25).unwrap(), // Christmas (Fri)
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_tuesday_expiry() {
        // July 22, 2026 is a Wednesday.
        // Next Tuesday is July 28, 2026.
        let date = NaiveDate::from_ymd_opt(2026, 7, 22).unwrap();
        let expiry = calculate_nifty_expiry(date, &[]);
        assert_eq!(expiry, NaiveDate::from_ymd_opt(2026, 7, 28).unwrap());
    }

    #[test]
    fn test_on_tuesday_expiry() {
        // July 28, 2026 is a Tuesday.
        // It should return July 28 itself.
        let date = NaiveDate::from_ymd_opt(2026, 7, 28).unwrap();
        let expiry = calculate_nifty_expiry(date, &[]);
        assert_eq!(expiry, NaiveDate::from_ymd_opt(2026, 7, 28).unwrap());
    }

    #[test]
    fn test_holiday_shifted_expiry() {
        // July 28, 2026 is a Tuesday.
        // Suppose July 28, 2026 is a holiday.
        // Expiry shifts to Monday, July 27, 2026.
        let date = NaiveDate::from_ymd_opt(2026, 7, 22).unwrap();
        let holidays = vec![
            NaiveDate::from_ymd_opt(2026, 7, 28).unwrap()
        ];
        let expiry = calculate_nifty_expiry(date, &holidays);
        assert_eq!(expiry, NaiveDate::from_ymd_opt(2026, 7, 27).unwrap());
    }

    #[test]
    fn test_double_holiday_shifted_expiry() {
        // July 28, 2026 is a Tuesday.
        // Suppose July 28 (Tuesday) and July 27 (Monday) are holidays.
        // Expiry shifts to Friday, July 24, 2026.
        let date = NaiveDate::from_ymd_opt(2026, 7, 22).unwrap();
        let holidays = vec![
            NaiveDate::from_ymd_opt(2026, 7, 28).unwrap(),
            NaiveDate::from_ymd_opt(2026, 7, 27).unwrap(),
        ];
        let expiry = calculate_nifty_expiry(date, &holidays);
        assert_eq!(expiry, NaiveDate::from_ymd_opt(2026, 7, 24).unwrap());
    }

    #[test]
    fn test_format_fyers_expiry_suffix() {
        let d1 = NaiveDate::from_ymd_opt(2026, 7, 30).unwrap();
        assert_eq!(format_fyers_expiry_suffix(d1), "26730");

        let d2 = NaiveDate::from_ymd_opt(2026, 10, 5).unwrap();
        assert_eq!(format_fyers_expiry_suffix(d2), "26O05");

        let d3 = NaiveDate::from_ymd_opt(2026, 12, 15).unwrap();
        assert_eq!(format_fyers_expiry_suffix(d3), "26D15");
    }
}
