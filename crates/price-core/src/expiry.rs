use chrono::{NaiveDate, Weekday, Datelike, DateTime, Utc, Timelike};

/// Checks if a UTC timestamp falls within Indian Standard Time (IST = UTC + 5:30) NSE market hours:
/// - Weekday: Monday to Friday
/// - Time: 09:15:00 to 15:30:00 IST
/// - Date: Not in standard NSE holidays list
pub fn is_indian_market_hours(timestamp: DateTime<Utc>) -> bool {
    let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    let ist_time = timestamp.with_timezone(&ist_offset);
    
    let weekday = ist_time.weekday();
    if weekday == Weekday::Sat || weekday == Weekday::Sun {
        return false;
    }
    
    let hour = ist_time.hour();
    let minute = ist_time.minute();
    let second = ist_time.second();
    
    let total_secs = hour * 3600 + minute * 60 + second;
    let market_start_secs = 9 * 3600 + 15 * 60; // 09:15:00
    let market_end_secs = 15 * 3600 + 30 * 60;  // 15:30:00
    
    if total_secs < market_start_secs || total_secs > market_end_secs {
        return false;
    }
    
    let date = ist_time.date_naive();
    let holidays = get_nse_holidays_2026();
    if holidays.contains(&date) {
        return false;
    }
    
    true
}

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

    #[test]
    fn test_is_indian_market_hours() {
        use chrono::TimeZone;
        
        // 1. Wed, July 22, 2026 at 10:30 AM IST (which is 05:00 AM UTC) -> should be true
        let dt_open = Utc.with_ymd_and_hms(2026, 7, 22, 5, 0, 0).unwrap();
        assert!(is_indian_market_hours(dt_open));

        // 2. Wed, July 22, 2026 at 08:30 AM IST (which is 03:00 AM UTC) -> should be false (too early)
        let dt_early = Utc.with_ymd_and_hms(2026, 7, 22, 3, 0, 0).unwrap();
        assert!(!is_indian_market_hours(dt_early));

        // 3. Wed, July 22, 2026 at 04:00 PM IST (which is 10:30 AM UTC) -> should be false (too late)
        let dt_late = Utc.with_ymd_and_hms(2026, 7, 22, 10, 30, 0).unwrap();
        assert!(!is_indian_market_hours(dt_late));

        // 4. Sun, July 26, 2026 at 11:00 AM IST (which is 05:30 AM UTC) -> should be false (weekend)
        let dt_weekend = Utc.with_ymd_and_hms(2026, 7, 26, 5, 30, 0).unwrap();
        assert!(!is_indian_market_hours(dt_weekend));

        // 5. Republic Day 2026 (Jan 26, Mon) -> should be false (holiday)
        let dt_holiday = Utc.with_ymd_and_hms(2026, 1, 26, 5, 30, 0).unwrap();
        assert!(!is_indian_market_hours(dt_holiday));
    }
}
