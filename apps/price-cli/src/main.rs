use clap::{Parser, Subcommand};
use chrono::NaiveDate;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use dotenvy::dotenv;
use price_timeseries::TimescaleClient;
use price_backtester::{HistoricalDownloader, ReplayRunner};

#[derive(Parser)]
#[command(name = "price")]
#[command(about = "PRICE Command Line Tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download historical market data
    Download {
        /// Symbol (e.g. NSE:NIFTY50-INDEX)
        #[arg(long)]
        symbol: String,

        /// Exchange prefix
        #[arg(long, default_value = "NSE")]
        exchange: String,

        /// Starting date (YYYY-MM-DD)
        #[arg(long)]
        from: String,

        /// Ending date (YYYY-MM-DD)
        #[arg(long)]
        to: String,
    },
    /// Run historical strategy backtest
    Backtest {
        /// Symbol (e.g. NSE:NIFTY50-INDEX)
        #[arg(long, default_value = "NSE:NIFTY50-INDEX")]
        symbol: String,

        /// Starting date (YYYY-MM-DD)
        #[arg(long)]
        from: String,

        /// Ending date (YYYY-MM-DD)
        #[arg(long)]
        to: String,

        /// Initial capital balance (INR)
        #[arg(long, default_value = "100000.0")]
        capital: f64,

        /// Output folder path for generated reports
        #[arg(long, default_value = "results")]
        output: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let cli = Cli::parse();

    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/price".to_string());
    let python_broker_url = std::env::var("PYTHON_BROKER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8001".to_string());

    let db = TimescaleClient::new(&db_url).await?;
    db.init_db().await?;

    match cli.command {
        Commands::Download { symbol, exchange, from, to } => {
            let from_date = NaiveDate::parse_from_str(&from, "%Y-%m-%d")?;
            let to_date = NaiveDate::parse_from_str(&to, "%Y-%m-%d")?;

            let downloader = HistoricalDownloader::new(&python_broker_url, db);
            downloader.download_history(&symbol, &exchange, from_date, to_date).await?;
        }
        Commands::Backtest { symbol, from, to, capital, output } => {
            let from_date = NaiveDate::parse_from_str(&from, "%Y-%m-%d")?;
            let to_date = NaiveDate::parse_from_str(&to, "%Y-%m-%d")?;

            let runner = ReplayRunner::new(db);
            let report = runner.run_backtest(&symbol, from_date, to_date, capital, &output).await?;

            println!("==========================================");
            println!("           BACKTEST RESULTS SUMMARY       ");
            println!("==========================================");
            println!("Total Trades Executed : {}", report.total_trades);
            println!("Winning Trades        : {}", report.winning_trades);
            println!("Losing Trades         : {}", report.losing_trades);
            println!("Win Rate              : {:.2}%", report.win_rate * 100.0);
            println!("Initial Capital       : Rs {:.2}", report.initial_capital);
            println!("Final Equity          : Rs {:.2}", report.final_equity);
            println!("Net Profit            : Rs {:.2} ({:.2}%)", report.net_profit, report.net_profit_pct);
            println!("Max Drawdown          : {:.2}%", report.max_drawdown_pct);
            println!("Sharpe Ratio (Daily)  : {:.2}", report.sharpe_ratio);
            println!("==========================================");
            println!("Reports written to: {}", output);
        }
    }

    Ok(())
}
