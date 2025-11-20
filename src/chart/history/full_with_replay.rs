use crate::pine_indicator::{BuiltinIndicators, PineIndicator, PineInfo, ScriptType};
use crate::utils::gen_study_id;
use crate::{
    DataPoint, DataServer, Error, Interval, MarketAdjustment, MarketSymbol, OHLCV as _, Result,
    StudyOptions, SymbolInfo, Ticker,
    chart::ChartOptions,
    error::TradingViewError,
    get_builtin_indicators, get_indicator_metadata,
    history::resolve_auth_token,
    live::handler::{
        command::CommandRunner,
        message::{Command, TradingViewResponse},
        types::{CommandTx, DataRx, DataTx},
    },
    options::Range,
    websocket::{SeriesInfo, WebSocketClient},
};
use bon::builder;
use chrono::Duration as ChronoDuration;
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::Path;
use std::{sync::Arc, time::Duration};
use tokio::time::interval;
use tokio::{
    spawn,
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use ustr::{Ustr, ustr};

#[derive(Debug)]
pub enum CompletionSignal {
    Success,
    Error(String),
    Timeout,
}

#[derive(Debug, Clone)]
struct DataCollector {
    pub data: Arc<Mutex<Vec<DataPoint>>>,
    pub symbol_info: Arc<Mutex<Option<SymbolInfo>>>,
    pub series_info: Arc<Mutex<Option<SeriesInfo>>>,
    pub completion_tx: Arc<Mutex<Option<oneshot::Sender<CompletionSignal>>>>,
    pub error_count: Arc<Mutex<u32>>,
    pub csv_writer: Arc<Mutex<Option<csv::Writer<File>>>>,
    pub last_saved_timestamp: Arc<Mutex<Option<i64>>>,
}

impl DataCollector {
    fn new(
        completion_tx: oneshot::Sender<CompletionSignal>,
        csv_writer: Option<csv::Writer<File>>,
        last_timestamp: Option<i64>,
    ) -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
            symbol_info: Arc::new(Mutex::new(None)),
            series_info: Arc::new(Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
            error_count: Arc::new(Mutex::new(0)),
            csv_writer: Arc::new(Mutex::new(csv_writer)),
            last_saved_timestamp: Arc::new(Mutex::new(last_timestamp)),
        }
    }

    async fn signal_completion(&self, signal: CompletionSignal) {
        if let Some(sender) = self.completion_tx.lock().await.take() {
            if let Err(e) = sender.send(signal) {
                tracing::error!("Failed to send completion signal: {:?}", e);
            }
        }
    }

    async fn save_data_points(&self, data_points: &[DataPoint]) -> Result<()> {
        let mut writer_guard = self.csv_writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            let mut last_ts_guard = self.last_saved_timestamp.lock().await;
            let last_saved = last_ts_guard.unwrap_or(0);

            for point in data_points {
                let timestamp = point.timestamp();

                // Skip if already saved
                if timestamp <= last_saved {
                    continue;
                }

                // Write CSV row: timestamp, open, high, low, close, volume
                writer
                    .write_record(&[
                        timestamp.to_string(),
                        point.open().to_string(),
                        point.high().to_string(),
                        point.low().to_string(),
                        point.close().to_string(),
                        point.volume().to_string(),
                    ])
                    .map_err(|e| Error::Internal(ustr(&format!("Failed to write CSV: {}", e))))?;

                *last_ts_guard = Some(timestamp);
            }

            // Flush to ensure data is written to disk
            writer
                .flush()
                .map_err(|e| Error::Internal(ustr(&format!("Failed to flush CSV: {}", e))))?;
        }
        Ok(())
    }
}

/// Read the last timestamp from an existing CSV file
fn read_last_timestamp(csv_path: &Path) -> Option<i64> {
    use std::io::{Seek, SeekFrom};

    if !csv_path.exists() {
        return None;
    }

    let mut file = File::open(csv_path).ok()?;
    let file_size = file.metadata().ok()?.len();

    // If file is empty or only has header, return None
    if file_size < 2 {
        return None;
    }

    // Start from the end and read backwards to find the last line
    let mut buffer = Vec::new();
    let mut pos = file_size.saturating_sub(1);
    let chunk_size = 1024; // Read in 1KB chunks

    loop {
        // Calculate how much to read
        let read_size = chunk_size.min(pos + 1);
        let seek_pos = pos.saturating_sub(read_size - 1);

        // Seek to position and read chunk
        file.seek(SeekFrom::Start(seek_pos)).ok()?;
        let mut chunk = vec![0u8; read_size as usize];
        std::io::Read::read_exact(&mut file, &mut chunk).ok()?;

        // Prepend to buffer
        chunk.extend_from_slice(&buffer);
        buffer = chunk;

        // Look for newline from the end (skip the very last char if it's a newline)
        let search_start = if buffer.len() > 1 && buffer[buffer.len() - 1] == b'\n' {
            buffer.len() - 2
        } else {
            buffer.len() - 1
        };

        if let Some(newline_pos) = buffer[..=search_start].iter().rposition(|&b| b == b'\n') {
            // Found a newline, extract the last line
            let last_line = &buffer[newline_pos + 1..];
            let last_line_str = String::from_utf8_lossy(last_line);

            // Parse the timestamp from the first field
            if let Some(first_field) = last_line_str.trim().split(',').next() {
                if let Ok(timestamp) = first_field.parse::<i64>() {
                    return Some(timestamp);
                }
            }
            return None;
        }

        // If we've read the entire file and found no newline, it's a single line file
        if seek_pos == 0 {
            let line_str = String::from_utf8_lossy(&buffer);
            if let Some(first_field) = line_str.trim().split(',').next() {
                if let Ok(timestamp) = first_field.parse::<i64>() {
                    return Some(timestamp);
                }
            }
            return None;
        }

        pos = seek_pos.saturating_sub(1);
    }
}

/// Initialize CSV writer, creating file if needed or opening existing one
fn init_csv_writer(csv_path: &Path) -> Result<(csv::Writer<File>, Option<i64>)> {
    let file_exists = csv_path.exists();
    let last_timestamp = if file_exists {
        read_last_timestamp(csv_path)
    } else {
        None
    };

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_path)
        .map_err(|e| Error::Internal(ustr(&format!("Failed to open CSV file: {}", e))))?;

    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

    // Write headers only if file is new
    if !file_exists {
        writer
            .write_record(&["timestamp", "open", "high", "low", "close", "volume"])
            .map_err(|e| Error::Internal(ustr(&format!("Failed to write CSV headers: {}", e))))?;
        writer
            .flush()
            .map_err(|e| Error::Internal(ustr(&format!("Failed to flush CSV: {}", e))))?;
    }

    Ok((writer, last_timestamp))
}

#[derive(Debug, Clone, Default)]
struct ReplayState {
    pub enabled: bool,
    pub send_configured: bool,
    pub configured: bool,
    pub data_received: bool,
    pub series_completed: bool,
    pub study_completed: bool,
    pub target_latest_ts: i64,
    pub step_size: i64,
    pub earliest_ts: Option<i64>,
    pub latest_ts: Option<i64>,
    pub replay_from: i64,
}

/// Fetch historical chart data from TradingView
#[builder]
pub async fn retrieve(
    auth_token: Option<&str>,
    ticker: Option<Ticker>,
    symbol: Option<&str>,
    exchange: Option<&str>,
    interval: Interval,
    from_ts: i64,
    to_ts: i64,
    #[builder(default = Duration::from_secs(30_000))] timeout_duration: Duration,
    csv_path: Option<&str>,
) -> Result<(SymbolInfo, Vec<DataPoint>)> {
    let auth_token = resolve_auth_token(auth_token)?;
    let (symbol, exchange) = extract_symbol_exchange(ticker, symbol, exchange)?;

    // Initialize CSV writer if path is provided
    let (csv_writer, last_timestamp) = if let Some(path) = csv_path {
        let csv_path = Path::new(path);
        let (writer, last_ts) = init_csv_writer(csv_path)?;

        if let Some(ts) = last_ts {
            tracing::info!("Resuming from last saved timestamp: {}", ts);
        } else {
            tracing::info!("Starting fresh CSV file at: {}", path);
        }

        (Some(writer), last_ts)
    } else {
        (None, None)
    };

    let step_size = ChronoDuration::from(interval).as_seconds_f32() as u64 * 30000;

    // If resuming, start from last saved timestamp, otherwise use from_ts
    let effective_from_ts = if let Some(last_ts) = last_timestamp {
        // Resume from the next timestamp after the last saved one
        last_ts + 1
    } else {
        from_ts
    };

    let from_ts_upd = effective_from_ts as u64 + step_size;

    // Initialize chart WITHOUT replay options - replay will be set up after receiving initial data
    let options = ChartOptions::builder()
        .symbol(symbol.clone().into())
        .exchange(exchange.clone().into())
        .interval(interval)
        .replay_mode(false)
        .adjustment(MarketAdjustment::Splits)
        .build();

    // Create completion channel
    let (completion_tx, completion_rx) = oneshot::channel::<CompletionSignal>();
    let data_collector = DataCollector::new(completion_tx, csv_writer, last_timestamp);
    let replay_state = Arc::new(Mutex::new(ReplayState {
        enabled: true,
        send_configured: false,
        configured: false,
        data_received: false,
        series_completed: false,
        study_completed: false,
        target_latest_ts: to_ts,
        step_size: step_size as i64,
        earliest_ts: None,
        latest_ts: None,
        replay_from: from_ts_upd as i64,
    }));

    // Create data response channel - this will receive TradingViewResponse messages
    let (data_tx, data_rx) = mpsc::unbounded_channel();

    // Create command channel for sending commands to the runner
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    // Initialize WebSocket client with the standard data handler
    let websocket = setup_websocket(&auth_token, Some(DataServer::ProData), data_tx).await?;
    let websocket_shared = Arc::new(websocket);

    // Create and start command runner
    let command_runner = CommandRunner::new(cmd_rx, Arc::clone(&websocket_shared));
    let shutdown_token = command_runner.shutdown_token();
    let runner_task = spawn(async move {
        if let Err(e) = command_runner.run().await {
            tracing::error!("Command runner failed: {}", e);
        }
    });

    // Start data receiver task that processes TradingViewResponse messages
    let data_task = spawn(process_data_events(
        data_rx,
        data_collector.clone(),
        Arc::clone(&replay_state),
        cmd_tx.clone(),
        options,
    ));

    // Send initial commands to set up the session
    if let Err(e) = setup_initial_session(&cmd_tx, &options).await {
        cleanup_tasks(shutdown_token, runner_task, data_task).await;
        return Err(e);
    }

    // Wait for completion with timeout
    let result = timeout(timeout_duration, completion_rx).await;

    // Cleanup
    cleanup_tasks(shutdown_token, runner_task, data_task).await;

    match result {
        Ok(Ok(CompletionSignal::Success)) => {
            let data = data_collector.data.lock().await;
            let symbol_info = data_collector.symbol_info.lock().await;

            let symbol_info = symbol_info
                .as_ref()
                .ok_or_else(|| Error::Internal(ustr("No symbol info received")))?
                .clone();

            let mut data_points = data.clone();
            data_points.dedup_by_key(|point| point.timestamp());
            data_points.sort_by_key(|a| a.timestamp());

            tracing::debug!(
                "Data collection completed with {} points",
                data_points.len()
            );
            Ok((symbol_info, data_points))
        }
        Ok(Ok(CompletionSignal::Error(error))) => Ok(Box::pin(
            retrieve()
                .auth_token(&auth_token)
                .maybe_ticker(ticker)
                .symbol(&symbol)
                .exchange(&exchange)
                .interval(interval)
                .from_ts(from_ts)
                .to_ts(to_ts)
                .timeout_duration(timeout_duration)
                .maybe_csv_path(csv_path)
                .call(),
        )
        .await?), //Err(Error::Internal(ustr(&error))),
        Ok(Ok(CompletionSignal::Timeout)) => Err(Error::Timeout(ustr("Data collection timed out"))),
        Ok(Err(_)) => Err(Error::Internal(ustr(
            "Completion channel closed unexpectedly",
        ))),
        Err(_) => Err(Error::Timeout(ustr(
            "Data collection timed out after specified duration",
        ))),
    }
}

fn extract_symbol_exchange(
    ticker: Option<Ticker>,
    symbol: Option<&str>,
    exchange: Option<&str>,
) -> Result<(String, String)> {
    if let Some(ticker) = ticker {
        Ok((ticker.symbol().to_string(), ticker.exchange().to_string()))
    } else if let Some(symbol) = symbol {
        if let Some(exchange) = exchange {
            Ok((symbol.to_string(), exchange.to_string()))
        } else {
            Err(Error::TradingView {
                source: TradingViewError::MissingExchange,
            })
        }
    } else {
        Err(Error::TradingView {
            source: TradingViewError::MissingSymbol,
        })
    }
}

/// Set up and initialize WebSocket connection with standard data handler
async fn setup_websocket(
    auth_token: &str,
    server: Option<DataServer>,
    data_tx: DataTx,
) -> Result<Arc<WebSocketClient>> {
    let websocket = WebSocketClient::builder()
        .server(server.unwrap_or(DataServer::ProData))
        .auth_token(auth_token)
        .data_tx(data_tx)
        .build()
        .await?;

    Ok(websocket)
}

/// Send initial commands to set up the trading session
async fn setup_initial_session(cmd_tx: &CommandTx, options: &ChartOptions) -> Result<()> {
    // Set market configuration
    cmd_tx
        .send(Command::set_market(*options))
        .map_err(|_| Error::Internal(ustr("Failed to send set_market command")))?;

    // // Create quote session
    // cmd_tx
    //     .send(Command::CreateQuoteSession)
    //     .map_err(|_| Error::Internal(ustr("Failed to send create_quote_session command")))?;
    //
    // // Set quote fields
    // cmd_tx
    //     .send(Command::SetQuoteFields)
    //     .map_err(|_| Error::Internal(ustr("Failed to send set_quote_fields command")))?;

    Ok(())
}

/// Process incoming TradingViewResponse messages from data_rx
async fn process_data_events(
    mut data_rx: DataRx,
    collector: DataCollector,
    replay_state: Arc<Mutex<ReplayState>>,
    cmd_tx: CommandTx,
    options: ChartOptions,
) {
    while let Some(response) = data_rx.recv().await {
        tracing::debug!("Received data response: {:?}", response);

        match response {
            TradingViewResponse::ChartData(series_info, data_points) => {
                handle_chart_data(
                    &collector,
                    &replay_state,
                    &cmd_tx,
                    &options,
                    series_info,
                    data_points,
                )
                .await;
            }
            TradingViewResponse::SymbolInfo(symbol_info) => {
                handle_symbol_info(&collector, symbol_info).await;
            }
            TradingViewResponse::SeriesCompleted(message) => {
                handle_series_completed(&collector, &replay_state, &cmd_tx, message, false).await;
            }
            TradingViewResponse::StudyCompleted(message) => {
                handle_series_completed(&collector, &replay_state, &cmd_tx, message, true).await;
            }
            TradingViewResponse::StudyData(study_options, study_data) => {
                tracing::info!(
                    "📈 Study Data: {:?} - {} points",
                    study_options,
                    study_data.studies.len()
                );
            }
            TradingViewResponse::ReplayOk(message) => {
                handle_replay_confirmation(&replay_state, "ReplayOk", message).await;
            }
            TradingViewResponse::ReplayPoint(message) => {
                handle_replay_confirmation(&replay_state, "ReplayPoint", message).await;
            }
            TradingViewResponse::Error(error, message) => {
                handle_error(&collector, error, message).await;
            }
            _ => {
                // Handle other response types as needed
                tracing::debug!("Received unhandled response type: {:?}", response);
            }
        }
    }
    tracing::debug!("Data receiver task completed");
}

async fn handle_chart_data(
    collector: &DataCollector,
    replay_state: &Arc<Mutex<ReplayState>>,
    cmd_tx: &CommandTx,
    options: &ChartOptions,
    series_info: SeriesInfo,
    data_points: Vec<DataPoint>,
) {
    tracing::debug!("Received data batch with {} points", data_points.len());

    // Store series info
    *collector.series_info.lock().await = Some(series_info.clone());

    // Save data points to CSV only AFTER replay is configured
    // This prevents saving the initial "latest candles" which have timestamps
    // newer than the replay start time, which would break checkpoint/resume logic
    if !data_points.is_empty() {
        let state = replay_state.lock().await;
        if state.configured && data_points.len() > 1 {
            drop(state); // Release lock before async operation
            if let Err(e) = collector.save_data_points(&data_points).await {
                tracing::error!("Failed to save data points to CSV: {:?}", e);
            } else {
                tracing::debug!("Saved {} data points to CSV", data_points.len());
            }
        } else {
            tracing::debug!(
                "Skipping CSV save - replay not yet configured ({} points)",
                data_points.len()
            );
        }
    }

    // Store data points
    collector
        .data
        .lock()
        .await
        .extend(data_points.iter().cloned());

    // Update replay state
    {
        let mut state = replay_state.lock().await;
        state.data_received = true;

        // Only track timestamps after replay is confirmed configured
        if state.configured && !data_points.is_empty() && data_points.len() > 1 {
            let earliest = data_points.iter().map(|p| p.timestamp()).min().unwrap_or(0);
            let latest = data_points.iter().map(|p| p.timestamp()).max().unwrap_or(0);
            state.earliest_ts = Some(
                state
                    .earliest_ts
                    .map(|existing| existing.min(earliest))
                    .unwrap_or(earliest),
            );
            state.latest_ts = Some(
                state
                    .latest_ts
                    .map(|existing| existing.max(latest))
                    .unwrap_or(latest),
            );
            tracing::debug!(
                "Updated state {:?} {:?} {} {}",
                state,
                data_points[data_points.len() - 1],
                earliest,
                latest
            );
        }
    }

    // Handle replay setup if needed - this ensures series_info is available before replay is configured
    if let Err(e) = handle_replay_setup(replay_state, cmd_tx, options).await {
        tracing::error!("Failed to setup replay: {}", e);
    }
}

async fn handle_symbol_info(collector: &DataCollector, symbol_info: SymbolInfo) {
    tracing::debug!("Received symbol info: {:?}", symbol_info);
    *collector.symbol_info.lock().await = Some(symbol_info);
}

async fn handle_series_completed(
    collector: &DataCollector,
    replay_state: &Arc<Mutex<ReplayState>>,
    cmd_tx: &CommandTx,
    message: Vec<Value>,
    is_study: bool,
) {
    tracing::debug!("Series completed with message: {:?}", message);

    let msg_json = serde_json::to_string(&message)
        .unwrap_or_else(|_| "Failed to serialize message".to_string());

    let mut state = replay_state.lock().await;

    // Early return if replay is not yet configured - this is the initial series completion
    // before replay starts. We need to keep the channel open to receive replay confirmation.
    if !state.configured {
        tracing::debug!("Series completed before replay configured - ignoring");
        return;
    }

    // Handle empty data windows - if no data was received for this window, advance to next window
    if state.latest_ts.is_none() || state.earliest_ts.is_none() {
        tracing::debug!("No data in current replay window, advancing to next window");

        // Get series info to access replay session details
        let series_info = collector
            .series_info
            .lock()
            .await
            .as_ref()
            .expect("series_info should always be available after replay is configured")
            .clone();

        // Calculate next replay timestamp
        let current_replay_ts = state.replay_from;
        let next_ts = current_replay_ts + state.step_size;

        tracing::debug!(
            "Advancing replay from {} to {} (step_size: {})",
            current_replay_ts,
            next_ts,
            state.step_size
        );

        // Check if we've reached the target
        if next_ts >= state.target_latest_ts {
            tracing::info!("Reached target timestamp, completing");
            drop(state); // Release lock before async operation
            collector.signal_completion(CompletionSignal::Success).await;
            return;
        }

        // Update replay position
        state.replay_from = next_ts;

        // Reset replay to next window
        let replay_series_info = series_info
            .replay_series_info
            .expect("replay_series_info should be available when replay is configured");

        drop(state); // Release lock before sending command

        cmd_tx
            .send(Command::ResetReplay {
                session: replay_series_info.replay_session.into(),
                series_id: replay_series_info.replay_series_id.into(),
                timestamp: next_ts,
            })
            .map_err(|_| Error::Internal(ustr("Failed to send ResetReplay command")))
            .unwrap();

        return;
    }

    let mut should_complete = false;

    // With the new pattern, series_info is ALWAYS available because replay is only configured
    // after initial data is received, ensuring series_info is set
    if state.data_received {
        // series_info is guaranteed to be available here
        let series_info = collector
            .series_info
            .lock()
            .await
            .as_ref()
            .expect("series_info should always be available after data is received")
            .clone();
        let latest = state.latest_ts.unwrap();

        if is_study {
            state.study_completed = true;
        } else {
            state.series_completed = true;
        }

        if msg_json.contains("replay") && state.series_completed {
            if latest >= state.target_latest_ts {
                should_complete = true;
            } else {
                let replay_series_info = series_info.replay_series_info.unwrap();
                let next_replay_ts = latest + state.step_size;

                cmd_tx
                    .send(Command::ResetReplay {
                        session: replay_series_info.replay_session.into(),
                        series_id: replay_series_info.replay_series_id.into(),
                        timestamp: next_replay_ts,
                    })
                    .map_err(|_| Error::Internal(ustr("Failed to send ResetReplay command")))
                    .unwrap();
            }
            state.series_completed = false;
            state.study_completed = false;
        }

        if should_complete {
            collector.signal_completion(CompletionSignal::Success).await;
        }
    }
}

async fn handle_error(collector: &DataCollector, error: Error, message: Vec<Value>) {
    tracing::error!("WebSocket error: {:?} - {:?}", error, message);

    let mut error_count = collector.error_count.lock().await;
    *error_count += 1;

    // Signal completion on critical errors or too many errors
    if is_critical_error(&error) || *error_count > 5 {
        let error_msg = format!("Critical error or too many errors: {error:?}");
        collector
            .signal_completion(CompletionSignal::Error(error_msg))
            .await;
    }
}

async fn handle_replay_confirmation(
    replay_state: &Arc<Mutex<ReplayState>>,
    message_type: &str,
    message: Vec<Value>,
) {
    let mut state = replay_state.lock().await;

    // Only set configured flag once when replay is confirmed
    if !state.configured {
        state.configured = true;
        tracing::info!(
            "✅ Replay confirmed active via {} message: {:?}",
            message_type,
            message
        );
    }
}

fn is_critical_error(error: &Error) -> bool {
    matches!(
        error,
        Error::TradingView {
            source: TradingViewError::CriticalError
        } | Error::WebSocket(_)
    )
}

/// Cleanup background tasks
async fn cleanup_tasks(
    shutdown_token: CancellationToken,
    runner_task: tokio::task::JoinHandle<()>,
    data_task: tokio::task::JoinHandle<()>,
) {
    // Signal shutdown
    shutdown_token.cancel();

    // Wait for tasks to complete with timeout
    let cleanup_timeout = Duration::from_secs(2);

    if let Err(e) = timeout(cleanup_timeout, runner_task).await {
        tracing::debug!("Command runner cleanup timeout: {:?}", e);
    }

    if let Err(e) = timeout(cleanup_timeout, data_task).await {
        tracing::debug!("Data receiver cleanup timeout: {:?}", e);
    }

    tracing::debug!("Cleanup completed");
}
/// Handle replay mode setup - called after initial data is received
async fn handle_replay_setup(
    replay_state: &Arc<Mutex<ReplayState>>,
    cmd_tx: &CommandTx,
    options: &ChartOptions,
) -> Result<()> {
    let mut state = replay_state.lock().await;

    // Only configure replay if enabled, not yet configured, and data has been received
    if state.enabled && !state.configured && !state.send_configured && state.data_received {
        tracing::debug!("Setting up replay mode after initial data received");

        let replay_from = state.replay_from;

        let mut replay_options = *options;
        replay_options.replay_mode = true;
        replay_options.replay_from = replay_from;

        tracing::debug!(
            "Configuring replay mode: from={}, to={}",
            replay_from,
            state.target_latest_ts
        );

        // Send command to update market with replay settings
        cmd_tx
            .send(Command::set_market(replay_options))
            .map_err(|_| Error::Internal(ustr("Failed to send replay market command")))?;
        state.send_configured = true;
        // Do NOT set configured = true here - wait for socket confirmation
        tracing::debug!("Replay mode configuration command sent, waiting for confirmation");
    }

    Ok(())
}
