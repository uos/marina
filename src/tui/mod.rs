//! Interactive dataset explorer, started by running `marina` with no command.

mod app;
mod jobs;
mod terminal;
mod ui;

use anyhow::Result;
use crossterm::event::{Event, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use app::App;

/// Redraw cadence for spinners and job timers.
const TICK: std::time::Duration = std::time::Duration::from_millis(120);

/// Job events handled before the next redraw. A pack or unpack reports progress
/// per chunk, so draining the queue keeps one frame from being drawn per
/// message.
const MAX_JOB_EVENTS_PER_FRAME: usize = 256;

type Screen = Terminal<CrosstermBackend<terminal::Writer>>;

pub async fn run() -> Result<()> {
    mt_dataset::cleanup::init();

    let (tx, rx) = mpsc::unbounded_channel();
    // Build the app before taking over the terminal so a config error is a
    // plain message instead of a flash of alternate screen.
    let app = App::new(tx)?;

    let (writer, mut redirect) = terminal::acquire()?;
    install_panic_hook();

    let mut screen = Terminal::new(CrosstermBackend::new(writer))?;
    enable_raw_mode()?;
    crossterm::execute!(screen.backend_mut(), EnterAlternateScreen)?;

    let result = event_loop(&mut screen, app, rx).await;

    let _ = crossterm::execute!(screen.backend_mut(), LeaveAlternateScreen);
    let _ = screen.show_cursor();
    let _ = disable_raw_mode();
    // Hands stdout back before anything is printed on it.
    redirect.restore();

    // Printed after the terminal is restored so `cd $(marina)` works the same
    // way `marina resolve` does.
    if let Some(line) = result? {
        println!("{line}");
    }
    Ok(())
}

/// Restores the terminal before a panic message reaches the screen, otherwise
/// the report lands in raw mode on the alternate screen and is unreadable.
fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let mut writer = terminal::emergency_writer();
        let _ = crossterm::execute!(writer, LeaveAlternateScreen);
        let _ = disable_raw_mode();
        previous(info);
    }));
}

/// Reads terminal events on a dedicated thread.
///
/// `crossterm::event::read` blocks, and the async `EventStream` has to be
/// re-polled to make progress, which is awkward next to a timer in the same
/// `select!`. A thread plus a channel keeps every keystroke queued while the
/// loop is busy drawing or handling job events.
fn spawn_event_reader() -> mpsc::UnboundedReceiver<std::io::Result<Event>> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::Builder::new()
        .name("marina-tui-input".to_string())
        .spawn(move || {
            loop {
                let event = crossterm::event::read();
                let failed = event.is_err();
                if tx.send(event).is_err() || failed {
                    break;
                }
            }
        })
        .expect("terminal input thread");
    rx
}

async fn event_loop(
    screen: &mut Screen,
    mut app: App,
    mut job_rx: mpsc::UnboundedReceiver<jobs::JobEvent>,
) -> Result<Option<String>> {
    let mut events = spawn_event_reader();
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        screen.draw(|frame| ui::draw(frame, &mut app))?;
        if app.should_quit {
            break;
        }

        tokio::select! {
            event = events.recv() => match event {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => app.on_key(key),
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
            job = job_rx.recv() => match job {
                Some(event) => {
                    app.on_job_event(event);
                    // Whatever else arrived while that frame was on screen.
                    for _ in 0..MAX_JOB_EVENTS_PER_FRAME {
                        match job_rx.try_recv() {
                            Ok(event) => app.on_job_event(event),
                            Err(_) => break,
                        }
                    }
                }
                None => break,
            },
            _ = ticker.tick() => app.on_tick(),
        }
    }

    Ok(app.deferred_stdout)
}
