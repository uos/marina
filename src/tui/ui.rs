//! Rendering for the TUI screens.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, BorderType, Cell, Clear, List, ListItem, Padding, Paragraph, Row, Table, Tabs, Wrap,
};

use crate::format::{format_bag_info, format_pushed_at, human_bytes, human_bytes_compact};

use super::app::{
    App, DatasetRow, FieldKind, Focus, JobStatus, Modal, RemoteState, SPINNER, Screen, VisibleRow,
};
use super::jobs::{JobId, JobTarget};

const ACCENT: Color = Color::Cyan;

pub fn draw(frame: &mut Frame, app: &mut App) {
    let active = app.active_jobs();
    // The detail pane rarely fills its column, so running jobs share it instead
    // of taking a strip across the whole width. Screens without a detail pane
    // only grow the column while something is running.
    let detail = app.screen == Screen::Datasets;
    let sidebar = detail || !active.is_empty();

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let body = if sidebar {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(rows[1])
    } else {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(100)])
            .split(rows[1])
    };

    draw_tabs(frame, rows[0], app);
    match app.screen {
        Screen::Datasets => draw_datasets(frame, body[0], app),
        Screen::Registries => draw_registries(frame, body[0], app),
        Screen::Settings => draw_settings(frame, body[0], app),
    }

    if sidebar {
        let column = body[1];
        let jobs_height = jobs_height(&active, column.height);
        let split = if detail {
            // Detail on top, jobs docked under it.
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(3), Constraint::Length(jobs_height)])
                .split(column)
        } else {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(jobs_height), Constraint::Min(0)])
                .split(column)
        };
        let (detail_area, jobs_area) = if detail {
            (Some(split[0]), split[1])
        } else {
            (None, split[0])
        };
        if let Some(area) = detail_area {
            draw_detail(frame, area, app);
        }
        if !active.is_empty() {
            draw_jobs(frame, jobs_area, app, &active);
        }
    }

    draw_status(frame, rows[2], app);

    if app.modal.is_some() {
        draw_modal(frame, app);
    }
}

/// Height of the jobs dock: two lines per job in a narrow column, never more
/// than half of it, and nothing at all when idle.
fn jobs_height(active: &[JobId], column_height: u16) -> u16 {
    if active.is_empty() {
        return 0;
    }
    let wanted = active.len() as u16 * 2 + 2;
    wanted.min((column_height / 2).max(4))
}

fn spinner(app: &App) -> &'static str {
    SPINNER[(app.tick / 2) % SPINNER.len()]
}

fn draw_tabs(frame: &mut Frame, area: Rect, app: &App) {
    let titles = ["1 datasets", "2 registries", "3 settings"];
    let selected = match app.screen {
        Screen::Datasets => 0,
        Screen::Registries => 1,
        Screen::Settings => 2,
    };
    let running = app
        .jobs
        .values()
        .filter(|job| job.status == JobStatus::Running)
        .count();
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(20), Constraint::Length(24)])
        .split(area);

    frame.render_widget(
        Tabs::new(titles.to_vec())
            .select(selected)
            .highlight_style(Style::default().fg(ACCENT).add_modifier(Modifier::BOLD))
            .divider(" "),
        columns[0],
    );

    let jobs_label = if running > 0 {
        Span::styled(
            format!("{} {running} running ", spinner(app)),
            Style::default().fg(ACCENT),
        )
    } else {
        Span::styled("idle ", Style::default().fg(Color::DarkGray))
    };
    frame.render_widget(
        Paragraph::new(Line::from(jobs_label)).alignment(Alignment::Right),
        columns[1],
    );
}

fn draw_datasets(frame: &mut Frame, area: Rect, app: &mut App) {
    let time_display = app.settings.time_display;

    let rows: Vec<Row> = app
        .visible
        .iter()
        .map(|entry| match entry {
            VisibleRow::Spacer => Row::new(Vec::<Cell>::new()),
            VisibleRow::Namespace(namespace) => {
                Row::new(vec![Cell::from(Line::from(Span::styled(
                    format!(" {namespace}/"),
                    Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
                )))])
            }
            VisibleRow::Group { name, namespaced } => {
                // Sits exactly above the dimmed prefixes of its rows.
                let indent = if *namespaced { "    " } else { "  " };
                Row::new(vec![Cell::from(Line::from(Span::styled(
                    format!("{indent}{name}"),
                    Style::default().add_modifier(Modifier::BOLD),
                )))])
            }
            VisibleRow::Dataset { index, grouped } => {
                let row = &app.rows[*index];
                let (_, remote_size, _, _, pushed) = format_bag_info(row.info(), time_display);
                let busy = app.is_busy(&JobTarget::Dataset(row.key.clone()));
                // A cached dataset reports what is actually on disk; anything
                // else reports what the registry holds.
                let size = if row.is_cached() {
                    human_bytes_compact(row.local_bytes)
                } else {
                    remote_size
                };
                Row::new(vec![
                    Cell::from(dataset_cell(row, *grouped, busy.then(|| spinner(app)))),
                    Cell::from(presence_cell(row)),
                    Cell::from(truncate(&row.registry_label(), 14)),
                    Cell::from(Line::from(size).alignment(Alignment::Right)),
                    Cell::from(pushed),
                ])
            }
        })
        .collect();

    let widths = [
        // Names get room to grow, but not the whole table: the trailing spacer
        // takes the slack so the columns stay packed to the left.
        Constraint::Max(46),
        Constraint::Length(4),
        Constraint::Length(14),
        Constraint::Length(5),
        Constraint::Length(10),
        Constraint::Min(0),
    ];
    let title = format!(
        " datasets · {} ({} of {}){} ",
        app.scope.label(),
        app.dataset_count(),
        app.rows.len(),
        if app.hide_duplicates {
            " · duplicates hidden"
        } else {
            ""
        }
    );
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec![
                Line::from("  DATASET"),
                Line::from("L R"),
                Line::from("REGISTRY"),
                Line::from("SIZE").alignment(Alignment::Right),
                Line::from("PUSHED"),
                Line::from(""),
            ])
            .style(Style::default().fg(Color::DarkGray)),
        )
        .block(panel(&title, app.focus == Focus::Main))
        .row_highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        );

    // Rows minus the border and the header, so page keys move a real screen.
    app.viewport_rows = area.height.saturating_sub(3) as usize;
    app.dataset_table_area = area;
    frame.render_stateful_widget(table, area, &mut app.table_state);
}

/// Dataset name cell: the namespace lives in its section heading, and inside a
/// group the shared base name is dimmed so only the tags stand out.
fn dataset_cell(row: &DatasetRow, grouped: bool, spinner: Option<&str>) -> Line<'static> {
    let mut spans = vec![Span::styled(
        spinner.map(|s| format!("{s} ")).unwrap_or("  ".to_string()),
        Style::default().fg(ACCENT),
    )];
    if row.namespace.is_some() {
        spans.push(Span::raw("  "));
    }

    match grouped
        .then(|| row.display_name.strip_prefix(row.base_name.as_str()))
        .flatten()
    {
        Some(rest) => {
            spans.push(Span::styled(
                row.base_name.clone(),
                Style::default().fg(Color::DarkGray),
            ));
            spans.push(Span::raw(rest.to_string()));
        }
        None => spans.push(Span::raw(row.display_name.clone())),
    }
    Line::from(spans)
}

/// Where the dataset is, in two slots: `L` for the local cache, `R` for a
/// registry. A missing side is a dim dot, so the pair reads at a glance and the
/// `L R` header is the whole legend.
fn presence_cell(row: &DatasetRow) -> Line<'static> {
    let slot = |present: bool, label: &'static str, color: Color| {
        if present {
            Span::styled(label, Style::default().fg(color))
        } else {
            Span::styled("·", Style::default().fg(Color::DarkGray))
        }
    };
    Line::from(vec![
        slot(row.is_cached(), "L", Color::Green),
        slot(row.is_remote(), "R", ACCENT),
    ])
}

fn short_hash(hash: &str) -> String {
    if hash == "-" {
        hash.to_string()
    } else {
        hash.chars().take(12).collect()
    }
}

/// Section heading inside the detail column.
fn section(title: &str, accent: bool) -> Line<'static> {
    let style = if accent {
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    };
    Line::from(Span::styled(title.to_string(), style))
}

/// One `label   value` row. The column is narrow, so every fact gets its own
/// line rather than being paired up with its neighbour.
fn field(label: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("  {label:<8}"),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(value.into()),
    ])
}

fn dim(text: impl Into<String>) -> Line<'static> {
    Line::from(Span::styled(
        text.into(),
        Style::default().fg(Color::DarkGray),
    ))
}

fn draw_detail(frame: &mut Frame, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();

    match app.selected_row() {
        None => lines.push(dim("no dataset selected")),
        Some(row) => {
            lines.push(section(&row.key, true));

            lines.push(Line::from(""));
            lines.push(section("LOCAL", false));
            match &row.local_dir {
                Some(dir) => {
                    lines.push(field("size", human_bytes(row.local_bytes)));
                    // The path gets the full width: it does not fit beside a
                    // label, and it is what you came here to copy.
                    lines.push(dim(format!("  {}", abbreviate_home(dir))));
                }
                None => lines.push(dim("  not in the cache")),
            }

            if row.remotes.is_empty() {
                lines.push(Line::from(""));
                lines.push(section("REGISTRIES", false));
                lines.push(dim("  none"));
            }
            for (registry, info) in &row.remotes {
                lines.push(Line::from(""));
                lines.push(section(registry, true));
                let Some(info) = info else {
                    lines.push(dim("  no metadata published"));
                    continue;
                };
                lines.push(field("size", human_bytes(info.original_bytes)));
                lines.push(field(
                    "packed",
                    format!(
                        "{}{}",
                        human_bytes(info.packed_bytes),
                        packed_ratio(info.original_bytes, info.packed_bytes)
                    ),
                ));
                lines.push(field(
                    "clouds",
                    info.pointcloud.clone().unwrap_or_else(|| "-".to_string()),
                ));
                lines.push(field(
                    "archive",
                    info.mcap_compression
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                ));
                if let Some(hash) = info.bundle_hash.as_deref() {
                    lines.push(field("hash", short_hash(hash)));
                }
                lines.push(field(
                    "pushed",
                    format_pushed_at(info.pushed_at, app.settings.time_display),
                ));
            }

            lines.push(Line::from(""));
            lines.push(section("FILES", false));
            if !row.is_cached() {
                lines.push(dim("  pull the dataset to list its files"));
            } else if app.files.is_empty() {
                lines.push(dim("  the cache directory is empty"));
            } else {
                for file in &app.files {
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("  {:>7}  ", human_bytes_compact(file.size_bytes)),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::raw(file.relative_path.clone()),
                    ]));
                }
                if app.files_truncated > 0 {
                    lines.push(dim(format!("  +{} more", app.files_truncated)));
                }
            }
        }
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            // No border: the detail column is never focusable, and a border
            // here reads as "you can Tab into this".
            .block(Block::default().padding(Padding::new(1, 1, 1, 0))),
        area,
    );
}

/// Writes a cache path as `~/.cache/…` so it stands a chance of fitting the
/// column.
fn abbreviate_home(path: &std::path::Path) -> String {
    let display = path.display().to_string();
    let Some(home) = dirs::home_dir() else {
        return display;
    };
    match path.strip_prefix(&home) {
        Ok(rest) => format!("~/{}", rest.display()),
        Err(_) => display,
    }
}

/// ` · 19%` next to a packed size, or nothing when there is no ratio to give.
fn packed_ratio(original: u64, packed: u64) -> String {
    if original == 0 {
        return String::new();
    }
    format!(" · {:.0}%", packed as f64 / original as f64 * 100.0)
}

fn draw_registries(frame: &mut Frame, area: Rect, app: &mut App) {
    let configs: Vec<_> = app
        .marina
        .list_registry_configs()
        .into_iter()
        .cloned()
        .collect();

    if configs.is_empty() {
        frame.render_widget(
            Paragraph::new("no registries configured — press a to add one")
                .block(panel(" registries ", app.focus == Focus::Main)),
            area,
        );
        return;
    }

    let rows: Vec<Row> = configs
        .iter()
        .enumerate()
        .map(|(index, cfg)| {
            let state = app
                .registry_state
                .get(&cfg.name)
                .cloned()
                .unwrap_or(RemoteState::Idle);
            let (status, style) = match &state {
                RemoteState::Idle => ("idle".to_string(), Style::default().fg(Color::DarkGray)),
                RemoteState::Loading => (
                    format!("{} loading", spinner(app)),
                    Style::default().fg(ACCENT),
                ),
                RemoteState::Loaded(count) => (
                    format!("{count} datasets"),
                    Style::default().fg(Color::Green),
                ),
                RemoteState::Failed(error) => (
                    first_line(error).to_string(),
                    Style::default().fg(Color::Red),
                ),
            };
            let selected = index == app.registry_state_index;
            let marker = if selected { "❯ " } else { "  " };
            let default_marker = if app.settings.default_registry.as_deref() == Some(&cfg.name) {
                "default"
            } else {
                ""
            };
            Row::new(vec![
                Cell::from(format!("{marker}{}", truncate(&cfg.name, 24))),
                Cell::from(cfg.kind.clone()),
                Cell::from(cfg.uri.clone()),
                Cell::from(default_marker),
                Cell::from(status).style(style),
            ])
            .style(if selected {
                Style::default().bg(Color::DarkGray)
            } else {
                Style::default()
            })
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(26),
            Constraint::Length(8),
            Constraint::Min(30),
            Constraint::Length(8),
            Constraint::Length(28),
        ],
    )
    .header(
        Row::new(vec!["  NAME", "KIND", "URI", "", "STATUS"])
            .style(Style::default().fg(Color::DarkGray)),
    )
    .block(panel(
        " registries — a add · D remove · A auth · m mirror · R refresh · ⏎ default ",
        app.focus == Focus::Main,
    ));

    frame.render_widget(table, area);
}

fn draw_settings(frame: &mut Frame, area: Rect, app: &App) {
    let entries = [
        ("time display", format!("{:?}", app.settings.time_display)),
        (
            "default registry",
            app.settings
                .default_registry
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "registry timeout",
            format!("{}s", app.settings.registry_timeout_secs),
        ),
        (
            "completion cache ttl",
            format!("{}s", app.settings.completion_cache_ttl_secs),
        ),
        (
            "pointcloud mode",
            format!("{:?}", app.compression.pointcloud_mode),
        ),
        (
            "packed mcap compression",
            format!("{:?}", app.compression.packed_mcap_compression),
        ),
    ];

    let items: Vec<ListItem> = entries
        .iter()
        .enumerate()
        .map(|(index, (label, value))| {
            let selected = index == app.settings_selected;
            let style = if selected {
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(vec![
                Span::raw(if selected { "❯ " } else { "  " }),
                Span::raw(format!("{label:<26}")),
                Span::styled(value.to_lowercase(), Style::default().fg(ACCENT)),
            ]))
            .style(style)
        })
        .collect();

    frame.render_widget(
        List::new(items).block(panel(
            " settings — ←/→ change · w write to the config file ",
            app.focus == Focus::Main,
        )),
        area,
    );
}

fn draw_jobs(frame: &mut Frame, area: Rect, app: &App, active: &[JobId]) {
    let width = area.width.saturating_sub(4) as usize;
    // Two lines per job, plus one for the "more" note when they do not all fit.
    let capacity = (area.height.saturating_sub(2) / 2) as usize;
    let shown = active.len().min(capacity.max(1));
    let hidden = active.len() - shown;

    // Oldest first, so a new job appears at the bottom and the cursor keeps
    // pointing at the same row.
    let mut items: Vec<ListItem> = active
        .iter()
        .take(shown)
        .enumerate()
        .filter_map(|(position, id)| app.jobs.get(id).map(|job| (position, job)))
        .map(|(position, job)| {
            let (symbol, style) = match job.status {
                JobStatus::Detached => ("⇢".to_string(), Style::default().fg(Color::Yellow)),
                _ => (spinner(app).to_string(), Style::default().fg(ACCENT)),
            };
            let selected = app.focus == Focus::Jobs && position == app.job_selected;
            let head = Line::from(vec![
                Span::raw(if selected { "❯" } else { " " }),
                Span::styled(symbol, style),
                Span::raw(" "),
                Span::raw(truncate(&job.kind.label(), width.saturating_sub(3))),
            ]);
            let detail = Line::from(Span::styled(
                format!(
                    "   {:>3}s  {}",
                    job.elapsed_secs(),
                    truncate(first_line(&job.last_line), width.saturating_sub(9))
                ),
                Style::default().fg(Color::DarkGray),
            ));
            ListItem::new(vec![head, detail]).style(if selected {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            })
        })
        .collect();

    if hidden > 0 {
        items.push(ListItem::new(Line::from(Span::styled(
            format!("  +{hidden} more running"),
            Style::default().fg(Color::DarkGray),
        ))));
    }

    frame.render_widget(
        List::new(items).block(panel(
            &format!(" running ({}) ", active.len()),
            app.focus == Focus::Jobs,
        )),
        area,
    );
}

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    let left = if app.filter_editing {
        Span::styled(
            format!("filter: {}_", app.filter),
            Style::default().fg(ACCENT),
        )
    } else if !app.filter.is_empty() {
        Span::raw(format!("filter: {}", app.filter))
    } else {
        Span::raw(truncate(first_line(&app.status), area.width as usize / 2))
    };

    let hint = if app.screen == Screen::Datasets && app.focus == Focus::Main {
        "⏎ copy · ? help · / filter · r refresh · q quit"
    } else {
        "? help · / filter · r refresh · q quit"
    };

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(10), Constraint::Length(54)])
        .split(area);
    frame.render_widget(Paragraph::new(Line::from(left)), columns[0]);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            hint,
            Style::default().fg(Color::DarkGray),
        )))
        .alignment(Alignment::Right),
        columns[1],
    );
}

/// Rows a line occupies once the paragraph wraps it into `width` columns.
///
/// The paragraph breaks on whitespace, so a token that cannot fit — a cache
/// path, typically — starts a fresh row and then fills whole ones. Counting
/// `len / width` instead underestimates exactly those lines, which is how the
/// newest log line used to get pushed off the bottom.
fn wrapped_rows(line: &str, width: usize) -> usize {
    if width == 0 {
        return 1;
    }
    let mut rows = 1usize;
    let mut column = 0usize;
    for word in line.split_whitespace() {
        let length = word.chars().count();
        // The separating space only costs a column mid-row.
        if column > 0 && column + 1 + length > width {
            rows += 1;
            column = 0;
        }
        if column == 0 && length > width {
            rows += (length - 1) / width;
            column = (length - 1) % width + 1;
        } else if column == 0 {
            column = length;
        } else {
            column += 1 + length;
        }
    }
    rows
}

const MODAL_WIDTH_PERCENT: u16 = 78;

fn draw_modal(frame: &mut Frame, app: &App) {
    let Some(modal) = app.modal.as_ref() else {
        return;
    };
    // Known before the layout so the log can budget for wrapped lines.
    let inner_width = (frame.area().width * MODAL_WIDTH_PERCENT / 100).saturating_sub(2) as usize;
    let (title, lines, height) = match modal {
        Modal::Help => {
            let lines = help_lines();
            let height = lines.len() as u16 + 2;
            (" help ".to_string(), lines, height)
        }
        Modal::Confirm { title, body, .. } => {
            let mut lines: Vec<Line> = body.iter().map(|line| Line::from(line.clone())).collect();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "y confirm · n cancel",
                Style::default().fg(Color::DarkGray),
            )));
            let height = lines.len() as u16 + 4;
            (format!(" {title} "), lines, height)
        }
        Modal::JobHistory { selected } => {
            let mut lines: Vec<Line> = app
                .job_order
                .iter()
                .rev()
                .enumerate()
                .filter_map(|(position, id)| app.jobs.get(id).map(|job| (position, job)))
                .map(|(position, job)| {
                    let (symbol, style) = match job.status {
                        JobStatus::Running => {
                            (spinner(app).to_string(), Style::default().fg(ACCENT))
                        }
                        JobStatus::Done => ("✔".to_string(), Style::default().fg(Color::Green)),
                        JobStatus::Failed => ("✖".to_string(), Style::default().fg(Color::Red)),
                        JobStatus::Detached => {
                            ("⇢".to_string(), Style::default().fg(Color::Yellow))
                        }
                    };
                    let current = position == *selected;
                    Line::from(vec![
                        Span::raw(if current { "❯ " } else { "  " }),
                        Span::styled(symbol, style),
                        Span::raw(format!(" {:<34}", truncate(&job.kind.label(), 34))),
                        Span::styled(
                            format!("{:>4}s  ", job.elapsed_secs()),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::raw(truncate(first_line(&job.last_line), 40)),
                    ])
                    .style(if current {
                        Style::default().add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    })
                })
                .collect();
            if lines.is_empty() {
                lines.push(Line::from(Span::styled(
                    "  nothing has run yet",
                    Style::default().fg(Color::DarkGray),
                )));
            }
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "⏎ log · C clear finished · esc close",
                Style::default().fg(Color::DarkGray),
            )));
            let height = (lines.len() as u16 + 2).min(24);
            (" job history ".to_string(), lines, height)
        }
        Modal::JobLog { id, scroll } => {
            const MAX_ROWS: usize = 18;

            let log: Vec<String> = app
                .jobs
                .get(id)
                .map(|job| {
                    if job.log.is_empty() {
                        vec![job.last_line.clone()]
                    } else {
                        job.log.clone()
                    }
                })
                .unwrap_or_else(|| vec!["job is gone".to_string()]);

            // Fill the window from the newest line backwards, counting the rows
            // each line takes once wrapped. Counting logical lines instead used
            // to push the newest one off the bottom whenever an earlier line —
            // a long archive path, say — wrapped.
            let end = log.len().saturating_sub(*scroll).max(1);
            let mut rows = 0usize;
            let mut start = end;
            while start > 0 {
                let needed = wrapped_rows(&log[start - 1], inner_width);
                if rows > 0 && rows + needed > MAX_ROWS {
                    break;
                }
                rows += needed;
                start -= 1;
            }

            let lines: Vec<Line> = log[start..end]
                .iter()
                .map(|line| Line::from(line.clone()))
                .collect();

            let title = if *scroll == 0 {
                format!(
                    " job log · {} line{} · ↑/↓ scroll ",
                    log.len(),
                    if log.len() == 1 { "" } else { "s" }
                )
            } else {
                format!(" job log · {end} of {} · end follows ", log.len())
            };
            (title, lines, (rows as u16 + 2).clamp(4, 20))
        }
        Modal::Form(form) => {
            let mut lines: Vec<Line> = form
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let selected = index == form.selected;
                    let value = match field.kind {
                        FieldKind::Text if selected => format!("{}_", field.value_display()),
                        _ => field.value_display(),
                    };
                    Line::from(vec![
                        Span::raw(if selected { "❯ " } else { "  " }),
                        Span::raw(format!("{:<34}", field.label)),
                        Span::styled(value, Style::default().fg(ACCENT)),
                    ])
                    .style(if selected {
                        Style::default().add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    })
                })
                .collect();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "↑/↓ field · ←/→ or space toggle · ⏎ run · esc cancel",
                Style::default().fg(Color::DarkGray),
            )));
            let height = lines.len() as u16 + 4;
            (format!(" {} ", form.title), lines, height)
        }
    };

    let area = centered(frame.area(), MODAL_WIDTH_PERCENT, height);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            .block(
                Block::bordered()
                    .border_type(BorderType::Rounded)
                    .border_style(Style::default().fg(ACCENT))
                    .title(title),
            ),
        area,
    );
}

fn help_lines() -> Vec<Line<'static>> {
    [
        "global    1/2/3 screens · ⇥ focus running jobs · J job history · r refresh · ? help · q quit",
        "",
        "datasets  L = in the local cache · R = in a registry (which one is in REGISTRY)",
        "          ↑/↓ move · / filter · l cycle all/local/remote · s search registries",
        "          d hide duplicate registries · ^f/^b page · ^d/^u half page · g/G ends",
        "          ⏎ copy the dataset's identifier · o resolve it and copy its local path",
        "          p pull · P push · I import · e export · x remove · M mirror cache over ssh",
        "          C clean the cache",
        "",
        "registries a add · D remove · A authenticate gdrive · m mirror into another registry",
        "          R refresh this registry · ⏎ make it the default",
        "",
        "settings  ←/→ change a value · w write it to the config file",
        "",
        "jobs      the dock under the detail pane lists only what is running;",
        "          ⏎ opens its log, x detaches it",
        "          J lists every job this session ran, with its log",
    ]
    .into_iter()
    .map(Line::from)
    .collect()
}

fn panel(title: &str, focused: bool) -> Block<'_> {
    let border = if focused {
        Style::default().fg(ACCENT)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(border)
        .title(title.to_string())
}

fn centered(area: Rect, width_percent: u16, height: u16) -> Rect {
    let width = area.width * width_percent / 100;
    let height = height.min(area.height.saturating_sub(2));
    Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    }
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or(text)
}

fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    let mut out: String = text.chars().take(width.saturating_sub(1)).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrapping_counts_the_rows_a_line_really_takes() {
        assert_eq!(
            wrapped_rows("", 40),
            1,
            "an empty line still occupies a row"
        );
        assert_eq!(wrapped_rows("short", 40), 1);
        assert_eq!(wrapped_rows(&"x".repeat(40), 40), 1);
        assert_eq!(wrapped_rows(&"x".repeat(41), 40), 2);
        assert_eq!(wrapped_rows(&"x".repeat(120), 40), 3);
        assert_eq!(
            wrapped_rows("anything", 0),
            1,
            "a zero-width modal cannot divide"
        );
    }

    #[test]
    fn an_unbreakable_path_starts_its_own_row() {
        // The line that used to push the live progress line off the bottom.
        let line = "[unpack] extracting archive /home/chsieh/.cache/marina/bags/fwurm_robosense-test/bundle.remote.tar.gz";
        assert_eq!(
            wrapped_rows(line, 72),
            3,
            "the words fit on one row, the path takes two more"
        );
    }

    #[test]
    fn words_pack_onto_a_row_until_they_do_not_fit() {
        assert_eq!(wrapped_rows("aaa bbb ccc", 11), 1);
        assert_eq!(wrapped_rows("aaa bbb ccc", 10), 2);
        assert_eq!(wrapped_rows("aaa bbb ccc ddd", 7), 2);
    }
}
