//! TUI state and input handling.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::time::Instant;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::widgets::TableState;
use tokio::sync::mpsc::UnboundedSender;

use mt_dataset::core::{CacheMirrorOptions, Marina, PullOptions, PushOptions};
use mt_dataset::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
use mt_dataset::io::pack::ArchiveCompression;
use mt_dataset::model::bag_ref::BagRef;
use mt_dataset::registry::driver::BagInfo;
use mt_dataset::storage::config::{
    self, CompressionConfig, ConfigArchiveCompression, ConfigMcapCompression, ConfigPointcloudMode,
    RegistryConfig, RegistryDownloadMode, Settings, TimeDisplay,
};

use super::jobs::{self, JobEvent, JobId, JobKind, JobOutcome, JobTarget, Refresh};

/// Transfers running at once. Listings are not throttled: they are a single
/// round trip each and are the whole reason the TUI feels different from `list`.
const MAX_ACTIVE_TRANSFERS: usize = 4;

/// Progress lines kept per job. A pack or unpack reports every chunk, so the
/// log is a tail, not a transcript.
const MAX_JOB_LOG_LINES: usize = 200;

pub const SPINNER: [&str; 8] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Datasets,
    Registries,
    Settings,
}

/// Which side of the catalog the overview lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Scope {
    #[default]
    All,
    /// Only what is in the local cache.
    Local,
    /// Only what a registry carries.
    Remote,
}

impl Scope {
    fn next(self) -> Self {
        match self {
            Self::All => Self::Local,
            Self::Local => Self::Remote,
            Self::Remote => Self::All,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }

    fn accepts(self, row: &DatasetRow) -> bool {
        match self {
            Self::All => true,
            Self::Local => row.is_cached(),
            Self::Remote => row.is_remote(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Main,
    Jobs,
}

/// Per-registry listing state, shown as a spinner or badge on the registry row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteState {
    Idle,
    Loading,
    Loaded(usize),
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Running,
    Done,
    Failed,
    Detached,
}

pub struct JobState {
    pub kind: JobKind,
    pub started: Instant,
    pub finished: Option<Instant>,
    pub status: JobStatus,
    pub last_line: String,
    pub log: Vec<String>,
}

impl JobState {
    /// Appends a log line, dropping the oldest once the tail is full.
    fn push_log(&mut self, line: String) {
        // An unpack reports every chunk. Keeping one live counter instead of
        // twelve thousand lines is what makes the log readable.
        if let Some(previous) = self.log.last_mut()
            && supersedes(previous, &line)
        {
            *previous = line;
            return;
        }
        if self.log.len() == MAX_JOB_LOG_LINES {
            self.log.remove(0);
        }
        self.log.push(line);
    }

    pub fn elapsed_secs(&self) -> u64 {
        let end = self.finished.unwrap_or_else(Instant::now);
        end.duration_since(self.started).as_secs()
    }
}

/// One dataset, merged across the local cache and every registry that has it.
#[derive(Debug, Clone)]
pub struct DatasetRow {
    pub bag: BagRef,
    pub key: String,
    pub namespace: Option<String>,
    pub base_name: String,
    pub display_name: String,
    pub local_dir: Option<PathBuf>,
    pub local_bytes: u64,
    pub remotes: Vec<(String, Option<BagInfo>)>,
}

/// One file inside a cached dataset.
#[derive(Debug, Clone)]
pub struct CachedFile {
    pub relative_path: String,
    pub size_bytes: u64,
}

/// One line of the dataset list: the section headings that group the rows the
/// way `marina list --remote` prints them, and the rows themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisibleRow {
    /// `namespace/` heading.
    Namespace(String),
    /// Base name shared by several tag variants below it.
    Group {
        name: String,
        /// Sits inside a namespace section, so it carries that indent too.
        namespaced: bool,
    },
    Dataset {
        index: usize,
        /// Part of a multi-variant group, so the shared prefix can be dimmed.
        grouped: bool,
    },
    Spacer,
}

impl DatasetRow {
    pub fn is_cached(&self) -> bool {
        self.local_dir.is_some()
    }

    /// In at least one registry.
    pub fn is_remote(&self) -> bool {
        !self.remotes.is_empty()
    }

    pub fn registry_label(&self) -> String {
        if self.remotes.is_empty() {
            "-".to_string()
        } else {
            self.remotes
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }

    /// Metadata of the first registry carrying this dataset, used for the size
    /// and compression columns.
    pub fn info(&self) -> Option<&BagInfo> {
        self.remotes.iter().find_map(|(_, info)| info.as_ref())
    }

    pub fn primary_registry(&self) -> Option<&str> {
        self.remotes.first().map(|(name, _)| name.as_str())
    }
}

#[derive(Debug, Clone)]
pub enum FieldKind {
    Text,
    Toggle,
    Choice(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct Field {
    pub label: String,
    pub kind: FieldKind,
    pub text: String,
    pub toggle: bool,
    pub choice: usize,
}

impl Field {
    pub fn text(label: &str, value: impl Into<String>) -> Self {
        Self {
            label: label.to_string(),
            kind: FieldKind::Text,
            text: value.into(),
            toggle: false,
            choice: 0,
        }
    }

    pub fn toggle(label: &str, value: bool) -> Self {
        Self {
            label: label.to_string(),
            kind: FieldKind::Toggle,
            text: String::new(),
            toggle: value,
            choice: 0,
        }
    }

    pub fn choice(label: &str, options: &[&str], selected: usize) -> Self {
        Self {
            label: label.to_string(),
            kind: FieldKind::Choice(options.iter().map(|o| o.to_string()).collect()),
            text: String::new(),
            toggle: false,
            choice: selected,
        }
    }

    pub fn value_display(&self) -> String {
        match &self.kind {
            FieldKind::Text => self.text.clone(),
            FieldKind::Toggle => if self.toggle { "yes" } else { "no" }.to_string(),
            FieldKind::Choice(options) => options
                .get(self.choice)
                .cloned()
                .unwrap_or_else(|| "-".to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FormAction {
    Pull { target: String },
    Push { bag: BagRef },
    Import,
    Export { bag: BagRef },
    MirrorCache,
    MirrorRegistry { source: String },
    RegistryAdd,
    RemoveRemote { bag: BagRef },
}

#[derive(Debug, Clone)]
pub struct Form {
    pub title: String,
    pub fields: Vec<Field>,
    pub selected: usize,
    pub action: FormAction,
}

impl Form {
    fn text_at(&self, index: usize) -> String {
        self.fields
            .get(index)
            .map(|f| f.text.trim().to_string())
            .unwrap_or_default()
    }

    fn optional_at(&self, index: usize) -> Option<String> {
        let value = self.text_at(index);
        if value.is_empty() { None } else { Some(value) }
    }

    fn toggle_at(&self, index: usize) -> bool {
        self.fields.get(index).map(|f| f.toggle).unwrap_or(false)
    }

    fn choice_at(&self, index: usize) -> usize {
        self.fields.get(index).map(|f| f.choice).unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub enum ConfirmAction {
    RemoveLocal(BagRef),
    /// `alternate` in [`App::run_confirm`] means "also drop the registries".
    Clean,
    /// `alternate` means "also delete the registry's local data".
    RegistryRemove {
        name: String,
    },
    Pull {
        target: String,
        registry: Option<String>,
    },
}

pub enum Modal {
    Form(Form),
    Confirm {
        title: String,
        body: Vec<String>,
        action: ConfirmAction,
    },
    JobLog {
        id: JobId,
        /// Lines held back from the bottom. Zero follows the tail.
        scroll: usize,
    },
    /// Every job this session ran, newest first.
    JobHistory {
        selected: usize,
    },
    Help,
}

pub struct App {
    pub marina: Marina,
    pub settings: Settings,
    pub compression: CompressionConfig,

    pub screen: Screen,
    pub focus: Focus,
    pub should_quit: bool,
    pub status: String,
    /// Printed to stdout after the terminal is restored, so `cd $(marina)` works.
    pub deferred_stdout: Option<String>,

    pub rows: Vec<DatasetRow>,
    pub visible: Vec<VisibleRow>,
    pub table_state: TableState,
    /// Rows the dataset table last had room for, so a page key moves a screen.
    pub viewport_rows: usize,
    pub filter: String,
    pub filter_editing: bool,
    pub scope: Scope,
    pub hide_duplicates: bool,

    pub remote: BTreeMap<String, Vec<(BagRef, Option<BagInfo>)>>,
    pub registry_state: BTreeMap<String, RemoteState>,
    pub registry_state_index: usize,

    /// Files of the selected dataset, read straight from the cache directory.
    pub files: Vec<CachedFile>,
    /// Files beyond the ones listed.
    pub files_truncated: usize,
    files_key: Option<String>,

    pub jobs: HashMap<JobId, JobState>,
    pub job_order: Vec<JobId>,
    pub job_selected: usize,
    queue: VecDeque<(JobId, JobKind)>,
    active_transfers: usize,
    next_job_id: JobId,
    tx: UnboundedSender<JobEvent>,

    pub settings_selected: usize,
    pub modal: Option<Modal>,
    pub tick: usize,
    /// Kept alive for as long as the app runs: on X11/Wayland the clipboard
    /// contents vanish as soon as the owning `Clipboard` handle is dropped.
    clipboard: Option<arboard::Clipboard>,
}

impl App {
    pub fn new(tx: UnboundedSender<JobEvent>) -> anyhow::Result<Self> {
        let marina = Marina::load()?;
        let file = config::load_registries()?;
        let mut app = Self {
            marina,
            settings: file.settings,
            compression: file.compression,
            screen: Screen::Datasets,
            focus: Focus::Main,
            should_quit: false,
            status: "loading registries".to_string(),
            deferred_stdout: None,
            rows: Vec::new(),
            visible: Vec::new(),
            table_state: TableState::default(),
            viewport_rows: 10,
            filter: String::new(),
            filter_editing: false,
            scope: Scope::default(),
            hide_duplicates: false,
            remote: BTreeMap::new(),
            registry_state: BTreeMap::new(),
            registry_state_index: 0,
            files: Vec::new(),
            files_truncated: 0,
            files_key: None,
            jobs: HashMap::new(),
            job_order: Vec::new(),
            job_selected: 0,
            queue: VecDeque::new(),
            active_transfers: 0,
            next_job_id: 1,
            tx,
            settings_selected: 0,
            modal: None,
            tick: 0,
            clipboard: None,
        };
        app.rebuild_rows();
        app.refresh_all_registries();
        Ok(app)
    }

    // ---------------------------------------------------------------- jobs

    pub fn spawn_job(&mut self, kind: JobKind) -> JobId {
        let id = self.next_job_id;
        self.next_job_id += 1;

        let throttled = kind.is_transfer() && self.active_transfers >= MAX_ACTIVE_TRANSFERS;
        if let JobTarget::Registry(name) = kind.target()
            && matches!(kind, JobKind::ListRemote { .. } | JobKind::Search { .. })
        {
            self.registry_state.insert(name, RemoteState::Loading);
        }

        self.jobs.insert(
            id,
            JobState {
                kind: kind.clone(),
                started: Instant::now(),
                finished: None,
                status: JobStatus::Running,
                last_line: if throttled {
                    "queued".to_string()
                } else {
                    "starting".to_string()
                },
                log: Vec::new(),
            },
        );
        self.job_order.push(id);

        if throttled {
            self.queue.push_back((id, kind));
        } else {
            if kind.is_transfer() {
                self.active_transfers += 1;
            }
            jobs::spawn(id, kind, self.tx.clone());
        }
        id
    }

    fn start_queued(&mut self) {
        while self.active_transfers < MAX_ACTIVE_TRANSFERS
            && let Some((id, kind)) = self.queue.pop_front()
        {
            self.active_transfers += 1;
            if let Some(job) = self.jobs.get_mut(&id) {
                job.started = Instant::now();
                job.last_line = "starting".to_string();
            }
            jobs::spawn(id, kind, self.tx.clone());
        }
    }

    pub fn on_job_event(&mut self, event: JobEvent) {
        match event {
            JobEvent::Progress { id, phase, message } => {
                if let Some(job) = self.jobs.get_mut(&id) {
                    let line = format!("[{}] {}", phase, message);
                    job.last_line = line.clone();
                    job.push_log(line);
                }
            }
            JobEvent::Done { id, outcome } => {
                let detached = self
                    .jobs
                    .get(&id)
                    .is_some_and(|job| job.status == JobStatus::Detached);
                let summary = outcome_summary(&outcome);
                if let Some(job) = self.jobs.get_mut(&id) {
                    job.push_log(summary.clone());
                    job.last_line = summary;
                }
                self.finish_job(id, JobStatus::Done);
                if !detached {
                    self.apply_outcome(*outcome);
                }
            }
            JobEvent::Failed { id, error } => {
                let kind = self.jobs.get(&id).map(|job| job.kind.clone());
                if let Some(JobKind::ListRemote { registry } | JobKind::Search { registry, .. }) =
                    kind
                {
                    self.registry_state
                        .insert(registry, RemoteState::Failed(error.clone()));
                }
                if let Some(job) = self.jobs.get_mut(&id) {
                    job.push_log(error.clone());
                    job.last_line = error.clone();
                }
                let detached = self
                    .jobs
                    .get(&id)
                    .is_some_and(|job| job.status == JobStatus::Detached);
                self.finish_job(id, JobStatus::Failed);
                if !detached {
                    self.status = error;
                }
            }
        }
    }

    fn finish_job(&mut self, id: JobId, status: JobStatus) {
        let mut was_transfer = false;
        if let Some(job) = self.jobs.get_mut(&id) {
            was_transfer = job.kind.is_transfer();
            job.finished = Some(Instant::now());
            if job.status != JobStatus::Detached {
                job.status = status;
            }
        }
        if was_transfer {
            self.active_transfers = self.active_transfers.saturating_sub(1);
        }
        // The pane lists live jobs only, so finishing one moves the cursor.
        self.clamp_job_selection();
        self.start_queued();
    }

    fn apply_outcome(&mut self, outcome: JobOutcome) {
        match outcome {
            JobOutcome::RemoteListing { registry, rows } => {
                self.registry_state
                    .insert(registry.clone(), RemoteState::Loaded(rows.len()));
                self.remote.insert(registry, rows);
                self.rebuild_rows();
            }
            JobOutcome::Resolved { target, path } => {
                self.status = match self.copy_to_clipboard(&path.display().to_string()) {
                    Ok(()) => format!("{target} -> {} (copied to clipboard)", path.display()),
                    Err(err) => format!("failed to copy {} to clipboard: {err}", path.display()),
                };
            }
            JobOutcome::Message { text, refresh } => {
                self.status = text;
                self.apply_refresh(refresh);
            }
        }
    }

    fn apply_refresh(&mut self, refresh: Refresh) {
        if refresh.local {
            // A pull or import changed what is on disk under the selection.
            self.files_key = None;
        }
        if refresh.local || refresh.registries {
            if let Ok(marina) = Marina::load() {
                self.marina = marina;
            }
            if let Ok(file) = config::load_registries() {
                self.settings = file.settings;
                self.compression = file.compression;
            }
            self.rebuild_rows();
        }
        if refresh.registries {
            let known: Vec<String> = self
                .marina
                .list_registry_configs()
                .iter()
                .map(|cfg| cfg.name.clone())
                .collect();
            self.remote.retain(|name, _| known.contains(name));
            self.registry_state.retain(|name, _| known.contains(name));
        }
        if refresh.remotes {
            self.refresh_all_registries();
        }
    }

    pub fn refresh_all_registries(&mut self) {
        let names: Vec<String> = self
            .marina
            .list_registry_configs()
            .iter()
            .map(|cfg| cfg.name.clone())
            .collect();
        if names.is_empty() {
            self.status = "no registries configured — press 2 then a to add one".to_string();
            return;
        }
        for name in names {
            self.spawn_job(JobKind::ListRemote { registry: name });
        }
        self.status = "refreshing registries".to_string();
    }

    /// True while any job is running for this target, driving the row spinners.
    pub fn is_busy(&self, target: &JobTarget) -> bool {
        self.jobs
            .values()
            .any(|job| job.status == JobStatus::Running && &job.kind.target() == target)
    }

    /// Jobs still doing something, oldest first. The pane shows only these: a
    /// finished command is history, and history belongs behind a key.
    pub fn active_jobs(&self) -> Vec<JobId> {
        self.job_order
            .iter()
            .copied()
            .filter(|id| {
                self.jobs.get(id).is_some_and(|job| {
                    matches!(job.status, JobStatus::Running | JobStatus::Detached)
                })
            })
            .collect()
    }

    /// Id under the jobs-pane cursor.
    pub fn selected_job(&self) -> Option<JobId> {
        self.active_jobs().get(self.job_selected).copied()
    }

    pub fn detach_selected_job(&mut self) {
        let Some(id) = self.selected_job() else {
            return;
        };
        let Some(job) = self.jobs.get_mut(&id) else {
            return;
        };
        if job.status == JobStatus::Running {
            job.status = JobStatus::Detached;
            job.last_line = "detached (the transfer keeps running in the background)".to_string();
            self.status = format!("detached {}", job.kind.label());
        } else {
            // Already detached: stop listing it entirely.
            self.jobs.remove(&id);
            self.job_order.retain(|other| *other != id);
        }
        self.clamp_job_selection();
    }

    fn clamp_job_selection(&mut self) {
        let count = self.active_jobs().len();
        self.job_selected = self.job_selected.min(count.saturating_sub(1));
        if count == 0 && self.focus == Focus::Jobs {
            self.focus = Focus::Main;
        }
    }

    /// Drops every finished job from the history.
    fn clear_finished_jobs(&mut self) {
        let live = self.active_jobs();
        self.job_order.retain(|id| live.contains(id));
        self.jobs.retain(|id, _| live.contains(id));
        self.status = "cleared finished jobs".to_string();
    }

    // ---------------------------------------------------------------- rows

    /// Merges the local cache with every registry listing received so far.
    pub fn rebuild_rows(&mut self) {
        let mut by_key: BTreeMap<String, DatasetRow> = BTreeMap::new();

        for cached in self.marina.list_cached_bags() {
            let key = cached.bag.to_string();
            by_key.insert(
                key.clone(),
                DatasetRow {
                    namespace: cached.bag.namespace.clone(),
                    base_name: cached.bag.name.clone(),
                    display_name: display_name(&cached.bag),
                    bag: cached.bag,
                    key,
                    local_dir: Some(cached.local_dir),
                    local_bytes: cached.original_bytes,
                    remotes: Vec::new(),
                },
            );
        }

        for (registry, rows) in &self.remote {
            for (bag, info) in rows {
                let key = bag.to_string();
                let entry = by_key.entry(key.clone()).or_insert_with(|| DatasetRow {
                    namespace: bag.namespace.clone(),
                    base_name: bag.name.clone(),
                    display_name: display_name(bag),
                    bag: bag.clone(),
                    key,
                    local_dir: None,
                    local_bytes: 0,
                    remotes: Vec::new(),
                });
                entry.remotes.push((registry.clone(), info.clone()));
            }
        }

        let mut rows: Vec<DatasetRow> = by_key.into_values().collect();
        if self.hide_duplicates {
            hide_duplicate_registries(&mut rows);
        }
        // Same order as `marina list --remote`: namespace, then name, then the
        // full reference.
        rows.sort_by(|a, b| {
            a.namespace
                .as_deref()
                .unwrap_or("")
                .cmp(b.namespace.as_deref().unwrap_or(""))
                .then_with(|| a.base_name.cmp(&b.base_name))
                .then_with(|| a.key.cmp(&b.key))
        });
        self.rows = rows;
        self.apply_filter();
    }

    pub fn apply_filter(&mut self) {
        let previous = self.selected_row().map(|row| row.key.clone());
        let pattern = self.filter.trim().to_lowercase();
        let matching: Vec<usize> = self
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| self.scope.accepts(row) && matches_filter(row, &pattern))
            .map(|(index, _)| index)
            .collect();
        self.visible = group_rows(&self.rows, &matching);

        let restored = previous.and_then(|key| {
            self.visible.iter().position(|entry| {
                matches!(entry, VisibleRow::Dataset { index, .. } if self.rows[*index].key == key)
            })
        });
        let selection = restored.or_else(|| self.first_selectable(0));
        self.table_state.select(selection);
    }

    /// Dataset rows currently listed, ignoring the headings and spacers.
    pub fn dataset_count(&self) -> usize {
        self.visible
            .iter()
            .filter(|entry| matches!(entry, VisibleRow::Dataset { .. }))
            .count()
    }

    pub fn selected_row(&self) -> Option<&DatasetRow> {
        let position = self.table_state.selected()?;
        match self.visible.get(position)? {
            VisibleRow::Dataset { index, .. } => self.rows.get(*index),
            _ => None,
        }
    }

    /// First dataset row at or after `from`, skipping the group headings.
    fn first_selectable(&self, from: usize) -> Option<usize> {
        first_selectable(&self.visible, from)
    }

    /// Moves by `delta` dataset rows, stepping over headings and spacers so the
    /// grouped list navigates like a flat one.
    fn move_selection(&mut self, delta: isize) {
        if let Some(position) = step_selection(&self.visible, self.table_state.selected(), delta) {
            self.table_state.select(Some(position));
        }
    }

    /// Moves a screen at a time, the way `Ctrl-f` and `Ctrl-b` do in Helix.
    /// `fraction` is 1 for a full page and 2 for a half one.
    fn page_selection(&mut self, down: bool, fraction: usize) {
        let lines = (self.viewport_rows / fraction).max(1) as isize;
        let lines = if down { lines } else { -lines };
        if let Some(position) = page_selection(&self.visible, self.table_state.selected(), lines) {
            self.table_state.select(Some(position));
        }
    }

    /// Jumps to the first or last dataset row.
    fn select_edge(&mut self, first: bool) {
        if let Some(position) = edge_selection(&self.visible, first) {
            self.table_state.select(Some(position));
        }
    }

    fn move_job_selection(&mut self, delta: isize) {
        let count = self.active_jobs().len();
        if count == 0 {
            return;
        }
        let last = count as isize - 1;
        self.job_selected = (self.job_selected as isize + delta).clamp(0, last) as usize;
    }

    fn move_registry_selection(&mut self, delta: isize) {
        let count = self.marina.list_registry_configs().len();
        if count == 0 {
            return;
        }
        let last = count as isize - 1;
        self.registry_state_index =
            (self.registry_state_index as isize + delta).clamp(0, last) as usize;
    }

    pub fn selected_registry(&self) -> Option<RegistryConfig> {
        self.marina
            .list_registry_configs()
            .get(self.registry_state_index)
            .map(|cfg| (*cfg).clone())
    }

    pub fn on_tick(&mut self) {
        self.tick = self.tick.wrapping_add(1);
        // Cheap when the selection has not moved, and it catches a pull that
        // just filled the cache directory.
        self.sync_files();
    }

    // -------------------------------------------------------------- input

    pub fn on_key(&mut self, key: KeyEvent) {
        if self.modal.is_some() {
            self.modal_key(key);
            return;
        }
        if self.filter_editing {
            self.filter_key(key);
            return;
        }

        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Char('c') if ctrl => self.should_quit = true,
            // Helix paging.
            KeyCode::Char('f') if ctrl => self.page_selection(true, 1),
            KeyCode::Char('b') if ctrl => self.page_selection(false, 1),
            KeyCode::Char('d') if ctrl => self.page_selection(true, 2),
            KeyCode::Char('u') if ctrl => self.page_selection(false, 2),
            KeyCode::Char('?') => self.modal = Some(Modal::Help),
            KeyCode::Char('1') => self.screen = Screen::Datasets,
            KeyCode::Char('2') => self.screen = Screen::Registries,
            KeyCode::Char('3') => self.screen = Screen::Settings,
            KeyCode::Tab => {
                self.focus = match self.focus {
                    // Nothing running means there is no pane to move into.
                    Focus::Main if self.active_jobs().is_empty() => Focus::Main,
                    Focus::Main => Focus::Jobs,
                    Focus::Jobs => Focus::Main,
                };
                self.clamp_job_selection();
            }
            KeyCode::Char('J') => {
                self.modal = Some(Modal::JobHistory { selected: 0 });
            }
            KeyCode::Char('r') => self.refresh_all_registries(),
            _ if self.focus == Focus::Jobs => self.jobs_key(key),
            _ => match self.screen {
                Screen::Datasets => self.datasets_key(key),
                Screen::Registries => self.registries_key(key),
                Screen::Settings => self.settings_key(key),
            },
        }
        self.sync_files();
    }

    fn jobs_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.move_job_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_job_selection(-1),
            KeyCode::Enter => {
                if let Some(id) = self.selected_job() {
                    self.modal = Some(Modal::JobLog { id, scroll: 0 });
                }
            }
            KeyCode::Char('x') | KeyCode::Delete => self.detach_selected_job(),
            _ => {}
        }
    }

    fn filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.filter.clear();
                self.filter_editing = false;
                self.apply_filter();
            }
            KeyCode::Enter => self.filter_editing = false,
            KeyCode::Backspace => {
                self.filter.pop();
                self.apply_filter();
            }
            KeyCode::Char(c) => {
                self.filter.push(c);
                self.apply_filter();
            }
            _ => {}
        }
    }

    fn datasets_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::PageDown => self.page_selection(true, 1),
            KeyCode::PageUp => self.page_selection(false, 1),
            KeyCode::Home | KeyCode::Char('g') => self.select_edge(true),
            KeyCode::End | KeyCode::Char('G') => self.select_edge(false),
            KeyCode::Char('/') => {
                self.filter_editing = true;
                self.filter.clear();
            }
            KeyCode::Char('l') => {
                self.scope = self.scope.next();
                self.apply_filter();
                self.status = match self.scope {
                    Scope::All => "listing the cache and every registry".to_string(),
                    Scope::Local => "listing the local cache only".to_string(),
                    Scope::Remote => "listing registries only".to_string(),
                };
            }
            KeyCode::Char('d') => {
                self.hide_duplicates = !self.hide_duplicates;
                self.rebuild_rows();
                self.status = format!(
                    "duplicate registries {}",
                    if self.hide_duplicates {
                        "hidden"
                    } else {
                        "shown"
                    }
                );
            }
            KeyCode::Char('s') => self.search_remote(),
            KeyCode::Enter => self.copy_selected_identifier(),
            KeyCode::Char('o') => self.resolve_selected(),
            KeyCode::Char('p') => self.pull_form(),
            KeyCode::Char('P') => self.push_form(),
            KeyCode::Char('I') => self.import_form(),
            KeyCode::Char('e') => self.export_form(),
            KeyCode::Char('x') | KeyCode::Delete => self.remove_selected(),
            KeyCode::Char('M') => self.mirror_cache_form(),
            KeyCode::Char('C') => {
                self.modal = Some(Modal::Confirm {
                    title: "Clean the local cache?".to_string(),
                    body: vec![
                        "Removes cached datasets. Press y to confirm, a to also drop registries."
                            .to_string(),
                    ],
                    action: ConfirmAction::Clean,
                });
            }
            _ => {}
        }
    }

    fn registries_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.move_registry_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_registry_selection(-1),
            KeyCode::Char('a') => self.registry_add_form(),
            KeyCode::Char('D') | KeyCode::Delete => {
                if let Some(cfg) = self.selected_registry() {
                    self.modal = Some(Modal::Confirm {
                        title: format!("Remove registry '{}'?", cfg.name),
                        body: vec![
                            format!("{} {}", cfg.kind, cfg.uri),
                            "y removes the entry, d also deletes local registry data.".to_string(),
                        ],
                        action: ConfirmAction::RegistryRemove { name: cfg.name },
                    });
                }
            }
            KeyCode::Char('A') => {
                if let Some(cfg) = self.selected_registry() {
                    self.spawn_job(JobKind::RegistryAuth { name: cfg.name });
                }
            }
            KeyCode::Char('m') => {
                if let Some(cfg) = self.selected_registry() {
                    self.mirror_registry_form(cfg.name);
                }
            }
            KeyCode::Char('R') => {
                if let Some(cfg) = self.selected_registry() {
                    self.spawn_job(JobKind::ListRemote { registry: cfg.name });
                }
            }
            KeyCode::Enter => self.set_default_registry(),
            _ => {}
        }
    }

    fn settings_key(&mut self, key: KeyEvent) {
        const FIELDS: usize = 6;
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                self.settings_selected = (self.settings_selected + 1).min(FIELDS - 1);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.settings_selected = self.settings_selected.saturating_sub(1);
            }
            KeyCode::Left => self.adjust_setting(-1),
            KeyCode::Right | KeyCode::Enter => self.adjust_setting(1),
            KeyCode::Char('w') => self.save_settings(),
            _ => {}
        }
    }

    fn adjust_setting(&mut self, delta: i64) {
        match self.settings_selected {
            0 => {
                self.settings.time_display = match self.settings.time_display {
                    TimeDisplay::Relative => TimeDisplay::Absolute,
                    TimeDisplay::Absolute => TimeDisplay::Relative,
                };
            }
            1 => {
                let names: Vec<String> = self
                    .marina
                    .list_registry_configs()
                    .iter()
                    .map(|cfg| cfg.name.clone())
                    .collect();
                self.settings.default_registry =
                    cycle_option(self.settings.default_registry.clone(), &names, delta);
            }
            2 => {
                self.settings.registry_timeout_secs =
                    step_u64(self.settings.registry_timeout_secs, delta, 1, 600);
            }
            3 => {
                self.settings.completion_cache_ttl_secs = step_u64(
                    self.settings.completion_cache_ttl_secs,
                    delta * 60,
                    0,
                    86400,
                );
            }
            4 => {
                self.compression.pointcloud_mode = match self.compression.pointcloud_mode {
                    ConfigPointcloudMode::Off => ConfigPointcloudMode::Lossy,
                    ConfigPointcloudMode::Lossy => ConfigPointcloudMode::Lossless,
                    ConfigPointcloudMode::Lossless => ConfigPointcloudMode::Off,
                };
            }
            5 => {
                self.compression.packed_mcap_compression =
                    match self.compression.packed_mcap_compression {
                        ConfigMcapCompression::None => ConfigMcapCompression::Zstd,
                        ConfigMcapCompression::Zstd => ConfigMcapCompression::Lz4,
                        ConfigMcapCompression::Lz4 => ConfigMcapCompression::None,
                    };
            }
            _ => {}
        }
    }

    fn save_settings(&mut self) {
        let result = config::load_registries().and_then(|mut file| {
            file.settings = self.settings.clone();
            file.compression = self.compression;
            config::save_registries(&file)
        });
        match result {
            Ok(()) => {
                self.status = "settings saved".to_string();
                if let Ok(marina) = Marina::load() {
                    self.marina = marina;
                }
            }
            Err(error) => self.status = format!("could not save settings: {error:#}"),
        }
    }

    fn set_default_registry(&mut self) {
        let Some(cfg) = self.selected_registry() else {
            return;
        };
        self.settings.default_registry = Some(cfg.name.clone());
        self.save_settings();
        self.status = format!("default registry: {}", cfg.name);
    }

    // ------------------------------------------------------------- actions

    fn search_remote(&mut self) {
        let pattern = if self.filter.trim().is_empty() {
            "*".to_string()
        } else {
            format!("*{}*", self.filter.trim())
        };
        let names: Vec<String> = self
            .marina
            .list_registry_configs()
            .iter()
            .map(|cfg| cfg.name.clone())
            .collect();
        for registry in names {
            self.spawn_job(JobKind::Search {
                registry,
                pattern: pattern.clone(),
            });
        }
        self.status = format!("searching registries for {pattern}");
    }

    /// Reads the selected dataset's files from disk when the selection moves.
    ///
    /// This is a cache directory walk, not a registry call: `marina inspect`
    /// would add a network round trip per registry to tell us what the listing
    /// already said, and nothing at all for a dataset that is not cached.
    pub fn sync_files(&mut self) {
        let Some(row) = self.selected_row() else {
            self.files.clear();
            self.files_truncated = 0;
            self.files_key = None;
            return;
        };
        if self.files_key.as_deref() == Some(row.key.as_str()) {
            return;
        }

        let key = row.key.clone();
        let listing = row.local_dir.as_deref().map(list_cached_files);
        let (files, truncated) = listing.unwrap_or_default();
        self.files = files;
        self.files_truncated = truncated;
        self.files_key = Some(key);
    }

    fn copy_to_clipboard(&mut self, text: &str) -> anyhow::Result<()> {
        if self.clipboard.is_none() {
            self.clipboard = Some(arboard::Clipboard::new()?);
        }
        self.clipboard
            .as_mut()
            .expect("just initialized")
            .set_text(text)?;
        Ok(())
    }

    /// Copies the selected dataset's full identifier, e.g. for pasting into
    /// `marina pull <target>` elsewhere.
    fn copy_selected_identifier(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let target = row.key.clone();
        self.status = match self.copy_to_clipboard(&target) {
            Ok(()) => format!("copied {target} to clipboard"),
            Err(err) => format!("failed to copy {target} to clipboard: {err}"),
        };
    }

    /// Resolves the selected dataset to its local path and copies that path
    /// to the clipboard once the job completes, without quitting the TUI.
    fn resolve_selected(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let target = row.key.clone();
        let registry = row.primary_registry().map(|name| name.to_string());
        if row.is_cached() {
            self.spawn_job(JobKind::Resolve { target, registry });
        } else {
            self.modal = Some(Modal::Confirm {
                title: format!("{target} is not cached"),
                body: vec!["Pull it now? Press y to start the transfer.".to_string()],
                action: ConfirmAction::Pull { target, registry },
            });
        }
    }

    fn remove_selected(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let bag = row.bag.clone();
        if row.is_cached() {
            self.modal = Some(Modal::Confirm {
                title: format!("Remove {bag} from the local cache?"),
                body: vec![
                    row.local_dir
                        .as_ref()
                        .map(|dir| dir.display().to_string())
                        .unwrap_or_default(),
                ],
                action: ConfirmAction::RemoveLocal(bag),
            });
        } else if !row.remotes.is_empty() {
            let form = Form {
                title: format!("Remove {bag} from a registry"),
                fields: vec![
                    Field::choice(
                        "registry",
                        &row.remotes
                            .iter()
                            .map(|(name, _)| name.as_str())
                            .collect::<Vec<_>>(),
                        0,
                    ),
                    Field::toggle("write http index", false),
                ],
                selected: 0,
                action: FormAction::RemoveRemote { bag },
            };
            self.modal = Some(Modal::Form(form));
        }
    }

    fn pull_form(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let target = row.key.clone();
        let registry = row.primary_registry().map(|name| name.to_string());
        let form = Form {
            title: format!("Pull {target}"),
            fields: vec![
                Field::text("registry", registry.clone().unwrap_or_default()),
                Field::choice(
                    "unpacked mcap compression",
                    &["zstd", "lz4", "none"],
                    mcap_choice_index(self.compression.unpacked_mcap_compression),
                ),
                Field::toggle("force", false),
            ],
            selected: 0,
            action: FormAction::Pull { target },
        };
        self.modal = Some(Modal::Form(form));
    }

    fn push_form(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let bag = row.bag.clone();
        let source = row
            .local_dir
            .as_ref()
            .map(|dir| dir.display().to_string())
            .unwrap_or_default();
        let default_registry = self
            .settings
            .default_registry
            .clone()
            .or_else(|| row.primary_registry().map(|name| name.to_string()))
            .unwrap_or_default();
        let form = Form {
            title: format!("Push {bag}"),
            fields: vec![
                Field::text("source", source),
                Field::text("registry", default_registry),
                Field::choice(
                    "pointcloud mode",
                    &["lossy", "lossless", "off"],
                    pointcloud_choice_index(self.compression.pointcloud_mode),
                ),
                Field::text(
                    "pointcloud accuracy mm",
                    format!("{}", self.compression.pointcloud_accuracy_mm),
                ),
                Field::choice(
                    "packed mcap compression",
                    &["zstd", "lz4", "none"],
                    mcap_choice_index(self.compression.packed_mcap_compression),
                ),
                Field::choice(
                    "packed archive compression",
                    &["none", "gzip"],
                    archive_choice_index(self.compression.packed_archive_compression),
                ),
                Field::toggle("write http index", false),
                Field::toggle("copy to cache (keep source)", false),
                Field::toggle("skip db3 vacuum", false),
                Field::toggle("dry run", false),
            ],
            selected: 0,
            action: FormAction::Push { bag },
        };
        self.modal = Some(Modal::Form(form));
    }

    fn import_form(&mut self) {
        let form = Form {
            title: "Import a local recording".to_string(),
            fields: vec![
                Field::text("dataset", String::new()),
                Field::text("path (empty allocates a cache dir)", String::new()),
                Field::toggle("move into the cache", false),
            ],
            selected: 0,
            action: FormAction::Import,
        };
        self.modal = Some(Modal::Form(form));
    }

    fn export_form(&mut self) {
        let Some(row) = self.selected_row() else {
            return;
        };
        let bag = row.bag.clone();
        let form = Form {
            title: format!("Export {bag}"),
            fields: vec![Field::text("output path", String::new())],
            selected: 0,
            action: FormAction::Export { bag },
        };
        self.modal = Some(Modal::Form(form));
    }

    fn mirror_cache_form(&mut self) {
        let form = Form {
            title: "Mirror the local cache over SSH".to_string(),
            fields: vec![
                Field::text("target (user@host[:port])", String::new()),
                Field::text("patterns (space separated)", String::new()),
                Field::text("auth env", String::new()),
                Field::text("proxy jump", String::new()),
                Field::text("ssh transport (openssh|native)", String::new()),
                Field::text("remote marina", String::new()),
            ],
            selected: 0,
            action: FormAction::MirrorCache,
        };
        self.modal = Some(Modal::Form(form));
    }

    fn mirror_registry_form(&mut self, source: String) {
        let targets: Vec<String> = self
            .marina
            .list_registry_configs()
            .iter()
            .map(|cfg| cfg.name.clone())
            .filter(|name| *name != source)
            .collect();
        if targets.is_empty() {
            self.status = "mirroring needs a second registry".to_string();
            return;
        }
        let form = Form {
            title: format!("Mirror {source} into another registry"),
            fields: vec![
                Field::choice(
                    "target",
                    &targets.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
                    0,
                ),
                Field::text("patterns (space separated, empty = all)", String::new()),
            ],
            selected: 0,
            action: FormAction::MirrorRegistry { source },
        };
        self.modal = Some(Modal::Form(form));
    }

    fn registry_add_form(&mut self) {
        let form = Form {
            title: "Add a registry".to_string(),
            fields: vec![
                Field::text("name", String::new()),
                Field::text("uri", String::new()),
                Field::text("kind (empty infers from the uri)", String::new()),
                Field::text("auth env", String::new()),
                Field::text("proxy jump", String::new()),
                Field::text("ssh transport (openssh|native)", String::new()),
            ],
            selected: 0,
            action: FormAction::RegistryAdd,
        };
        self.modal = Some(Modal::Form(form));
    }

    // -------------------------------------------------------------- modals

    fn modal_key(&mut self, key: KeyEvent) {
        match self.modal.as_mut() {
            Some(Modal::Help) => {
                if matches!(key.code, KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q')) {
                    self.modal = None;
                }
            }
            Some(Modal::JobLog { id, scroll }) => {
                let lines = self.jobs.get(id).map(|job| job.log.len()).unwrap_or(0);
                let back = |scroll: &mut usize, by: usize| {
                    *scroll = (*scroll + by).min(lines.saturating_sub(1));
                };
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => back(scroll, 1),
                    KeyCode::Down | KeyCode::Char('j') => *scroll = scroll.saturating_sub(1),
                    KeyCode::PageUp => back(scroll, 10),
                    KeyCode::PageDown => *scroll = scroll.saturating_sub(10),
                    KeyCode::Home => back(scroll, lines),
                    // Back to following the tail.
                    KeyCode::End => *scroll = 0,
                    KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => self.modal = None,
                    _ => {}
                }
            }
            Some(Modal::JobHistory { selected }) => {
                let last = self.job_order.len().saturating_sub(1);
                match key.code {
                    KeyCode::Down | KeyCode::Char('j') => *selected = (*selected + 1).min(last),
                    KeyCode::Up | KeyCode::Char('k') => *selected = selected.saturating_sub(1),
                    KeyCode::Enter => {
                        // The history lists newest first.
                        let position = self.job_order.len().saturating_sub(1 + *selected);
                        if let Some(id) = self.job_order.get(position).copied() {
                            self.modal = Some(Modal::JobLog { id, scroll: 0 });
                        }
                    }
                    KeyCode::Char('C') => {
                        self.clear_finished_jobs();
                        self.modal = Some(Modal::JobHistory { selected: 0 });
                    }
                    KeyCode::Esc | KeyCode::Char('q') => self.modal = None,
                    _ => {}
                }
            }
            Some(Modal::Confirm { action, .. }) => {
                let action = action.clone();
                match key.code {
                    KeyCode::Char('y') | KeyCode::Enter => {
                        self.modal = None;
                        self.run_confirm(action, false);
                    }
                    KeyCode::Char('a') if matches!(action, ConfirmAction::Clean) => {
                        self.modal = None;
                        self.run_confirm(action, true);
                    }
                    KeyCode::Char('d')
                        if matches!(action, ConfirmAction::RegistryRemove { .. }) =>
                    {
                        self.modal = None;
                        self.run_confirm(action, true);
                    }
                    KeyCode::Esc | KeyCode::Char('n') => self.modal = None,
                    _ => {}
                }
            }
            Some(Modal::Form(form)) => match key.code {
                KeyCode::Esc => self.modal = None,
                KeyCode::Down | KeyCode::Tab => {
                    form.selected = (form.selected + 1) % form.fields.len();
                }
                KeyCode::Up | KeyCode::BackTab => {
                    form.selected = form
                        .selected
                        .checked_sub(1)
                        .unwrap_or(form.fields.len() - 1);
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Char(' ') => {
                    let forward = !matches!(key.code, KeyCode::Left);
                    if let Some(field) = form.fields.get_mut(form.selected) {
                        match &field.kind {
                            FieldKind::Toggle => field.toggle = !field.toggle,
                            FieldKind::Choice(options) => {
                                let count = options.len().max(1);
                                field.choice = if forward {
                                    (field.choice + 1) % count
                                } else {
                                    (field.choice + count - 1) % count
                                };
                            }
                            FieldKind::Text => {
                                if key.code == KeyCode::Char(' ') {
                                    field.text.push(' ');
                                }
                            }
                        }
                    }
                }
                KeyCode::Backspace => {
                    if let Some(field) = form.fields.get_mut(form.selected)
                        && matches!(field.kind, FieldKind::Text)
                    {
                        field.text.pop();
                    }
                }
                KeyCode::Char(c) => {
                    if let Some(field) = form.fields.get_mut(form.selected)
                        && matches!(field.kind, FieldKind::Text)
                    {
                        field.text.push(c);
                    }
                }
                KeyCode::Enter => {
                    let form = form.clone();
                    self.modal = None;
                    self.submit_form(form);
                }
                _ => {}
            },
            None => {}
        }
    }

    fn run_confirm(&mut self, action: ConfirmAction, alternate: bool) {
        match action {
            ConfirmAction::RemoveLocal(bag) => {
                self.spawn_job(JobKind::RemoveLocal { bag });
            }
            ConfirmAction::Clean => {
                self.spawn_job(JobKind::Clean { all: alternate });
            }
            ConfirmAction::RegistryRemove { name } => {
                self.spawn_job(JobKind::RegistryRemove {
                    name,
                    delete_data: alternate,
                });
            }
            ConfirmAction::Pull { target, registry } => {
                let options = PullOptions {
                    unpacked_mcap_compression: mcap_to_core(
                        self.compression.unpacked_mcap_compression,
                    ),
                    force: false,
                };
                self.spawn_job(JobKind::Pull {
                    target,
                    registry,
                    options,
                });
            }
        }
    }

    fn submit_form(&mut self, form: Form) {
        match form.action.clone() {
            FormAction::Pull { target } => {
                let registry = form.optional_at(0);
                let options = PullOptions {
                    unpacked_mcap_compression: mcap_from_choice(form.choice_at(1)),
                    force: form.toggle_at(2),
                };
                self.spawn_job(JobKind::Pull {
                    target,
                    registry,
                    options,
                });
            }
            FormAction::Push { bag } => {
                let source = form.text_at(0);
                if source.is_empty() {
                    self.status = "push needs a source path".to_string();
                    return;
                }
                let accuracy: f64 = form
                    .text_at(3)
                    .parse()
                    .unwrap_or(self.compression.pointcloud_accuracy_mm);
                let options = PushOptions {
                    pointcloud_mode: pointcloud_from_choice(form.choice_at(2)),
                    pointcloud_precision_m: accuracy / 1000.0,
                    packed_mcap_compression: mcap_from_choice(form.choice_at(4)),
                    packed_archive_compression: archive_from_choice(form.choice_at(5)),
                    write_http_index: form.toggle_at(6),
                    db3_vacuum: !form.toggle_at(8),
                    dry_run: form.toggle_at(9),
                    move_source_to_cache: !form.toggle_at(7),
                };
                self.spawn_job(JobKind::Push {
                    bag,
                    source: PathBuf::from(source),
                    registry: form.optional_at(1),
                    options: Box::new(options),
                });
            }
            FormAction::Import => {
                let dataset = form.text_at(0);
                match dataset.parse::<BagRef>() {
                    Ok(bag) => {
                        self.spawn_job(JobKind::Import {
                            bag,
                            path: form.optional_at(1).map(PathBuf::from),
                            move_to_cache: form.toggle_at(2),
                        });
                    }
                    Err(error) => self.status = format!("invalid dataset reference: {error}"),
                }
            }
            FormAction::Export { bag } => {
                let output = form.text_at(0);
                if output.is_empty() {
                    self.status = "export needs an output path".to_string();
                    return;
                }
                self.spawn_job(JobKind::Export {
                    bag,
                    output: PathBuf::from(output),
                });
            }
            FormAction::MirrorCache => {
                let target = form.text_at(0);
                if target.is_empty() {
                    self.status = "mirror needs an SSH target".to_string();
                    return;
                }
                let options = CacheMirrorOptions {
                    auth_env: form.optional_at(2),
                    proxy_jump: form.optional_at(3),
                    ssh_transport: form.optional_at(4),
                    remote_marina: form.optional_at(5),
                };
                self.spawn_job(JobKind::MirrorCacheSsh {
                    target,
                    patterns: split_patterns(&form.text_at(1)),
                    options: Box::new(options),
                });
            }
            FormAction::MirrorRegistry { source } => {
                let target = form
                    .fields
                    .first()
                    .map(|field| field.value_display())
                    .unwrap_or_default();
                self.spawn_job(JobKind::MirrorRegistry {
                    source,
                    target,
                    patterns: split_patterns(&form.text_at(1)),
                });
            }
            FormAction::RegistryAdd => {
                let name = form.text_at(0);
                let uri = form.text_at(1);
                if name.is_empty() || uri.is_empty() {
                    self.status = "a registry needs a name and a uri".to_string();
                    return;
                }
                let kind = form
                    .optional_at(2)
                    .unwrap_or_else(|| config::infer_kind_from_uri(&uri).to_string());
                let uri = absolutize_folder_uri(&kind, uri);
                self.spawn_job(JobKind::RegistryAdd(Box::new(RegistryConfig {
                    name,
                    kind,
                    uri,
                    auth_env: form.optional_at(3),
                    proxy_jump: form.optional_at(4),
                    ssh_transport: form.optional_at(5),
                    download_mode: RegistryDownloadMode::Adaptive,
                })));
            }
            FormAction::RemoveRemote { bag } => {
                let registry = form
                    .fields
                    .first()
                    .map(|field| field.value_display())
                    .unwrap_or_default();
                self.spawn_job(JobKind::RemoveRemote {
                    bag,
                    registry,
                    write_http_index: form.toggle_at(1),
                });
            }
        }
    }
}

fn display_name(bag: &BagRef) -> String {
    let full = bag.to_string();
    match &bag.namespace {
        Some(namespace) => full
            .strip_prefix(&format!("{namespace}/"))
            .unwrap_or(&full)
            .to_string(),
        None => full,
    }
}

fn matches_filter(row: &DatasetRow, pattern: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let haystack = row.key.to_lowercase();
    if let Ok(glob) = glob::Pattern::new(pattern)
        && pattern.contains(['*', '?'])
    {
        return glob.matches(&haystack);
    }
    haystack.contains(pattern)
        || row
            .remotes
            .iter()
            .any(|(name, _)| name.to_lowercase().contains(pattern))
}

/// Lays the matching rows out the way `marina list --remote` prints them:
/// a `namespace/` heading per section, a heading for every base name that has
/// more than one tag variant, and blank lines between the groups.
///
/// `matching` must be indices into `rows` in the sorted order `rebuild_rows`
/// established.
fn group_rows(rows: &[DatasetRow], matching: &[usize]) -> Vec<VisibleRow> {
    let mut group_sizes: BTreeMap<(Option<&str>, &str), usize> = BTreeMap::new();
    for index in matching {
        let row = &rows[*index];
        *group_sizes
            .entry((row.namespace.as_deref(), row.base_name.as_str()))
            .or_insert(0) += 1;
    }

    let mut out: Vec<VisibleRow> = Vec::with_capacity(matching.len());
    let mut previous_namespace: Option<Option<&str>> = None;
    let mut previous_base: Option<&str> = None;
    let mut previous_grouped = false;

    for index in matching {
        let row = &rows[*index];
        let namespace = row.namespace.as_deref();
        let grouped = group_sizes
            .get(&(namespace, row.base_name.as_str()))
            .copied()
            .unwrap_or(0)
            > 1;

        let namespace_changed = previous_namespace != Some(namespace);
        let base_changed = previous_base != Some(row.base_name.as_str());

        if namespace_changed {
            if previous_namespace.is_some() {
                out.push(VisibleRow::Spacer);
            }
            if let Some(namespace) = namespace {
                out.push(VisibleRow::Namespace(namespace.to_string()));
            }
        } else if base_changed && (grouped || previous_grouped) {
            out.push(VisibleRow::Spacer);
        }

        if grouped && (base_changed || namespace_changed) {
            out.push(VisibleRow::Group {
                name: row.base_name.clone(),
                namespaced: namespace.is_some(),
            });
        }

        out.push(VisibleRow::Dataset {
            index: *index,
            grouped,
        });

        previous_namespace = Some(namespace);
        previous_base = Some(row.base_name.as_str());
        previous_grouped = grouped;
    }

    out
}

/// One-line result for the jobs pane, so a finished job says what it did
/// instead of keeping its last progress line.
fn outcome_summary(outcome: &JobOutcome) -> String {
    match outcome {
        JobOutcome::RemoteListing { registry, rows } => {
            format!("{} dataset(s) in {registry}", rows.len())
        }
        JobOutcome::Resolved { path, .. } => path.display().to_string(),
        JobOutcome::Message { text, .. } => text.clone(),
    }
}

/// True when `next` is a fresher take on `previous`: the same phase and verb,
/// with only a counter moving. `[unpack] processed 16/12208 (0.1%)` replaces
/// `[unpack] processed 1/12208 (0.0%)`, while `[unpack] unpack complete` is
/// kept as its own line.
fn supersedes(previous: &str, next: &str) -> bool {
    if !previous.contains('%') || !next.contains('%') {
        return false;
    }
    let head = |line: &str| {
        line.split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
    };
    head(previous) == head(next)
}

/// Files under a cached dataset, largest first, capped so a bag with thousands
/// of split files cannot stall the frame. Returns the listing and how many
/// files were left out.
fn list_cached_files(root: &std::path::Path) -> (Vec<CachedFile>, usize) {
    const MAX_FILES: usize = 64;

    let mut files: Vec<CachedFile> = walkdir::WalkDir::new(root)
        .max_depth(4)
        .follow_links(false)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| {
            let size_bytes = entry.metadata().ok()?.len();
            let relative_path = entry
                .path()
                .strip_prefix(root)
                .unwrap_or(entry.path())
                .display()
                .to_string();
            Some(CachedFile {
                relative_path,
                size_bytes,
            })
        })
        .collect();

    files.sort_by(|a, b| {
        b.size_bytes
            .cmp(&a.size_bytes)
            .then_with(|| a.relative_path.cmp(&b.relative_path))
    });
    let truncated = files.len().saturating_sub(MAX_FILES);
    files.truncate(MAX_FILES);
    (files, truncated)
}

/// First dataset row at or after `from`.
fn first_selectable(visible: &[VisibleRow], from: usize) -> Option<usize> {
    visible
        .iter()
        .enumerate()
        .skip(from)
        .find(|(_, entry)| matches!(entry, VisibleRow::Dataset { .. }))
        .map(|(position, _)| position)
}

/// First or last dataset row of the whole list.
fn edge_selection(visible: &[VisibleRow], first: bool) -> Option<usize> {
    if first {
        first_selectable(visible, 0)
    } else {
        visible
            .iter()
            .enumerate()
            .rev()
            .find(|(_, entry)| matches!(entry, VisibleRow::Dataset { .. }))
            .map(|(position, _)| position)
    }
}

/// Position roughly `lines` rendered rows away from `selected`, then snapped to
/// the nearest dataset row in the direction of travel. Paging counts what is on
/// screen, headings and spacers included, so one press moves one screen.
fn page_selection(visible: &[VisibleRow], selected: Option<usize>, lines: isize) -> Option<usize> {
    if visible.is_empty() || lines == 0 {
        return None;
    }
    let Some(current) = selected else {
        return first_selectable(visible, 0);
    };

    let last = visible.len() as isize - 1;
    let target = (current as isize + lines).clamp(0, last) as usize;
    let forward = lines > 0;

    // Prefer a row past the target, then fall back towards the start.
    let ahead = if forward {
        visible
            .iter()
            .enumerate()
            .skip(target)
            .find(|(_, entry)| matches!(entry, VisibleRow::Dataset { .. }))
            .map(|(position, _)| position)
    } else {
        visible
            .iter()
            .enumerate()
            .take(target + 1)
            .rfind(|(_, entry)| matches!(entry, VisibleRow::Dataset { .. }))
            .map(|(position, _)| position)
    };

    ahead.or_else(|| edge_selection(visible, !forward))
}

/// Position `delta` dataset rows away from `selected`, stepping over the
/// headings and spacers so the grouped list navigates like a flat one. Clamps at
/// the ends instead of wrapping, so a move past the last row returns that row.
fn step_selection(visible: &[VisibleRow], selected: Option<usize>, delta: isize) -> Option<usize> {
    let Some(current) = selected else {
        return first_selectable(visible, 0);
    };
    if delta == 0 || visible.is_empty() {
        return None;
    }

    let step = delta.signum();
    let last = visible.len() as isize - 1;
    let mut position = current as isize;

    for _ in 0..delta.abs() {
        let mut next = position + step;
        while (0..=last).contains(&next)
            && !matches!(visible[next as usize], VisibleRow::Dataset { .. })
        {
            next += step;
        }
        if !(0..=last).contains(&next) {
            break;
        }
        position = next;
    }

    matches!(visible[position as usize], VisibleRow::Dataset { .. }).then_some(position as usize)
}

/// Keeps one registry per dataset when several carry the same bundle hash, the
/// same rule `marina list --remote --no-duplicates` applies.
fn hide_duplicate_registries(rows: &mut [DatasetRow]) {
    for row in rows.iter_mut() {
        let mut seen: Vec<String> = Vec::new();
        row.remotes.retain(|(_, info)| {
            let Some(hash) = info.as_ref().and_then(|info| info.bundle_hash.clone()) else {
                return true;
            };
            if seen.contains(&hash) {
                false
            } else {
                seen.push(hash);
                true
            }
        });
    }
}

fn split_patterns(raw: &str) -> Vec<String> {
    raw.split_whitespace().map(|p| p.to_string()).collect()
}

fn absolutize_folder_uri(kind: &str, uri: String) -> String {
    if kind != "folder" {
        return uri;
    }
    let scheme_end = uri.find("://").map(|index| index + 3).unwrap_or(0);
    let path = std::path::Path::new(&uri[scheme_end..]);
    if path.is_relative()
        && let Ok(current) = std::env::current_dir()
    {
        return format!("folder://{}", current.join(path).display());
    }
    uri
}

fn cycle_option(current: Option<String>, names: &[String], delta: i64) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    // The list is the registries plus "none" at the end.
    let position = current
        .as_ref()
        .and_then(|name| names.iter().position(|candidate| candidate == name))
        .map(|index| index as i64)
        .unwrap_or(names.len() as i64);
    let count = names.len() as i64 + 1;
    let next = (position + delta).rem_euclid(count);
    if next == names.len() as i64 {
        None
    } else {
        Some(names[next as usize].clone())
    }
}

fn step_u64(value: u64, delta: i64, min: u64, max: u64) -> u64 {
    let next = value as i64 + delta;
    next.clamp(min as i64, max as i64) as u64
}

fn mcap_choice_index(value: ConfigMcapCompression) -> usize {
    match value {
        ConfigMcapCompression::Zstd => 0,
        ConfigMcapCompression::Lz4 => 1,
        ConfigMcapCompression::None => 2,
    }
}

fn mcap_from_choice(index: usize) -> McapChunkCompression {
    match index {
        1 => McapChunkCompression::Lz4,
        2 => McapChunkCompression::None,
        _ => McapChunkCompression::Zstd,
    }
}

fn mcap_to_core(value: ConfigMcapCompression) -> McapChunkCompression {
    match value {
        ConfigMcapCompression::None => McapChunkCompression::None,
        ConfigMcapCompression::Zstd => McapChunkCompression::Zstd,
        ConfigMcapCompression::Lz4 => McapChunkCompression::Lz4,
    }
}

fn pointcloud_choice_index(value: ConfigPointcloudMode) -> usize {
    match value {
        ConfigPointcloudMode::Lossy => 0,
        ConfigPointcloudMode::Lossless => 1,
        ConfigPointcloudMode::Off => 2,
    }
}

fn pointcloud_from_choice(index: usize) -> PointCloudCompressionMode {
    match index {
        1 => PointCloudCompressionMode::Lossless,
        2 => PointCloudCompressionMode::Disabled,
        _ => PointCloudCompressionMode::Lossy,
    }
}

fn archive_choice_index(value: ConfigArchiveCompression) -> usize {
    match value {
        ConfigArchiveCompression::None => 0,
        ConfigArchiveCompression::Gzip => 1,
    }
}

fn archive_from_choice(index: usize) -> ArchiveCompression {
    match index {
        1 => ArchiveCompression::Gzip,
        _ => ArchiveCompression::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, cached: bool, remotes: &[(&str, Option<&str>)]) -> DatasetRow {
        let bag: BagRef = key.parse().expect("valid reference");
        DatasetRow {
            namespace: bag.namespace.clone(),
            base_name: bag.name.clone(),
            display_name: display_name(&bag),
            key: bag.to_string(),
            bag,
            local_dir: cached.then(|| PathBuf::from("/tmp/marina")),
            local_bytes: 0,
            remotes: remotes
                .iter()
                .map(|(name, hash)| {
                    (
                        (*name).to_string(),
                        hash.map(|hash| BagInfo {
                            bundle_hash: Some(hash.to_string()),
                            original_bytes: 1,
                            packed_bytes: 1,
                            pointcloud: None,
                            mcap_compression: None,
                            pushed_at: None,
                        }),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn presence_covers_cache_and_registries() {
        let cached = row("a:v1", true, &[]);
        assert!(cached.is_cached() && !cached.is_remote());

        let remote = row("a:v1", false, &[("team", None)]);
        assert!(!remote.is_cached() && remote.is_remote());

        let both = row("a:v1", true, &[("team", None)]);
        assert!(both.is_cached() && both.is_remote());
    }

    #[test]
    fn filter_matches_substrings_and_globs() {
        let entry = row("ns/dlg_feldtage:cut", false, &[("team_ssh", None)]);
        assert!(matches_filter(&entry, ""));
        assert!(matches_filter(&entry, "feldtage"));
        assert!(matches_filter(&entry, "ns/*:cut"));
        assert!(matches_filter(&entry, "team_ssh"));
        assert!(!matches_filter(&entry, "helipr"));
    }

    #[test]
    fn scope_splits_the_cache_from_the_registries() {
        let cached = row("a:v1", true, &[]);
        let remote = row("b:v1", false, &[("team", None)]);
        let both = row("c:v1", true, &[("team", None)]);

        assert!(
            [&cached, &remote, &both]
                .iter()
                .all(|row| Scope::All.accepts(row))
        );

        assert!(Scope::Local.accepts(&cached));
        assert!(Scope::Local.accepts(&both));
        assert!(!Scope::Local.accepts(&remote));

        assert!(Scope::Remote.accepts(&remote));
        assert!(Scope::Remote.accepts(&both));
        assert!(!Scope::Remote.accepts(&cached));
    }

    #[test]
    fn scope_cycles_back_to_all() {
        assert_eq!(Scope::All.next(), Scope::Local);
        assert_eq!(Scope::Local.next(), Scope::Remote);
        assert_eq!(Scope::Remote.next(), Scope::All);
    }

    #[test]
    fn duplicate_hiding_keeps_one_registry_per_hash() {
        let mut rows = vec![row(
            "a:v1",
            false,
            &[
                ("one", Some("hash")),
                ("two", Some("hash")),
                ("three", Some("other")),
            ],
        )];
        hide_duplicate_registries(&mut rows);
        let names: Vec<&str> = rows[0]
            .remotes
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(names, vec!["one", "three"]);
    }

    #[test]
    fn duplicate_hiding_keeps_registries_without_hashes() {
        let mut rows = vec![row("a:v1", false, &[("one", None), ("two", None)])];
        hide_duplicate_registries(&mut rows);
        assert_eq!(rows[0].remotes.len(), 2);
    }

    #[test]
    fn default_registry_cycles_through_none() {
        let names = vec!["a".to_string(), "b".to_string()];
        assert_eq!(cycle_option(None, &names, 1), Some("a".to_string()));
        assert_eq!(
            cycle_option(Some("a".to_string()), &names, 1),
            Some("b".to_string())
        );
        assert_eq!(cycle_option(Some("b".to_string()), &names, 1), None);
        assert_eq!(
            cycle_option(None, &names, -1),
            Some("b".to_string()),
            "stepping back from none wraps to the last registry"
        );
    }

    fn grouped_layout(keys: &[&str]) -> Vec<String> {
        let mut rows: Vec<DatasetRow> = keys.iter().map(|key| row(key, false, &[])).collect();
        rows.sort_by(|a, b| {
            a.namespace
                .as_deref()
                .unwrap_or("")
                .cmp(b.namespace.as_deref().unwrap_or(""))
                .then_with(|| a.base_name.cmp(&b.base_name))
                .then_with(|| a.key.cmp(&b.key))
        });
        let matching: Vec<usize> = (0..rows.len()).collect();
        group_rows(&rows, &matching)
            .into_iter()
            .map(|entry| match entry {
                VisibleRow::Namespace(namespace) => format!("ns {namespace}"),
                VisibleRow::Group { name, .. } => format!("group {name}"),
                VisibleRow::Dataset { index, grouped } => {
                    format!("row {}{}", rows[index].key, if grouped { " *" } else { "" })
                }
                VisibleRow::Spacer => "blank".to_string(),
            })
            .collect()
    }

    #[test]
    fn grouping_heads_namespaces_and_tag_variants() {
        assert_eq!(
            grouped_layout(&[
                "solo:v1",
                "helipr/town:a",
                "helipr/town:b",
                "helipr/other:x"
            ]),
            vec![
                "row solo:v1",
                "blank",
                "ns helipr",
                "row helipr/other:x",
                "blank",
                "group town",
                "row helipr/town:a *",
                "row helipr/town:b *",
            ]
        );
    }

    #[test]
    fn grouping_leaves_single_variants_ungrouped() {
        assert_eq!(
            grouped_layout(&["a:v1", "b:v1"]),
            vec!["row a:v1", "row b:v1"],
            "no headings when nothing shares a base name"
        );
    }

    #[test]
    fn selection_steps_over_headings_and_spacers() {
        let visible = vec![
            VisibleRow::Namespace("helipr".to_string()),
            VisibleRow::Dataset {
                index: 0,
                grouped: false,
            },
            VisibleRow::Spacer,
            VisibleRow::Group {
                name: "town".to_string(),
                namespaced: true,
            },
            VisibleRow::Dataset {
                index: 1,
                grouped: true,
            },
        ];

        assert_eq!(
            first_selectable(&visible, 0),
            Some(1),
            "skips the namespace heading"
        );
        assert_eq!(
            step_selection(&visible, Some(1), 1),
            Some(4),
            "skips the spacer and the group heading"
        );
        assert_eq!(
            step_selection(&visible, Some(4), 1),
            Some(4),
            "clamps at the last row"
        );
        assert_eq!(step_selection(&visible, Some(4), -1), Some(1));
        assert_eq!(
            step_selection(&visible, Some(1), -1),
            Some(1),
            "clamps at the first row"
        );
        assert_eq!(edge_selection(&visible, false), Some(4));
        assert_eq!(edge_selection(&visible, true), Some(1));
    }

    fn job_state() -> JobState {
        JobState {
            kind: JobKind::Clean { all: false },
            started: Instant::now(),
            finished: None,
            status: JobStatus::Running,
            last_line: String::new(),
            log: Vec::new(),
        }
    }

    #[test]
    fn repeated_progress_updates_collapse_into_one_line() {
        let mut job = job_state();
        job.push_log("[unpack] extracting archive /tmp/bundle.tar.gz".to_string());
        job.push_log("[unpack] processed 1/12208 (0.0%)".to_string());
        job.push_log("[unpack] processed 16/12208 (0.1%)".to_string());
        job.push_log("[unpack] processed 32/12208 (0.3%)".to_string());
        job.push_log("[unpack] unpack complete".to_string());

        assert_eq!(
            job.log,
            vec![
                "[unpack] extracting archive /tmp/bundle.tar.gz".to_string(),
                "[unpack] processed 32/12208 (0.3%)".to_string(),
                "[unpack] unpack complete".to_string(),
            ]
        );
    }

    #[test]
    fn progress_of_a_different_phase_starts_a_new_line() {
        let mut job = job_state();
        job.push_log("[pack] processed 4/8 (50.0%)".to_string());
        job.push_log("[unpack] processed 4/8 (50.0%)".to_string());
        assert_eq!(job.log.len(), 2, "phases keep their own line");
    }

    #[test]
    fn the_log_is_capped() {
        let mut job = job_state();
        for index in 0..(MAX_JOB_LOG_LINES + 25) {
            job.push_log(format!("[pull] file {index}"));
        }
        assert_eq!(job.log.len(), MAX_JOB_LOG_LINES);
        assert_eq!(
            job.log.last().map(String::as_str),
            Some(format!("[pull] file {}", MAX_JOB_LOG_LINES + 24).as_str())
        );
    }

    #[test]
    fn paging_moves_a_screen_and_lands_on_a_dataset() {
        // ns heading, four rows, a spacer and a group heading in the middle.
        let visible = vec![
            VisibleRow::Namespace("helipr".to_string()),
            VisibleRow::Dataset {
                index: 0,
                grouped: false,
            },
            VisibleRow::Dataset {
                index: 1,
                grouped: false,
            },
            VisibleRow::Spacer,
            VisibleRow::Group {
                name: "town".to_string(),
                namespaced: true,
            },
            VisibleRow::Dataset {
                index: 2,
                grouped: true,
            },
            VisibleRow::Dataset {
                index: 3,
                grouped: true,
            },
        ];

        // A three-line page from the first row lands past the spacer and the
        // group heading, on the next dataset.
        assert_eq!(page_selection(&visible, Some(1), 3), Some(5));
        // Paging past the end stops on the last dataset.
        assert_eq!(page_selection(&visible, Some(1), 99), Some(6));
        // Backwards skips the heading above it.
        assert_eq!(page_selection(&visible, Some(6), -3), Some(2));
        assert_eq!(page_selection(&visible, Some(2), -99), Some(1));
    }

    #[test]
    fn paging_an_empty_list_selects_nothing() {
        assert_eq!(page_selection(&[], Some(0), 5), None);
        assert_eq!(
            page_selection(&[VisibleRow::Spacer], Some(0), 5),
            None,
            "a list without datasets has nothing to land on"
        );
    }

    #[test]
    fn selection_ignores_a_list_without_datasets() {
        let visible = vec![
            VisibleRow::Namespace("helipr".to_string()),
            VisibleRow::Spacer,
        ];
        assert_eq!(first_selectable(&visible, 0), None);
        assert_eq!(step_selection(&visible, None, 1), None);
        assert_eq!(edge_selection(&visible, false), None);
    }

    #[test]
    fn patterns_split_on_whitespace() {
        assert_eq!(split_patterns("  "), Vec::<String>::new());
        assert_eq!(
            split_patterns("helipr/* other:*"),
            vec!["helipr/*".to_string(), "other:*".to_string()]
        );
    }
}
