use {
    crate::{
        channel::ParsedMessage,
        config::ConfigStorage,
        metrics::{
            CHANNEL_STORAGE_WRITE_COLLECTOR_INDEX, CHANNEL_STORAGE_WRITE_COMPRESSOR_INDEX,
            CHANNEL_STORAGE_WRITE_INDEX, STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL,
            STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL, STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL,
            STORAGE_WRITE_APPEND_SECONDS_TOTAL, STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL,
            STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL, STORAGE_WRITE_COMMIT_SECONDS_TOTAL,
            STORAGE_WRITE_COMPRESS_SECONDS_TOTAL, STORAGE_WRITE_ROTATE_SECONDS_TOTAL,
            STORAGE_WRITE_SERIALIZE_SECONDS_TOTAL, STORAGE_WRITE_TRIM_SECONDS_TOTAL,
        },
        storage::metadata::{
            ChunkMeta, Metadata, MetadataChunkCommit, MetadataMirror, MetadataTrimCommit,
            RotationCommit, SegmentMeta, SlotMeta,
        },
        util::{SpawnedThread, SpawnedThreads},
    },
    ::metrics::{counter, gauge},
    anyhow::{Context, anyhow},
    prost::encoding::{decode_varint, encode_varint},
    richat_filter::{
        filter::{FilteredUpdate, FilteredUpdateFilters},
        message::{Message, MessageParserEncoding, MessageRef},
    },
    richat_metrics::duration_to_seconds,
    richat_proto::geyser::SlotStatus,
    solana_clock::Slot,
    std::{
        borrow::Cow,
        collections::{BTreeMap, HashSet},
        fs::{File, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        path::PathBuf,
        sync::Arc,
        thread,
        time::Instant,
    },
    zstd::{bulk::Compressor, stream::decode_all as zstd_decode_all},
};

/// Compression algorithm used for a chunk payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkCompression {
    None,
    Zstd(i32),
}

impl ChunkCompression {
    const TAG_NONE: u8 = 0;
    const TAG_ZSTD: u8 = 1;

    const fn tag(self) -> u8 {
        match self {
            Self::None => Self::TAG_NONE,
            Self::Zstd(_) => Self::TAG_ZSTD,
        }
    }

    fn from_tag(tag: u8) -> anyhow::Result<Self> {
        match tag {
            Self::TAG_NONE => Ok(Self::None),
            Self::TAG_ZSTD => Ok(Self::Zstd(0)),
            other => anyhow::bail!("unsupported chunk compression tag: {other}"),
        }
    }
}

/// A single decompressed chunk that yields decoded records as an iterator.
pub struct DecompressedChunk {
    first_index: u64,
    skip: usize,
    offset: usize,
    record_index: usize,
    parser: MessageParserEncoding,
    data: Vec<u8>,
}

impl Iterator for DecompressedChunk {
    type Item = anyhow::Result<(u64, ParsedMessage)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.offset >= self.data.len() {
                return None;
            }

            let mut slice = &self.data[self.offset..];
            let record_len: usize = match decode_varint(&mut slice) {
                Ok(len) => match len.try_into() {
                    Ok(len) => len,
                    Err(_) => return Some(Err(anyhow!("record len overflows usize: {len}"))),
                },
                Err(error) => return Some(Err(error).context("failed to decode record len")),
            };

            let Some((record, rest)) = slice.split_at_checked(record_len) else {
                return Some(Err(anyhow!("record extends past chunk boundary")));
            };

            self.offset = self.data.len() - rest.len();
            let record_index = self.record_index;
            self.record_index += 1;
            if record_index < self.skip {
                continue;
            }

            let mut record_slice = record;
            if let Err(error) = decode_varint(&mut record_slice) {
                return Some(Err(error).context("invalid slice size, failed to decode slot"));
            }

            return Some(
                Message::parse(Cow::Borrowed(record_slice), self.parser)
                    .map(|msg| (self.first_index + record_index as u64, msg.into()))
                    .context("failed to parse message"),
            );
        }
    }
}

/// Sequential replay reader over chunk metadata and segment files.
pub struct SegmentReader {
    parser: MessageParserEncoding,
    metadata: Metadata,
    next_index: u64,
    current_segment_id: Option<u64>,
    current_file: Option<File>,
    failed: bool,
}

impl SegmentReader {
    pub fn new(metadata: &Metadata, start_index: u64, parser: MessageParserEncoding) -> Self {
        Self {
            parser,
            metadata: metadata.clone(),
            next_index: start_index,
            current_segment_id: None,
            current_file: None,
            failed: false,
        }
    }

    fn open_segment(&mut self, segment_id: u64) -> anyhow::Result<&mut File> {
        if self.current_segment_id != Some(segment_id) {
            let path = self
                .metadata
                .segments_path()
                .join(segment_file_name(segment_id));
            let file = File::open(&path)
                .with_context(|| format!("failed to open segment file: {path:?}"))?;
            self.current_segment_id = Some(segment_id);
            self.current_file = Some(file);
        }

        self.current_file
            .as_mut()
            .context("segment file should be opened")
    }

    fn load_next_chunk(&mut self) -> anyhow::Result<Option<DecompressedChunk>> {
        let chunk = {
            let catalog = self.metadata.catalog();
            let pos = catalog
                .chunks
                .partition_point(|c| c.last_index < self.next_index);
            match catalog.chunks.get(pos) {
                Some(&chunk) => chunk,
                None => return Ok(None),
            }
        };

        let file = self.open_segment(chunk.segment_id)?;
        file.seek(SeekFrom::Start(chunk.offset))?;

        let chunk_size: usize = chunk
            .size
            .try_into()
            .context("chunk size overflows usize")?;
        // SAFETY: read_exact either fills all bytes or returns Err,
        // dropping payload without reading uninitialized data.
        #[allow(clippy::uninit_vec)]
        let mut payload = unsafe {
            let mut vec = Vec::with_capacity(chunk_size);
            vec.set_len(chunk_size);
            vec
        };
        file.read_exact(&mut payload)?;
        counter!(STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL).increment(payload.len() as u64);

        let compression = ChunkCompression::from_tag(chunk.compression)
            .context("unsupported chunk compression")?;
        let uncompressed = match compression {
            ChunkCompression::None => payload,
            ChunkCompression::Zstd(_) => {
                zstd_decode_all(payload.as_slice()).context("failed to decompress chunk")?
            }
        };
        counter!(STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL).increment(uncompressed.len() as u64);

        let skip: usize = self
            .next_index
            .saturating_sub(chunk.first_index)
            .try_into()
            .context("skip offset overflows usize")?;
        self.next_index = chunk.last_index + 1;

        Ok(Some(DecompressedChunk {
            first_index: chunk.first_index,
            skip,
            offset: 0,
            record_index: 0,
            parser: self.parser,
            data: uncompressed,
        }))
    }
}

impl Iterator for SegmentReader {
    type Item = anyhow::Result<DecompressedChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        match self.load_next_chunk() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

#[derive(Debug)]
pub enum WriterCommand {
    PushMessage {
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
        source: Option<Arc<str>>,
    },
    RemoveReplay {
        slot: Slot,
        until: Option<u64>,
    },
}

enum CollectorOutput {
    RawChunk {
        seq: u64,
        first_index: u64,
        last_index: u64,
        records: Vec<RawRecord>,
        pending_slots: Vec<PendingSlot>,
        pending_finalized: HashSet<Slot>,
    },
    Trim {
        seq: u64,
        slot: Slot,
        until: Option<u64>,
    },
}

struct RawRecord {
    slot: Slot,
    message: ParsedMessage,
}

#[derive(Debug)]
struct PendingSlot {
    init: bool,
    slot: Slot,
    first_index: u64,
    source: Option<Arc<str>>,
}

#[derive(Debug, Default)]
struct PendingChunkMeta {
    estimated_uncompressed_size: usize,
    record_count: u32,
    first_index: u64,
    last_index: u64,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

impl PendingChunkMeta {
    fn push(
        &mut self,
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: &ParsedMessage,
        source: Option<Arc<str>>,
    ) {
        if self.record_count == 0 {
            self.first_index = index;
        }
        self.last_index = index;
        self.estimated_uncompressed_size = self
            .estimated_uncompressed_size
            .saturating_add(message.size().saturating_add(16));

        if init || source.is_some() {
            match self
                .pending_slots
                .iter_mut()
                .rev()
                .find(|pending| pending.slot == slot)
            {
                Some(pending)
                    if (!init || pending.init)
                        && (pending.source.is_none()
                            || source.is_none()
                            || pending.source == source) =>
                {
                    if pending.source.is_none() {
                        pending.source = source;
                    }
                }
                _ => self.pending_slots.push(PendingSlot {
                    init,
                    slot,
                    first_index: head,
                    source,
                }),
            }
        }
        if let ParsedMessage::Slot(message) = message
            && message.status() == SlotStatus::SlotFinalized
        {
            self.pending_finalized.insert(slot);
        }

        self.record_count += 1;
    }

    const fn should_flush(&self, target_size: usize) -> bool {
        self.estimated_uncompressed_size >= target_size
    }

    const fn is_empty(&self) -> bool {
        self.record_count == 0
    }
}

fn spawn_collector(
    chunk_target_size: usize,
    serialize_affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<SpawnedThread> {
    let th_name = "richatStrgCol".to_owned();
    let jh = thread::Builder::new()
        .name(th_name.clone())
        .spawn(move || {
            if let Some(cpus) = serialize_affinity {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            run_collector(chunk_target_size, rx, tx)
        })?;
    Ok((th_name, Some(jh)))
}

fn run_collector(
    chunk_target_size: usize,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<()> {
    let mut records: Vec<RawRecord> = Vec::new();
    let mut pending = PendingChunkMeta::default();
    let mut next_seq: u64 = 0;

    let mut closed = false;
    while !closed {
        let mut should_flush = false;
        let mut trim = None;

        closed = match rx.recv() {
            Ok(command) => {
                match command {
                    WriterCommand::PushMessage {
                        init,
                        slot,
                        head,
                        index,
                        message,
                        source,
                    } => {
                        pending.push(init, slot, head, index, &message, source);
                        records.push(RawRecord { slot, message });
                        counter!(CHANNEL_STORAGE_WRITE_COLLECTOR_INDEX).absolute(index);

                        should_flush = pending.should_flush(chunk_target_size);
                    }
                    WriterCommand::RemoveReplay { slot, until } => {
                        should_flush = true;
                        trim = Some((slot, until));
                    }
                }
                false
            }
            Err(_error) => true,
        };

        if (should_flush || closed) && !pending.is_empty() {
            let flushed = std::mem::take(&mut pending);

            tx.send(CollectorOutput::RawChunk {
                seq: next_seq,
                first_index: flushed.first_index,
                last_index: flushed.last_index,
                records: std::mem::take(&mut records),
                pending_slots: flushed.pending_slots,
                pending_finalized: flushed.pending_finalized,
            })
            .map_err(|_| anyhow!("collector output channel closed"))?;
            next_seq += 1;
        }

        if let Some((slot, until)) = trim {
            tx.send(CollectorOutput::Trim {
                seq: next_seq,
                slot,
                until,
            })
            .map_err(|_| anyhow!("collector output channel closed"))?;
            next_seq += 1;
        }
    }

    Ok(())
}

enum CompressorOutput {
    CompressedChunk {
        seq: u64,
        chunk: CompressedChunk,
    },
    Trim {
        seq: u64,
        slot: Slot,
        until: Option<u64>,
    },
}

impl CompressorOutput {
    const fn seq(&self) -> u64 {
        match self {
            Self::CompressedChunk { seq, .. } => *seq,
            Self::Trim { seq, .. } => *seq,
        }
    }
}

#[derive(Debug)]
struct CompressedChunk {
    compression: u8,
    first_index: u64,
    last_index: u64,
    payload: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

fn spawn_compressor_pool(
    threads: usize,
    chunk_compression: Option<ChunkCompression>,
    affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<Vec<SpawnedThread>> {
    let mut handles = Vec::with_capacity(threads);
    for index in 0..threads {
        let th_name = format!("richatStrgCmp{index:02}");
        let rx = rx.clone();
        let tx = tx.clone();
        let cpus = affinity.as_ref().map(|aff| {
            if threads == aff.len() {
                vec![aff[index]]
            } else {
                aff.clone()
            }
        });
        let jh = thread::Builder::new()
            .name(th_name.clone())
            .spawn(move || {
                if let Some(cpus) = cpus {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity");
                }
                run_compressor(index, chunk_compression, rx, tx)
            })?;
        handles.push((th_name, Some(jh)));
    }
    Ok(handles)
}

fn run_compressor(
    thread_index: usize,
    chunk_compression: Option<ChunkCompression>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<()> {
    let mut compressor = match chunk_compression {
        Some(ChunkCompression::Zstd(level)) => {
            Some(Compressor::new(level).context("failed to create zstd compressor")?)
        }
        _ => None,
    };
    let compression_tag = chunk_compression.unwrap_or(ChunkCompression::None).tag();
    let thread_index_str: &'static str = thread_index.to_string().leak();

    // Per-thread serialization buffers
    let mut record_buf = Vec::with_capacity(11 * 1024 * 1024);
    let mut chunk_buf = Vec::new();

    loop {
        match rx.recv() {
            Ok(CollectorOutput::RawChunk {
                seq,
                first_index,
                last_index,
                records,
                pending_slots,
                pending_finalized,
            }) => {
                // Serialize all records
                let serialize_started_at = Instant::now();
                chunk_buf.clear();
                for record in &records {
                    record_buf.clear();
                    let message_ref: MessageRef = (&record.message).into();
                    let filtered = FilteredUpdate {
                        filters: FilteredUpdateFilters::new(),
                        filtered_update: message_ref.into(),
                    };
                    encode_varint(record.slot, &mut record_buf);
                    filtered.encode(&mut record_buf);
                    encode_varint(record_buf.len() as u64, &mut chunk_buf);
                    chunk_buf.extend_from_slice(&record_buf);
                }
                gauge!(STORAGE_WRITE_SERIALIZE_SECONDS_TOTAL)
                    .increment(duration_to_seconds(serialize_started_at.elapsed()));
                counter!(CHANNEL_STORAGE_WRITE_COMPRESSOR_INDEX, "thread" => thread_index_str)
                    .absolute(last_index);

                counter!(STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL)
                    .increment(chunk_buf.len() as u64);

                // Compress
                let payload = if let Some(compressor) = compressor.as_mut() {
                    let compress_started_at = Instant::now();
                    let compressed = compressor
                        .compress(&chunk_buf)
                        .context("failed to compress chunk")?;
                    gauge!(STORAGE_WRITE_COMPRESS_SECONDS_TOTAL)
                        .increment(duration_to_seconds(compress_started_at.elapsed()));
                    compressed
                } else {
                    std::mem::take(&mut chunk_buf)
                };
                counter!(STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL)
                    .increment(payload.len() as u64);

                let compressed = CompressedChunk {
                    compression: compression_tag,
                    first_index,
                    last_index,
                    payload,
                    pending_slots,
                    pending_finalized,
                };

                if tx
                    .send(CompressorOutput::CompressedChunk {
                        seq,
                        chunk: compressed,
                    })
                    .is_err()
                {
                    break;
                }
            }
            Ok(CollectorOutput::Trim { seq, slot, until }) => {
                if tx
                    .send(CompressorOutput::Trim { seq, slot, until })
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}

/// Long-lived state for the ordered storage append path.
struct SegmentWriter {
    segments_path: PathBuf,
    segment_target_size: u64,
    metadata: Metadata,
    active_segment: SegmentMeta,
    active_file: File,
}

impl SegmentWriter {
    fn open(
        segments_path: PathBuf,
        segment_target_size: u64,
        metadata: Metadata,
    ) -> anyhow::Result<Self> {
        {
            let catalog = metadata.catalog();
            if catalog.state.active_segment_id == 0 {
                let mut state = catalog.state;
                state.active_segment_id = state.next_segment_id;
                state.next_segment_id += 1;
                let segment = SegmentMeta::empty(state.active_segment_id, 0);
                drop(catalog);
                let path = segments_path.join(segment_file_name(segment.segment_id));
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .with_context(|| format!("failed to create segment file: {path:?}"))?;
                metadata.initialize_empty(&state, segment)?;
            }
        }

        let active_segment = {
            let catalog = metadata.catalog();
            catalog
                .segments
                .get(&catalog.state.active_segment_id)
                .copied()
                .context("missing active segment metadata")?
        };
        let active_path = segments_path.join(segment_file_name(active_segment.segment_id));
        let mut active_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)
            .with_context(|| format!("failed to open active segment: {active_path:?}"))?;
        active_file.seek(SeekFrom::Start(active_segment.file_len))?;

        Ok(Self {
            segments_path,
            segment_target_size,
            metadata,
            active_segment,
            active_file,
        })
    }

    fn run(mut self, rx: kanal::Receiver<CompressorOutput>) -> anyhow::Result<()> {
        let mut next_seq: u64 = 0;
        let mut pending: BTreeMap<u64, CompressorOutput> = BTreeMap::new();

        while let Ok(output) = rx.recv() {
            pending.insert(output.seq(), output);
            while let Some(item) = pending.remove(&next_seq) {
                match item {
                    CompressorOutput::CompressedChunk { chunk, .. } => {
                        self.append_chunk(chunk)?;
                    }
                    CompressorOutput::Trim { slot, until, .. } => {
                        self.handle_trim(slot, until)?;
                    }
                }
                next_seq += 1;
            }
        }

        Ok(())
    }

    fn append_chunk(&mut self, chunk: CompressedChunk) -> anyhow::Result<()> {
        let chunk_meta = ChunkMeta {
            segment_id: self.active_segment.segment_id,
            offset: self.active_segment.file_len,
            size: chunk.payload.len() as u64,
            compression: chunk.compression,
            first_index: chunk.first_index,
            last_index: chunk.last_index,
        };

        let append_started_at = Instant::now();
        self.active_file
            .seek(SeekFrom::Start(self.active_segment.file_len))?;
        self.active_file.write_all(&chunk.payload)?;
        self.active_file.sync_data()?;
        gauge!(STORAGE_WRITE_APPEND_SECONDS_TOTAL)
            .increment(duration_to_seconds(append_started_at.elapsed()));

        let commit_started_at = Instant::now();
        let commit = {
            let catalog = self.metadata.catalog();
            Self::build_chunk_commit(
                &catalog,
                self.active_segment,
                chunk_meta,
                &chunk.pending_slots,
                &chunk.pending_finalized,
            )?
        };
        self.metadata.apply_chunk_commit(&commit)?;
        gauge!(STORAGE_WRITE_COMMIT_SECONDS_TOTAL)
            .increment(duration_to_seconds(commit_started_at.elapsed()));

        counter!(CHANNEL_STORAGE_WRITE_INDEX).absolute(chunk_meta.last_index);
        counter!(STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL).increment(1);

        self.active_segment = commit.segment;
        if self.active_segment.file_len >= self.segment_target_size {
            self.rotate_segment()?;
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> anyhow::Result<()> {
        let rotate_started_at = Instant::now();
        let (new_segment, state) = {
            let catalog = self.metadata.catalog();
            let mut state = catalog.state;
            let new_segment_id = state.next_segment_id;
            state.next_segment_id += 1;
            state.active_segment_id = new_segment_id;
            (SegmentMeta::empty(new_segment_id, 0), state)
        };

        let new_path = self
            .segments_path
            .join(segment_file_name(new_segment.segment_id));
        let new_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&new_path)
            .with_context(|| format!("failed to create next segment: {new_path:?}"))?;

        let sealed_segment = SegmentMeta {
            sealed: true,
            ..self.active_segment
        };
        let commit = RotationCommit {
            sealed_segment,
            new_segment,
            state,
        };
        self.metadata.apply_rotation_commit(commit)?;

        gauge!(STORAGE_WRITE_ROTATE_SECONDS_TOTAL)
            .increment(duration_to_seconds(rotate_started_at.elapsed()));
        self.active_segment = new_segment;
        self.active_file = new_file;
        Ok(())
    }

    fn handle_trim(&mut self, slot: Slot, until: Option<u64>) -> anyhow::Result<()> {
        let trim_started_at = Instant::now();
        let commit = {
            let catalog = self.metadata.catalog();
            Self::build_trim_commit(&catalog, slot, until)
        };
        self.metadata.apply_trim_commit(&commit)?;

        for segment_id in &commit.deleted_segments {
            let path = self.segments_path.join(segment_file_name(*segment_id));
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to delete segment {path:?}"));
                }
            }
        }
        gauge!(STORAGE_WRITE_TRIM_SECONDS_TOTAL)
            .increment(duration_to_seconds(trim_started_at.elapsed()));
        Ok(())
    }

    fn build_chunk_commit(
        catalog: &MetadataMirror,
        active_segment: SegmentMeta,
        chunk: ChunkMeta,
        pending_slots: &[PendingSlot],
        pending_finalized: &HashSet<Slot>,
    ) -> anyhow::Result<MetadataChunkCommit> {
        let mut segment = active_segment;
        segment.last_index = chunk.last_index;
        segment.file_len += chunk.size;
        segment.chunk_count += 1;

        let mut slot_updates = BTreeMap::<Slot, SlotMeta>::new();
        for pending in pending_slots {
            if !pending.init
                && !catalog.slots.contains_key(&pending.slot)
                && !slot_updates.contains_key(&pending.slot)
            {
                continue;
            }
            let metadata = slot_updates.entry(pending.slot).or_insert_with(|| {
                catalog
                    .slots
                    .get(&pending.slot)
                    .cloned()
                    .unwrap_or_else(|| SlotMeta {
                        slot: pending.slot,
                        finalized: false,
                        first_index: pending.first_index,
                        segment_id: chunk.segment_id,
                        source: None,
                    })
            });
            if let Some(source) = &pending.source {
                anyhow::ensure!(
                    metadata.source.as_ref().is_none_or(|owner| owner == source),
                    "slot {} source ownership changed from {:?} to {source}",
                    pending.slot,
                    metadata.source,
                );
                if metadata.source.is_none() {
                    metadata.source = Some(Arc::clone(source));
                }
            }
        }

        for slot in pending_finalized {
            if let Some(metadata) = slot_updates.get_mut(slot) {
                metadata.finalized = true;
            } else if let Some(metadata) = catalog.slots.get(slot) {
                let mut metadata = metadata.clone();
                metadata.finalized = true;
                slot_updates.insert(*slot, metadata);
            }
        }
        let (new_slots, updated_slots) = slot_updates
            .into_values()
            .partition(|metadata| !catalog.slots.contains_key(&metadata.slot));

        Ok(MetadataChunkCommit {
            new_slots,
            updated_slots,
            chunk,
            segment,
            state: catalog.state,
        })
    }

    fn build_trim_commit(
        catalog: &MetadataMirror,
        slot: Slot,
        until: Option<u64>,
    ) -> MetadataTrimCommit {
        let deleted_segments = until
            .map(|floor| {
                catalog
                    .segments
                    .values()
                    .filter(|segment| {
                        segment.sealed && segment.chunk_count > 0 && segment.last_index < floor
                    })
                    .map(|segment| segment.segment_id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let deleted_slots = catalog
            .slots
            .values()
            .filter(|meta| deleted_segments.contains(&meta.segment_id))
            .map(|meta| meta.slot)
            .collect::<Vec<_>>();
        let deleted_chunks = catalog
            .chunks
            .iter()
            .filter(|chunk| deleted_segments.contains(&chunk.segment_id))
            .map(|chunk| chunk.first_index)
            .collect::<Vec<_>>();

        MetadataTrimCommit {
            removed_slot: slot,
            deleted_slots,
            deleted_segments,
            deleted_chunks,
        }
    }
}

fn spawn_writer(
    segments_path: PathBuf,
    segment_target_size: u64,
    affinity: Option<Vec<usize>>,
    metadata: Metadata,
    rx: kanal::Receiver<CompressorOutput>,
) -> anyhow::Result<SpawnedThread> {
    let th_name = "richatStrgWrt".to_owned();
    let jh = thread::Builder::new()
        .name(th_name.clone())
        .spawn(move || {
            if let Some(cpus) = affinity {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            SegmentWriter::open(segments_path, segment_target_size, metadata)?.run(rx)
        })?;
    Ok((th_name, Some(jh)))
}

/// Creates channels and spawns collector, compressor, and writer threads.
pub fn spawn_write_pipeline(
    config: &ConfigStorage,
    metadata: Metadata,
) -> anyhow::Result<(kanal::Sender<WriterCommand>, SpawnedThreads)> {
    let (write_tx, write_rx) = kanal::bounded(config.collector_channel_size);
    let (collector_tx, collector_rx) = kanal::bounded(config.compressor_channel_size);
    let (compressor_tx, compressor_rx) = kanal::bounded(config.writer_channel_size);

    let mut threads = vec![];
    threads.push(spawn_collector(
        config.chunk_target_size,
        config.serialize_affinity.clone(),
        write_rx,
        collector_tx,
    )?);
    threads.extend(spawn_compressor_pool(
        config.compressor_threads,
        config.chunk_compression,
        config.compressor_affinity.clone(),
        collector_rx,
        compressor_tx,
    )?);
    threads.push(spawn_writer(
        config.segments_path(),
        config.segment_target_size as u64,
        config.write_affinity.clone(),
        metadata,
        compressor_rx,
    )?);

    Ok((write_tx, threads))
}

fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:012}.seg")
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        prost::Message as _,
        richat_proto::geyser::{
            SubscribeUpdate, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        },
    };

    fn message(slot: Slot) -> ParsedMessage {
        let update = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                status: SlotStatus::SlotCreatedBank as i32,
                ..Default::default()
            })),
            created_at: Some(Default::default()),
            ..Default::default()
        };
        Message::parse(update.encode_to_vec().into(), MessageParserEncoding::Prost)
            .unwrap()
            .into()
    }

    fn commit(
        catalog: &MetadataMirror,
        pending: &[PendingSlot],
    ) -> anyhow::Result<MetadataChunkCommit> {
        SegmentWriter::build_chunk_commit(
            catalog,
            SegmentMeta::empty(9, 0),
            ChunkMeta {
                segment_id: 9,
                offset: 0,
                size: 100,
                compression: 0,
                first_index: 3,
                last_index: 4,
            },
            pending,
            &HashSet::from([100]),
        )
    }

    fn catalog(source: Option<&str>) -> MetadataMirror {
        let mut catalog = MetadataMirror::default();
        catalog.slots.insert(
            100,
            SlotMeta {
                slot: 100,
                finalized: false,
                first_index: 1,
                segment_id: 2,
                source: source.map(Arc::from),
            },
        );
        catalog
    }

    #[test]
    fn pending_chunk_coalesces_repeated_identical_owner_metadata() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        for index in 1..=1_024 {
            pending.push(
                index == 1,
                100,
                index,
                index,
                &message,
                Some(Arc::from("one")),
            );
        }
        assert_eq!(pending.record_count, 1_024);
        assert_eq!((pending.first_index, pending.last_index), (1, 1_024));
        assert_eq!(pending.pending_slots.len(), 1);
        assert!(pending.pending_slots[0].init);
        assert_eq!(pending.pending_slots[0].first_index, 1);
        let result = commit(&MetadataMirror::default(), &pending.pending_slots).unwrap();
        assert_eq!(result.new_slots.len(), 1);
        assert_eq!(result.new_slots[0].source.as_deref(), Some("one"));
    }

    #[test]
    fn pending_chunk_merges_late_owner_without_resetting_initial_head() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        pending.push(true, 100, 1, 1, &message, None);
        pending.push(false, 100, 999, 2, &message, Some(Arc::from("one")));
        pending.push(true, 100, 999, 3, &message, None);
        assert_eq!(pending.pending_slots.len(), 1);
        assert!(pending.pending_slots[0].init);
        assert_eq!(pending.pending_slots[0].first_index, 1);
        let result = commit(&MetadataMirror::default(), &pending.pending_slots).unwrap();
        assert_eq!(result.new_slots.len(), 1);
        assert_eq!(result.new_slots[0].first_index, 1);
        assert_eq!(result.new_slots[0].source.as_deref(), Some("one"));
    }

    #[test]
    fn pending_chunk_late_owner_across_chunks_preserves_original_start() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        pending.push(true, 100, 1, 1, &message, None);
        let initial = std::mem::take(&mut pending);
        let initial = commit(&MetadataMirror::default(), &initial.pending_slots).unwrap();
        assert_eq!(initial.new_slots.len(), 1);
        assert!(initial.new_slots[0].source.is_none());
        let mut catalog = MetadataMirror::default();
        catalog.slots.insert(100, initial.new_slots[0].clone());

        for index in 2..=16 {
            pending.push(false, 100, 999, index, &message, Some(Arc::from("one")));
        }
        assert_eq!(pending.pending_slots.len(), 1);
        assert!(!pending.pending_slots[0].init);
        let result = commit(&catalog, &pending.pending_slots).unwrap();
        assert!(result.new_slots.is_empty());
        assert_eq!(result.updated_slots.len(), 1);
        assert_eq!(result.updated_slots[0].first_index, 1);
        assert_eq!(result.updated_slots[0].segment_id, 9);
        assert_eq!(result.updated_slots[0].source.as_deref(), Some("one"));
    }

    #[test]
    fn pending_chunk_cannot_coalesce_away_conflicting_owners() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        for (index, owner) in [(1, "one"), (2, "two"), (3, "one"), (4, "one")] {
            pending.push(index == 1, 100, 1, index, &message, Some(Arc::from(owner)));
        }
        let error = commit(&MetadataMirror::default(), &pending.pending_slots).unwrap_err();
        assert!(error.to_string().contains("source ownership changed"));
    }

    #[test]
    fn pending_chunk_source_only_updates_cannot_resurrect_a_slot() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        for index in 1..=16 {
            pending.push(false, 100, 999, index, &message, Some(Arc::from("one")));
        }
        assert_eq!(pending.pending_slots.len(), 1);
        assert!(!pending.pending_slots[0].init);
        let result = commit(&MetadataMirror::default(), &pending.pending_slots).unwrap();
        assert!(result.new_slots.is_empty());
        assert!(result.updated_slots.is_empty());
    }

    #[test]
    fn pending_chunk_ignored_source_before_init_cannot_claim_the_new_slot() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        pending.push(false, 100, 999, 1, &message, Some(Arc::from("old")));
        pending.push(true, 100, 2, 2, &message, None);
        pending.push(false, 100, 999, 3, &message, Some(Arc::from("new")));
        let result = commit(&MetadataMirror::default(), &pending.pending_slots).unwrap();
        assert_eq!(result.new_slots.len(), 1);
        assert_eq!(result.new_slots[0].first_index, 2);
        assert_eq!(result.new_slots[0].source.as_deref(), Some("new"));
    }

    #[test]
    fn pending_chunk_ordinary_records_need_no_slot_metadata() {
        let mut pending = PendingChunkMeta::default();
        let message = message(100);
        for index in 1..=1_024 {
            pending.push(false, 100, 1, index, &message, None);
        }
        assert_eq!(pending.record_count, 1_024);
        assert!(pending.pending_slots.is_empty());
    }

    #[test]
    fn late_source_update_preserves_existing_replay_start() {
        let result = commit(
            &catalog(None),
            &[PendingSlot {
                init: false,
                slot: 100,
                first_index: 999,
                source: Some(Arc::from("one")),
            }],
        )
        .unwrap();
        assert!(result.new_slots.is_empty());
        assert_eq!(result.updated_slots.len(), 1);
        let slot = &result.updated_slots[0];
        assert_eq!(
            (slot.first_index, slot.segment_id, slot.finalized),
            (1, 2, true)
        );
        assert_eq!(slot.source.as_deref(), Some("one"));
    }

    #[test]
    fn repeated_init_preserves_original_start_and_owner() {
        let result = commit(
            &catalog(Some("one")),
            &[PendingSlot {
                init: true,
                slot: 100,
                first_index: 999,
                source: None,
            }],
        )
        .unwrap();
        assert!(result.new_slots.is_empty());
        assert_eq!(result.updated_slots.len(), 1);
        let slot = &result.updated_slots[0];
        assert_eq!((slot.first_index, slot.segment_id), (1, 2));
        assert_eq!(slot.source.as_deref(), Some("one"));
    }

    #[test]
    fn source_update_in_initial_chunk_keeps_first_record_index() {
        let result = commit(
            &MetadataMirror::default(),
            &[
                PendingSlot {
                    init: true,
                    slot: 100,
                    first_index: 1,
                    source: None,
                },
                PendingSlot {
                    init: false,
                    slot: 100,
                    first_index: 999,
                    source: Some(Arc::from("one")),
                },
            ],
        )
        .unwrap();
        assert!(result.updated_slots.is_empty());
        assert_eq!(result.new_slots.len(), 1);
        assert_eq!(result.new_slots[0].first_index, 1);
        assert_eq!(result.new_slots[0].source.as_deref(), Some("one"));
    }

    #[test]
    fn source_only_update_does_not_recreate_absent_or_trimmed_slot() {
        let result = commit(
            &MetadataMirror::default(),
            &[PendingSlot {
                init: false,
                slot: 100,
                first_index: 999,
                source: Some(Arc::from("one")),
            }],
        )
        .unwrap();
        assert!(result.new_slots.is_empty());
        assert!(result.updated_slots.is_empty());
    }

    #[test]
    fn source_update_cannot_replace_an_existing_owner() {
        assert!(
            commit(
                &catalog(Some("one")),
                &[PendingSlot {
                    init: false,
                    slot: 100,
                    first_index: 999,
                    source: Some(Arc::from("two")),
                }]
            )
            .is_err()
        );
    }
}
