// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::str::FromStr;

use itertools::Itertools;
use quickwit_actors::ActorContext;
use quickwit_common::pretty::PrettySample;
use quickwit_config::{RetentionPolicy, RetentionTimestampType};
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::metastore::{
    ListSplitsRequest, MarkSplitsForDeletionRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{IndexUid, SplitId};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::{info, warn};

use crate::actors::RetentionPolicyExecutor;

const RETENTION_TIMESTAMP_MODE_ENV_KEY: &str = "QW_RETENTION_TIMESTAMP_MODE";

/// Maximum number of splits logged for each difference reported by the dry run.
const DRY_RUN_MAX_SAMPLE_SPLITS: usize = 10;

/// Selects the split time range against which retention policies are evaluated.
///
/// Configured for the whole janitor with the `QW_RETENTION_TIMESTAMP_MODE` environment
/// variable (`primary`, `secondary-dry-run` or `secondary`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetentionTimestampMode {
    /// Splits expire based on their primary time range.
    #[default]
    Primary,
    /// Splits expire based on their primary time range, but the splits that the `Secondary`
    /// mode would treat differently are logged.
    SecondaryDryRun,
    /// Splits expire based on their secondary time range, or on their primary time range
    /// when they don't have a secondary one.
    Secondary,
}

impl FromStr for RetentionTimestampMode {
    type Err = String;

    fn from_str(mode_str: &str) -> Result<Self, Self::Err> {
        match mode_str {
            "primary" => Ok(Self::Primary),
            "secondary-dry-run" => Ok(Self::SecondaryDryRun),
            "secondary" => Ok(Self::Secondary),
            _ => Err(format!("unknown retention timestamp mode `{mode_str}`")),
        }
    }
}

impl RetentionTimestampMode {
    /// Reads the mode from the environment. Unset or invalid values fall back to `Primary`.
    pub fn from_env() -> Self {
        quickwit_common::get_from_env(RETENTION_TIMESTAMP_MODE_ENV_KEY, Self::Primary, false)
    }
}

/// Detect all expired splits based a retention policy and
/// only mark them as `MarkedForDeletion`. Actual split deletion
/// is taken care of by the garbage collector.
///
/// * `index_id` - The target index id.
/// * `metastore` - The metastore managing the target index.
/// * `retention_policy` - The retention policy to used to evaluate the splits.
/// * `timestamp_mode` - The split time range the retention policy is evaluated against.
/// * `secondary_timestamp_field_opt` - The secondary timestamp field of the index, if any.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn run_execute_retention_policy(
    index_uid: IndexUid,
    metastore: MetastoreServiceClient,
    retention_policy: &RetentionPolicy,
    timestamp_mode: RetentionTimestampMode,
    secondary_timestamp_field_opt: Option<&str>,
    ctx: &ActorContext<RetentionPolicyExecutor>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    // Select splits that are published and older than the retention period.
    let retention_period = retention_policy.retention_period()?;
    let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
    let max_retention_timestamp = current_timestamp - retention_period.as_secs() as i64;

    // Without a secondary timestamp field, every split falls back to its primary time range, so
    // the secondary filter is skipped altogether.
    let expired_splits = match (timestamp_mode, secondary_timestamp_field_opt) {
        (RetentionTimestampMode::Primary, _) | (_, None) => {
            list_expired_splits(
                &index_uid,
                &metastore,
                max_retention_timestamp,
                RetentionTimestampType::Primary,
                ctx,
            )
            .await?
        }
        (RetentionTimestampMode::Secondary, Some(secondary_timestamp_field)) => {
            let secondary_expired_splits = list_expired_splits(
                &index_uid,
                &metastore,
                max_retention_timestamp,
                RetentionTimestampType::Secondary,
                ctx,
            )
            .await?;
            warn_on_primary_fallback(
                &index_uid,
                secondary_timestamp_field,
                &secondary_expired_splits,
            );
            secondary_expired_splits
        }
        (RetentionTimestampMode::SecondaryDryRun, Some(secondary_timestamp_field)) => {
            let primary_expired_splits = list_expired_splits(
                &index_uid,
                &metastore,
                max_retention_timestamp,
                RetentionTimestampType::Primary,
                ctx,
            )
            .await?;
            let secondary_expired_splits = list_expired_splits(
                &index_uid,
                &metastore,
                max_retention_timestamp,
                RetentionTimestampType::Secondary,
                ctx,
            )
            .await?;
            warn_on_primary_fallback(
                &index_uid,
                secondary_timestamp_field,
                &secondary_expired_splits,
            );
            log_secondary_dry_run(
                &index_uid,
                max_retention_timestamp,
                &primary_expired_splits,
                &secondary_expired_splits,
            );
            primary_expired_splits
        }
    };
    if expired_splits.is_empty() {
        return Ok(expired_splits);
    }
    // Mark the expired splits for deletion.
    let expired_split_ids: Vec<SplitId> = expired_splits
        .iter()
        .map(|split_metadata| split_metadata.split_id.to_string())
        .collect();
    info!(
        index_id=%index_uid.index_id,
        split_ids=?PrettySample::new(&expired_split_ids, 5),
        ?timestamp_mode,
        "Marking {} splits for deletion based on retention policy.",
        expired_split_ids.len()
    );
    let mark_splits_for_deletion_request =
        MarkSplitsForDeletionRequest::new(index_uid, expired_split_ids);
    ctx.protect_future(metastore.mark_splits_for_deletion(mark_splits_for_deletion_request))
        .await?;
    Ok(expired_splits)
}

/// Lists the published splits whose time range of type `timestamp_type` ends before
/// `max_retention_timestamp`. For `Secondary`, the metastore falls back to the primary time
/// range for splits without a secondary one.
async fn list_expired_splits(
    index_uid: &IndexUid,
    metastore: &MetastoreServiceClient,
    max_retention_timestamp: i64,
    timestamp_type: RetentionTimestampType,
    ctx: &ActorContext<RetentionPolicyExecutor>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    let query =
        ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
    let query = match timestamp_type {
        RetentionTimestampType::Primary => query.with_max_time_range_end(max_retention_timestamp),
        RetentionTimestampType::Secondary => {
            query.with_max_secondary_time_range_end(max_retention_timestamp)
        }
    };
    let list_splits_request = ListSplitsRequest::try_from_list_splits_query(&query)?;
    let (expired_splits, ignored_splits): (Vec<SplitMetadata>, Vec<SplitMetadata>) = ctx
        .protect_future(metastore.list_splits(list_splits_request))
        .await?
        .collect_splits_metadata()
        .await?
        .into_iter()
        .partition(|split_metadata| match timestamp_type {
            RetentionTimestampType::Primary => split_metadata.time_range.is_some(),
            RetentionTimestampType::Secondary => {
                split_metadata.time_range.is_some() || split_metadata.secondary_time_range.is_some()
            }
        });

    if !ignored_splits.is_empty() {
        let ignored_split_ids: Vec<String> = ignored_splits
            .into_iter()
            .map(|split_metadata| split_metadata.split_id)
            .collect();
        warn!(
            index_id=%index_uid.index_id,
            split_ids=?PrettySample::new(&ignored_split_ids, 5),
            "Retention policy could not be applied to {} splits because they lack a timestamp range.",
            ignored_split_ids.len()
        );
    }
    Ok(expired_splits)
}

/// Warns about expired splits that lack a secondary time range although the index has a
/// secondary timestamp field, because the secondary mode silently evaluates them against
/// their primary time range.
fn warn_on_primary_fallback(
    index_uid: &IndexUid,
    secondary_timestamp_field: &str,
    secondary_expired_splits: &[SplitMetadata],
) {
    let fallback_split_ids: Vec<&str> = secondary_expired_splits
        .iter()
        .filter(|split| split.secondary_time_range.is_none())
        .map(|split| split.split_id())
        .collect();
    if fallback_split_ids.is_empty() {
        return;
    }
    warn!(
        index_id=%index_uid.index_id,
        secondary_timestamp_field,
        split_ids=?PrettySample::new(&fallback_split_ids, 5),
        "Retention policy fell back to the primary timestamp for {} expired splits because they \
         lack a secondary time range.",
        fallback_split_ids.len()
    );
}

/// Splits on which the primary and secondary retention timestamp modes disagree.
#[derive(Debug, Default)]
struct SecondaryDryRunDiff<'a> {
    /// Splits expired in secondary mode but not in primary mode: switching to the
    /// secondary mode deletes them earlier than today.
    expired_earlier: Vec<&'a SplitMetadata>,
    /// Splits expired in primary mode but not in secondary mode: switching to the
    /// secondary mode retains them longer than today.
    retained_longer: Vec<&'a SplitMetadata>,
}

fn diff_secondary_dry_run<'a>(
    primary_expired_splits: &'a [SplitMetadata],
    secondary_expired_splits: &'a [SplitMetadata],
) -> SecondaryDryRunDiff<'a> {
    let primary_split_ids: HashSet<&str> = primary_expired_splits
        .iter()
        .map(|split| split.split_id())
        .collect();
    let secondary_split_ids: HashSet<&str> = secondary_expired_splits
        .iter()
        .map(|split| split.split_id())
        .collect();
    let mut expired_earlier: Vec<&SplitMetadata> = secondary_expired_splits
        .iter()
        .filter(|split| !primary_split_ids.contains(split.split_id()))
        .collect();
    // The splits with the most recent primary timestamps are the most surprising ones.
    expired_earlier.sort_by_key(|split| {
        std::cmp::Reverse(split.time_range.as_ref().map(|range| *range.end()))
    });
    let retained_longer: Vec<&SplitMetadata> = primary_expired_splits
        .iter()
        .filter(|split| !secondary_split_ids.contains(split.split_id()))
        .collect();
    SecondaryDryRunDiff {
        expired_earlier,
        retained_longer,
    }
}

fn log_secondary_dry_run(
    index_uid: &IndexUid,
    max_retention_timestamp: i64,
    primary_expired_splits: &[SplitMetadata],
    secondary_expired_splits: &[SplitMetadata],
) {
    let diff = diff_secondary_dry_run(primary_expired_splits, secondary_expired_splits);
    info!(
        index_id=%index_uid.index_id,
        cutoff=%format_timestamp(max_retention_timestamp),
        num_expired_primary=primary_expired_splits.len(),
        num_expired_secondary=secondary_expired_splits.len(),
        num_expired_earlier=diff.expired_earlier.len(),
        num_retained_longer=diff.retained_longer.len(),
        "retention secondary timestamp dry run"
    );
    if !diff.expired_earlier.is_empty() {
        warn!(
            index_id=%index_uid.index_id,
            cutoff=%format_timestamp(max_retention_timestamp),
            num_splits=diff.expired_earlier.len(),
            num_docs=diff.expired_earlier.iter().map(|split| split.num_docs).sum::<usize>(),
            size_bytes=diff.expired_earlier.iter().map(|split| split.footer_offsets.end).sum::<u64>(),
            splits=%format_split_samples(&diff.expired_earlier),
            "retention secondary timestamp dry run: splits that would be deleted in secondary mode but not in primary mode"
        );
    }
    if !diff.retained_longer.is_empty() {
        info!(
            index_id=%index_uid.index_id,
            cutoff=%format_timestamp(max_retention_timestamp),
            num_splits=diff.retained_longer.len(),
            num_docs=diff.retained_longer.iter().map(|split| split.num_docs).sum::<usize>(),
            size_bytes=diff.retained_longer.iter().map(|split| split.footer_offsets.end).sum::<u64>(),
            splits=%format_split_samples(&diff.retained_longer),
            "retention secondary timestamp dry run: splits that would be retained in secondary mode but are deleted in primary mode"
        );
    }
}

fn format_split_samples(splits: &[&SplitMetadata]) -> String {
    let mut samples = splits
        .iter()
        .take(DRY_RUN_MAX_SAMPLE_SPLITS)
        .map(|split| {
            format!(
                "{} time_range={} secondary_time_range={} create_timestamp={} num_docs={} \
                 size_bytes={}",
                split.split_id,
                format_time_range(split.time_range.as_ref()),
                format_time_range(split.secondary_time_range.as_ref()),
                format_timestamp(split.create_timestamp),
                split.num_docs,
                split.footer_offsets.end,
            )
        })
        .join("; ");
    if splits.len() > DRY_RUN_MAX_SAMPLE_SPLITS {
        samples.push_str(&format!(
            "; and {} more",
            splits.len() - DRY_RUN_MAX_SAMPLE_SPLITS
        ));
    }
    samples
}

fn format_time_range(time_range_opt: Option<&RangeInclusive<i64>>) -> String {
    match time_range_opt {
        Some(time_range) => format!(
            "[{}, {}]",
            format_timestamp(*time_range.start()),
            format_timestamp(*time_range.end())
        ),
        None => "none".to_string(),
    }
}

fn format_timestamp(timestamp: i64) -> String {
    OffsetDateTime::from_unix_timestamp(timestamp)
        .ok()
        .and_then(|datetime| datetime.format(&Rfc3339).ok())
        .unwrap_or_else(|| timestamp.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_split_metadata(
        split_id: &str,
        time_range: RangeInclusive<i64>,
        secondary_time_range: Option<RangeInclusive<i64>>,
    ) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            time_range: Some(time_range),
            secondary_time_range,
            ..Default::default()
        }
    }

    #[test]
    fn test_retention_timestamp_mode_from_str() {
        assert_eq!(
            "primary".parse::<RetentionTimestampMode>().unwrap(),
            RetentionTimestampMode::Primary
        );
        assert_eq!(
            "secondary-dry-run"
                .parse::<RetentionTimestampMode>()
                .unwrap(),
            RetentionTimestampMode::SecondaryDryRun
        );
        assert_eq!(
            "secondary".parse::<RetentionTimestampMode>().unwrap(),
            RetentionTimestampMode::Secondary
        );
        assert!("Secondary".parse::<RetentionTimestampMode>().is_err());
        assert!(
            "secondary_dry_run"
                .parse::<RetentionTimestampMode>()
                .is_err()
        );
        assert_eq!(
            RetentionTimestampMode::default(),
            RetentionTimestampMode::Primary
        );
    }

    #[test]
    fn test_diff_secondary_dry_run() {
        let primary_expired_splits = vec![
            make_split_metadata("both", 0..=100, Some(10..=110)),
            make_split_metadata("backfilled", 0..=100, Some(5000..=5010)),
        ];
        let secondary_expired_splits = vec![
            make_split_metadata("both", 0..=100, Some(10..=110)),
            make_split_metadata("future-1", 0..=8000, Some(10..=110)),
            make_split_metadata("future-2", 0..=9000, Some(10..=110)),
        ];
        let diff = diff_secondary_dry_run(&primary_expired_splits, &secondary_expired_splits);
        let expired_earlier_ids: Vec<&str> = diff
            .expired_earlier
            .iter()
            .map(|split| split.split_id())
            .collect();
        assert_eq!(expired_earlier_ids, ["future-2", "future-1"]);
        let retained_longer_ids: Vec<&str> = diff
            .retained_longer
            .iter()
            .map(|split| split.split_id())
            .collect();
        assert_eq!(retained_longer_ids, ["backfilled"]);
    }

    #[test]
    fn test_format_split_samples() {
        let splits: Vec<SplitMetadata> = (0..12)
            .map(|i| make_split_metadata(&format!("split-{i}"), 0..=60, None))
            .collect();
        let split_refs: Vec<&SplitMetadata> = splits.iter().collect();
        let samples = format_split_samples(&split_refs[..1]);
        assert_eq!(
            samples,
            "split-0 time_range=[1970-01-01T00:00:00Z, 1970-01-01T00:01:00Z] \
             secondary_time_range=none create_timestamp=1970-01-01T00:00:00Z num_docs=0 \
             size_bytes=0"
        );
        let samples = format_split_samples(&split_refs);
        assert!(samples.ends_with("; and 2 more"));
        assert_eq!(samples.matches("time_range=[").count(), 10);
    }
}
