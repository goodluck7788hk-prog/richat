//! Converts Alpenglow block footers and entry-update-parent markers to protobuf.
//!
//! Unlike accounts/transactions (thousands per second, need the zero-copy `raw`
//! encoder), these arrive about once per block and are small, so we always build a
//! normal `prost` struct and encode it, no matter which encoder is configured.
//! The output bytes are the same either way.

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaBlockFooterInfo, ReplicaEntryUpdateParentInfo,
    },
    log::warn,
    richat_proto::geyser::{
        BlockFooterFinalCert, BlockFooterNotarRewardCert, BlockFooterSignatureKind,
        BlockFooterSkipRewardCert, BlockFooterVotesAggregate, SubscribeUpdateBlockFooter,
        SubscribeUpdateEntryUpdateParent,
    },
    solana_bls_signatures::{BlsError, SignatureCompressed},
    solana_clock::BankId,
    solana_entry::block_component::{BlockFinalizationCert, VersionedBlockFooter, VotesAggregate},
};

fn aggregate(signature: &[u8], block_id: &[u8], bitmap: &[u8]) -> BlockFooterVotesAggregate {
    BlockFooterVotesAggregate {
        signature_kind: BlockFooterSignatureKind::CompressedBls12381G2 as i32,
        signature: signature.to_vec(),
        block_id: block_id.to_vec(),
        signer_bitmap: bitmap.to_vec(),
    }
}

// `VotesAggregate` exposes its signature only uncompressed, so compress it back to 96 bytes.
fn votes_aggregate(
    value: &VotesAggregate,
    block_id: &[u8],
) -> Result<BlockFooterVotesAggregate, BlsError> {
    let signature = SignatureCompressed::try_from(&value.uncompress_signature()?)?;
    Ok(aggregate(
        &signature.0,
        block_id,
        &value.clone().into_bitmap(),
    ))
}

// Slow finalization: `final_aggregate` votes on the slot (no block id), `notar_aggregate` on the
// block. Fast finalization: `final_aggregate` alone, on the block.
fn block_final_cert(cert: &BlockFinalizationCert) -> Result<BlockFooterFinalCert, BlsError> {
    let block_id = cert.block_id.as_ref();
    let (final_aggregate, notar_aggregate) = match &cert.notar_aggregate {
        Some(notar_aggregate) => (
            votes_aggregate(&cert.final_aggregate, &[])?,
            Some(votes_aggregate(notar_aggregate, block_id)?),
        ),
        None => (votes_aggregate(&cert.final_aggregate, block_id)?, None),
    };
    Ok(BlockFooterFinalCert {
        slot: cert.slot,
        final_aggregate: Some(final_aggregate),
        notar_aggregate,
    })
}

pub fn create_block_footer(
    info: &ReplicaBlockFooterInfo<'_>,
    bank_id: BankId,
) -> SubscribeUpdateBlockFooter {
    let VersionedBlockFooter::V1(footer) = info.block_footer;

    // A final cert with an invalid signature is dropped, so the rest of the footer still goes out.
    let block_final_cert = footer.block_final_cert.as_ref().and_then(|cert| {
        block_final_cert(cert)
            .inspect_err(|error| warn!("slot {}: invalid block final cert: {error}", info.slot))
            .ok()
    });

    SubscribeUpdateBlockFooter {
        slot: info.slot,
        bank_id,
        bank_hash: footer.bank_hash.as_ref().to_vec(),
        block_producer_time_nanos: footer.block_producer_time_nanos,
        block_user_agent: footer.block_user_agent.clone(),
        block_final_cert,
        skip_reward_cert: footer
            .skip_reward_cert
            .as_ref()
            .map(|cert| BlockFooterSkipRewardCert {
                slot: cert.slot,
                aggregate: Some(aggregate(&cert.signature.0, &[], cert.to_bitmap())),
            }),
        notar_reward_cert: footer.notar_reward_cert.as_ref().map(|cert| {
            BlockFooterNotarRewardCert {
                slot: cert.slot,
                aggregate: Some(aggregate(
                    &cert.signature.0,
                    cert.block_id.as_ref(),
                    cert.bitmap(),
                )),
            }
        }),
    }
}

pub fn create_entry_update_parent(
    info: &ReplicaEntryUpdateParentInfo<'_>,
) -> SubscribeUpdateEntryUpdateParent {
    SubscribeUpdateEntryUpdateParent {
        slot: info.slot,
        cleared_bank_id: info.cleared_bank_id,
        parent_slot: info.parent_slot,
        parent_block_id: info.parent_block_id.as_ref().to_vec(),
    }
}
