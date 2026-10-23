use {
    prost::Message as _,
    richat_proto::{convert_from, convert_to, solana::storage::confirmed_block as proto},
    solana_transaction_status::{Reward, RewardType},
};

#[test]
fn reward_enums_preserve_schema_values_through_protobuf_roundtrip() {
    for (reward_type, schema_type, expected_number) in [
        (None, proto::RewardType::Unspecified, 0),
        (Some(RewardType::Fee), proto::RewardType::Fee, 1),
        (Some(RewardType::Rent), proto::RewardType::Rent, 2),
        (Some(RewardType::Staking), proto::RewardType::Staking, 3),
        (Some(RewardType::Voting), proto::RewardType::Voting, 4),
        (
            Some(RewardType::DeactivatedStake),
            proto::RewardType::DeactivatedStake,
            5,
        ),
        (Some(RewardType::VATDebit), proto::RewardType::VatDebit, 6),
    ] {
        assert_eq!(schema_type as i32, expected_number);
        assert_eq!(convert_to::create_reward_type(reward_type), schema_type);
        let reward = Reward {
            pubkey: "11111111111111111111111111111111".to_owned(),
            lamports: -17,
            post_balance: 42,
            reward_type,
            commission: Some(0),
            commission_bps: Some(1200),
        };
        let encoded = convert_to::create_reward(&reward);
        assert_eq!(encoded.reward_type, expected_number);
        let decoded_wire = proto::Reward::decode(encoded.encode_to_vec().as_slice()).unwrap();
        let decoded_reward = convert_from::create_reward(decoded_wire).unwrap();
        assert_eq!(decoded_reward.reward_type, reward_type);
        assert_eq!(decoded_reward.pubkey, reward.pubkey);
        assert_eq!(decoded_reward.lamports, -17);
        assert_eq!(decoded_reward.post_balance, 42);
        assert_eq!(decoded_reward.commission, Some(0));
        assert_eq!(decoded_reward.commission_bps, Some(1200));
        assert_eq!(convert_to::create_reward(&decoded_reward), encoded);
    }
}
