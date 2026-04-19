use std::collections::HashSet;

pub fn all_pushdowns() -> HashSet<PushdownSetting> {
    [
        PushdownSetting::GroupBy,
        PushdownSetting::ValueConditions,
        PushdownSetting::Ordering,
    ]
    .into()
}

#[derive(Hash, Clone, Eq, PartialEq, Debug)]
pub enum PushdownSetting {
    ValueConditions,
    GroupBy,
    Ordering,
}
