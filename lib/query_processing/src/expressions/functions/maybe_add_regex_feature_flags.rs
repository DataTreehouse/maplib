pub fn maybe_add_regex_feature_flags(pattern: &str, flags: Option<&str>) -> String {
    if let Some(flags) = flags {
        //TODO: Validate flags..
        format!("(?{}){}", flags, pattern)
    } else {
        pattern.to_string()
    }
}
