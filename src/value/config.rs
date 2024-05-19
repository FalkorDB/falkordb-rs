use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigValue {
    String(String),
    Int64(i64),
}

impl Display for ConfigValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigValue::String(str_val) => str_val.fmt(f),
            ConfigValue::Int64(int_val) => int_val.fmt(f),
        }
    }
}

impl From<i64> for ConfigValue {
    fn from(value: i64) -> Self {
        ConfigValue::Int64(value)
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        ConfigValue::String(value)
    }
}

impl From<&str> for ConfigValue {
    fn from(value: &str) -> Self {
        ConfigValue::String(value.to_string())
    }
}
