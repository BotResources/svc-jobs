macro_rules! db_string_serde {
    ($name:ty) => {
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.as_db_str().to_owned()
            }
        }

        impl TryFrom<String> for $name {
            type Error = crate::error::JobsError;

            fn try_from(value: String) -> Result<Self, crate::error::JobsError> {
                Self::from_db_str(&value)
            }
        }
    };
}

pub(crate) use db_string_serde;
