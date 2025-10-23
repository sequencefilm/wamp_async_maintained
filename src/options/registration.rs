pub use crate::options::option::{InvokeOption, MatchOption, OptionBuilder, WampOption};
use crate::{Arg, WampDict};

/// Base struct for storing WampDict value
pub struct RegistrationOptionItem(Option<WampDict>);

/// Provides functions for adding defined options to the WampDict
impl RegistrationOptionItem {
    /// Add an option for how the router should invoke the endpoint
    pub fn invoke(invoke_option: InvokeOption) -> Self {
        RegistrationOptions::new().with_invoke(invoke_option)
    }

    /// Add an option for how the router should invoke the endpoint
    pub fn with_invoke(&self, invoke_option: InvokeOption) -> Self {
        self.with_option(WampOption::RegisterOption(
            "invoke".to_owned(),
            Arg::String(invoke_option.value()),
        ))
    }

    /// Add an option for how the router should match the rpc.
    pub fn match_(match_option: MatchOption) -> Self {
        RegistrationOptions::new().with_match(match_option)
    }

    /// Add an option for how the router should match the rpc.
    pub fn with_match(&self, match_option: MatchOption) -> Self {
        self.with_option(WampOption::RegisterOption(
            "match".to_owned(),
            Arg::String(match_option.value()),
        ))
    }
}

/// Add base OptionBuilder functionality
impl OptionBuilder for RegistrationOptionItem {
    /// Build a new RegistrationOptionItem from a provided Option<WampDict>
    fn create(options: Option<WampDict>) -> Self
    where
        Self: OptionBuilder + Sized,
    {
        Self(options)
    }

    /// Return the WampDict being operated on and stored by RegistrationOptionItem
    fn get_dict(&self) -> Option<WampDict> {
        self.0.clone()
    }
}

/// Default
impl Default for RegistrationOptionItem {
    /// Create a new empty RegistrationOptionItem
    fn default() -> Self {
        Self::empty()
    }
}

/// Alias for RegistrationOptionItem
pub type RegistrationOptions = RegistrationOptionItem;
