use crate::{Arg, WampDict, WampString};

pub enum InvokeOption {
    Single,
    First,
    Last,
    Roundrobin,
    Random,
}

impl InvokeOption {
    pub fn value(&self) -> String {
        match *self {
            InvokeOption::Single => "single".to_owned(),
            InvokeOption::First => "first".to_owned(),
            InvokeOption::Last => "last".to_owned(),
            InvokeOption::Roundrobin => "roundrobin".to_owned(),
            InvokeOption::Random => "random".to_owned(),
        }
    }
}

pub enum MatchOption {
    Exact,
    Prefix,
    Wildcard,
}

impl MatchOption {
    pub fn value(&self) -> String {
        match *self {
            MatchOption::Exact => "exact".to_owned(),
            MatchOption::Prefix => "prefix".to_owned(),
            MatchOption::Wildcard => "wildcard".to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
/// Options specific to roles for key/value pairs
pub enum WampOption<K, V> {
    /// A publisher role feature option
    PublishOption(K, V),
    /// A Subscriber role feature option
    SubscribeOption(K, V),
    /// A Caller role feature option
    CallOption(K, V),
    /// A Callee role feature option
    RegisterOption(K, V),
    /// An empty option
    None,
}

/// Provides generic functionality for role options dictionary generation
pub trait OptionBuilder {
    /// Clones or creates a WampDict and inserts the key/value pair from the supplied WampOption
    ///
    /// * `option` - The key/value pair to insert into the dictionary
    fn with_option(&self, option: WampOption<String, Arg>) -> Self
    where
        Self: OptionBuilder + Sized,
    {
        let mut next_options = match &self.get_dict() {
            Some(opts) => opts.clone(),
            None => WampDict::new(),
        };

        let (key, value) = match Self::validate_option(option.clone()) {
            Some(result) => result,
            None => panic!("Can't create invalid option {:?}", option),
        };

        next_options.insert(key, value);

        Self::create(Some(next_options.clone()))
    }

    // TODO: Actual validation per role here
    /// WIP (currently not functional)
    /// Validate that the option being passed in is relevant for the role, and that they type of the value is correct for the given key.
    ///
    /// * `option` - The key/value pair to validate
    fn validate_option(option: WampOption<String, Arg>) -> Option<(WampString, Arg)> {
        match option {
            WampOption::PublishOption(key, value) => Some((key, value)),
            WampOption::SubscribeOption(key, value) => Some((key, value)),
            WampOption::RegisterOption(key, value) => Some((key, value)),
            WampOption::CallOption(key, value) => Some((key, value)),
            WampOption::None => None,
        }
    }

    /// Create a new empty builder - provided for convention
    fn new() -> Self
    where
        Self: OptionBuilder + Sized,
    {
        Self::empty()
    }

    /// Create a new empty builder
    fn empty() -> Self
    where
        Self: OptionBuilder + Sized,
    {
        Self::create(None)
    }

    /// Create an OptionBuilder using the provided WampDict
    /// Must implement
    fn create(options: Option<WampDict>) -> Self
    where
        Self: OptionBuilder + Sized;
    /// Return the current builder WampDict
    /// Must implement
    fn get_dict(&self) -> Option<WampDict>;
}
