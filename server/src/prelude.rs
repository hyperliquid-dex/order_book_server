pub(crate) use crate::error::OrderBookError;
pub type Result<T> = std::result::Result<T, OrderBookError>;
pub(crate) use std::{fmt::Debug, fs, io};
