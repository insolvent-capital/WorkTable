pub mod data;
mod link;

pub use data::DATA_INNER_LENGTH;
pub use {data::Data, data::ExecutionError as DataExecutionError};
pub use {link::Link, link::LINK_LENGTH};
