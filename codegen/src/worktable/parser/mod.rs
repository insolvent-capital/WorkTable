mod columns;
mod index;
mod name;
mod punct;
pub mod queries;

use proc_macro2::{TokenStream, TokenTree};
use std::iter::Peekable;

pub struct Parser {
    pub input: TokenStream,
    pub input_iter: Peekable<proc_macro2::token_stream::IntoIter>,
}

impl Parser {
    pub fn new(input: TokenStream) -> Self {
        Self {
            input: input.clone(),
            input_iter: input.into_iter().peekable(),
        }
    }

    pub fn has_next(&mut self) -> bool {
        self.input_iter.peek().is_some()
    }

    pub fn peek_next(&mut self) -> Option<&TokenTree> {
        self.input_iter.peek()
    }
}
