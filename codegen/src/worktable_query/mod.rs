mod parser;
pub mod model;

use proc_macro2::TokenStream;

pub use parser::Parser;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
todo!()
}