use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_table::generator::Generator;
use crate::persist_table::parser::Parser;

mod generator;
mod parser;

pub use generator::{WT_DATA_EXTENSION, WT_INDEX_EXTENSION, WT_INFO_EXTENSION};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = Parser::parse_struct(input)?;
    let pk_ident = Parser::parse_pk_ident(&input_fn);
    let index_type_ident = Parser::parse_index_ident(&input_fn);

    let gen = Generator {
        struct_def: input_fn,
        pk_ident,
        index_type_ident,
    };

    let space_file_def = gen.gen_space_file_def();
    let size_measurable_impl = gen.gen_size_measurable_impl()?;

    Ok(quote! {
        #size_measurable_impl
        #space_file_def
    })
}
